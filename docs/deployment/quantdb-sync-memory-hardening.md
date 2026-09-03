# QuantDB 远程同步内存加固部署指南

本文记录“从远程 QuantDB 拉取数据时整机内存打满”的根因与完整部署步骤，包含代码分支、服务器应用、容器重启、验证、调参与回滚。适用于 QuantMind OSS（单镜像 Compose 部署）。

## 1. 背景与根因

现象：服务器无 Swap（16 GB 内存），管理台手动同步 QuantDB 时，整机内存打到 100%，宿主机失去响应，只能靠云控制台重启。

根因不是“下载”本身单点造成，而是多处重度 pandas/parquet 任务叠加：

1. 手动 QuantDB 同步原先是 API 进程内的 `threading.Thread`，与行情、市场分析等请求共享一个约 4 GB 基线的 API 进程。
2. 下载并发 `SYNC_WORKERS = 8`。
3. Celery 主 worker 同时承担回测、期货市场同步等重任务，与手动同步共用进程池。
4. 容器没有内存上限，主机又没有 Swap，任意叠加都会直接打满整机。
5. 管理台目录 `/catalog` 每次请求递归统计全部 parquet 文件，单次 5–9 秒，并发打开页面会放大压力。

## 2. 加固方案

| 措施 | 目的 |
| --- | --- |
| 手动 QuantDB 同步改为 Celery 后台任务 | 不再占用 API 进程内存 |
| 新增专用 `quantdb_sync` 队列与 `celery-quantdb-worker` 容器 | 与回测/期货任务隔离，不抢同一进程池 |
| API、主 Celery、专用同步 worker 分别设置内存硬上限 | 超限只影响单容器，宿主机不再失响应 |
| 下载文件并发从 8 降至 2 | 降低单任务瞬时内存/带宽峰值 |
| `/catalog` 加 60 秒缓存 | 降低页面反复全量统计的压力 |
| API 内嵌 Celery worker 显式绑定回测队列 | 防止内嵌 worker 因 `task_queues` 新增而顺带消费 `quantdb_sync` |

默认资源分配（均可用环境变量覆盖）：

| 容器 | 默认内存上限 |
| --- | --- |
| `quantmind`（API/Engine/Trade/Stream + 内嵌回测 Celery） | 6 GB |
| `quantmind-celery`（主 Celery：回测/定时同步） | 5 GB |
| `quantmind-celery-quantdb`（QuantDB 手动同步专用） | 4 GB |

## 3. 涉及文件

```text
backend/main_oss.py                                        # 内嵌 Celery worker 绑定 -Q 回测队列
backend/services/api/routers/admin/quantdb_console.py       # 同步改投递 Celery；目录缓存
backend/services/engine/qlib_app/celery_config.py           # 显式声明 quantdb_sync 队列
backend/services/engine/tasks/celery_tasks.py               # 新增 run_quantdb_console_sync 任务
backend/shared/quantdb_sync_jobs.py                         # Redis 任务取消/唯一 job id
backend/scripts/quantdb_daily_sync.py                       # SYNC_WORKERS 8 -> 2（可配置）
docker-compose.yml                                          # 内存上限 + 新增专用 worker
```

## 4. 代码分支（本地仓库）

推荐在独立分支上修改并提交，保留服务器上已有的部署改动：

```bash
cd /Users/ross/opensource/QuantMind   # 本机 QuantMind 仓库
git checkout -b codex/quantdb-memory-hardening

# 修改完成后检查与提交
git add backend/main_oss.py \
  backend/services/api/routers/admin/quantdb_console.py \
  backend/services/engine/qlib_app/celery_config.py \
  backend/services/engine/tasks/celery_tasks.py \
  backend/shared/quantdb_sync_jobs.py \
  backend/scripts/quantdb_daily_sync.py \
  docker-compose.yml
git commit -m 'fix(quantdb): isolate console sync in dedicated celery worker and harden memory limits'
```

不要提交服务器上已有的三个部署文件：

```text
config/data_sources_config.json
electron/src/features/auth/components/LoginPage.tsx
electron/src/features/auth/services/authService.ts
```

## 5. 打包并上传补丁

若本次改动连续提交在本地分支上，可用 `git format-patch` 导出：

```bash
git format-patch -2 --stdout > /tmp/quantdb-memory-hardening.patch

scp /tmp/quantdb-memory-hardening.patch root@<SERVER_IP>:/tmp/
```

其中 `<SERVER_IP>` 替换为目标服务器 IP；请使用部署方允许的认证方式（SSH Key 或密码）。补丁只包含本次内存加固文件，不应包含业务配置改动。

## 6. 在服务器应用

目标仓库默认位于 `/www/quantmind`（如使用 `deploy/deploy.sh` 也可能是 `/opt/quantmind`）：

```bash
ssh root@<SERVER_IP>
cd /www/quantmind

# 确认当前没有正在执行的重任务
docker ps --format '{{.Names}}\t{{.Status}}'
free -h
docker exec quantmind-celery \
  celery -A backend.services.engine.qlib_app.celery_config:celery_app \
  inspect active --timeout=8

# 在服务器仓库创建同一分支（保留已有脏文件）
git checkout -b codex/quantdb-memory-hardening

# 先校验再应用
git apply --check /tmp/quantdb-memory-hardening.patch
git apply /tmp/quantdb-memory-hardening.patch
```

## 7. 应用前校验

```bash
cd /www/quantmind

# Compose 可解析
docker compose config --quiet

# Python 语法检查
docker exec quantmind python - <<'PY'
import ast
for p in [
    "backend/main_oss.py",
    "backend/scripts/quantdb_daily_sync.py",
    "backend/shared/quantdb_sync_jobs.py",
    "backend/services/engine/tasks/celery_tasks.py",
    "backend/services/engine/qlib_app/celery_config.py",
    "backend/services/api/routers/admin/quantdb_console.py",
]:
    ast.parse(open(p, encoding="utf-8").read(), filename=p)
print("py-ok")
PY

# 展开后确认内存上限
docker compose config | grep -E 'mem_limit|container_name'
```

期望：

```text
container_name: quantmind-celery-quantdb
mem_limit: "4294967296"   # 4 GB
mem_limit: "5368709120"   # 5 GB
mem_limit: "6442450944"   # 6 GB
```

## 8. 重建并启动容器

代码以 bind mount 挂载进容器，补丁生效只需重建受影响的容器，不需要重新构建镜像：

```bash
cd /www/quantmind
docker compose up -d --no-deps --force-recreate \
  quantmind \
  celery-worker \
  celery-quantdb-worker
```

`quantmind-celery-quantdb` 是新容器，第一次执行会创建。`celery-beat`、数据库、Redis 等不重建。

启动后确认健康：

```bash
docker ps --filter name=quantmind --format '{{.Names}}\t{{.Status}}'

# 每个容器都应有 healthy
docker inspect quantmind quantmind-celery quantmind-celery-quantdb \
  --format '{{.Name}} mem={{.HostConfig.Memory}}'
```

## 9. 队列隔离验证

查看所有 Celery worker 活动队列：

```bash
docker exec quantmind-celery-quantdb \
  celery -A backend.services.engine.qlib_app.celery_config:celery_app \
  inspect active_queues --timeout=8
```

预期：

- `quantdb_sync@...` 只消费 `quantdb_sync`。
- 主 Celery 容器与 API 内嵌 Celery worker 只消费 `qlib_backtest_srv`。
- `quantdb_sync` 队列的 exchange 为 `qlib`（direct），routing key 为 `quantdb_sync`。

若发现 API 内嵌 worker（进程名仍是 `python backend/main_oss.py`）也在消费 `quantdb_sync`，说明缺少 `backend/main_oss.py` 中 `-Q CELERY_QUEUE` 的修复，需先补上再重启 `quantmind`。

## 10. 端到端小规模验证

### 10.1 管理台 UI

打开 Web 管理台 → 数据平台 → QuantDB 控制台 → 选择一个小数据集（如 `trading_calendar`）→ 同步。任务应立即出现在“同步任务”列表并变为 `completed`。

### 10.2 API 验证（可选）

登录并取得令牌后调用：

```bash
# 仅返回 HTTP 状态码，不在终端回显令牌
curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"default","username":"<ADMIN_USER>","password":"<ADMIN_PASS>"}'
```

随后触发一个小数据集同步：

```bash
curl -s -X POST \
  http://127.0.0.1:8000/api/v1/admin/data-platform/quantdb/sync-datasets \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"datasets":["trading_calendar"],"with_pg":false,"with_qlib":false}'
```

返回 `job.job_id` 后轮询：

```bash
curl -s \
  http://127.0.0.1:8000/api/v1/admin/data-platform/quantdb/sync-jobs/<JOB_ID> \
  -H "Authorization: Bearer $TOKEN"
```

### 10.3 直接投递任务验证（无需登录）

```bash
docker exec quantmind python3 - <<'PY'
from backend.shared.quantdb_sync_jobs import new_celery_job
from backend.services.engine.qlib_app.celery_config import celery_app

job = new_celery_job(datasets=["trading_calendar"], started_by="verify")
print(job["job_id"])
celery_app.send_task(
    "engine.tasks.run_quantdb_console_sync",
    kwargs={
        "job_id": job["job_id"],
        "datasets": ["trading_calendar"],
        "with_pg": False,
        "with_qlib": False,
        "pg_full": False,
    },
    queue="quantdb_sync",
)
PY
```

### 10.4 内存观察

验证期间观察内存：

```bash
watch -n 2 'docker stats --no-stream \
  quantmind quantmind-celery quantmind-celery-quantdb; \
  free -h | head -2'
```

健康预期：

- `quantmind` 稳定在 6 GB 上限以内；
- 同步任务只在 `quantmind-celery-quantdb` 中执行，峰值控制在 4 GB 上限以内；
- 主 Celery 与 API 容器内存曲线不应被同步任务抬高；
- 宿主机 `available` 保持充足，不再出现 100% 失响应。

## 11. 调参

可在 `/www/quantmind/.env`（不存在则新建）或 Compose 环境变量中覆盖：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `QM_API_MEM_LIMIT` | `6g` | API 容器内存硬上限 |
| `QM_CELERY_MEM_LIMIT` | `5g` | 主 Celery 容器内存硬上限 |
| `QM_QUANTDB_WORKER_MEM_LIMIT` | `4g` | QuantDB 专用 worker 内存硬上限 |
| `QUANTDB_SYNC_WORKERS` | `2` | 单数据集内文件下载并发 |
| `QUANTDB_SYNC_WORKER_CONCURRENCY` | `1` | 专用 worker 的 Celery 并发数 |
| `QUANTDB_SYNC_QUEUE` | `quantdb_sync` | 手动同步队列名 |

示例：

```bash
QM_API_MEM_LIMIT=8g
QM_CELERY_MEM_LIMIT=6g
QM_QUANTDB_WORKER_MEM_LIMIT=6g
QUANTDB_SYNC_WORKERS=2
```

修改后重建对应容器生效：

```bash
docker compose up -d --no-deps --force-recreate \
  quantmind celery-worker celery-quantdb-worker
```

注意：所有容器上限之和应明显小于宿主机总内存，并预留系统与其他容器（如 QwenPaw、Huntly、IB Gateway）的空间。

## 12. 使用建议

- 不在同步期间同时打开多个行情/市场分析/股票列表页面，这些接口本身会做重 DataFrame 运算。
- 大任务（`daily_forward` 等几千个文件）尽量安排在回测与期货定时同步的空窗期。
- 服务器维持 16 GB 以上内存时，仍建议给整机增加 4–8 GB Swap 作为最后的兜底，避免极端叠加再次导致无响应。
- 验证成功后如长期使用，建议将该分支合并到部署/发布分支并推送到仓库，保持服务器分支与代码库一致。

## 13. 回滚

如出现异常需要回滚到部署前基线：

```bash
cd /www/quantmind

# 方案 A：整文件恢复部署前版本（b9e1536 为本次基线示例）
git checkout b9e1536 -- \
  backend/main_oss.py \
  backend/services/api/routers/admin/quantdb_console.py \
  backend/services/engine/qlib_app/celery_config.py \
  backend/services/engine/tasks/celery_tasks.py \
  backend/shared/quantdb_sync_jobs.py \
  backend/scripts/quantdb_daily_sync.py \
  docker-compose.yml

# 重建原有容器并移除新增专用 worker
docker compose up -d --no-deps --force-recreate \
  --remove-orphans quantmind celery-worker
docker rm -f quantmind-celery-quantdb

# 确认回滚后健康
docker ps --filter name=quantmind --format '{{.Names}}\t{{.Status}}'
free -h
```

恢复内存上限等配置后同样建议用一个小数据集同步验证一次。

## 14. 本次实际部署记录

本次部署（示例环境，仅供追溯）：

| 项目 | 值 |
| --- | --- |
| 服务器仓库 | `/www/quantmind` |
| 服务器分支 | `codex/quantdb-memory-hardening` |
| 服务器提交 | `3819b83` |
| 本地分支 | `codex/quantdb-memory-hardening` |
| 验证数据集 | `trading_calendar` |
| 验证结果 | 任务 `completed`，容器内存均低于上限 |
