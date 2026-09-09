# QuantMind 开发架构

本文档是开发者架构索引。需要按步骤熟悉项目时，先阅读 [二次开发指南](二次开发指南.md)。用户部署请优先阅读根目录 [README.md](../../README.md) 和 [deploy/README.md](../../deploy/README.md)；API 的实时定义以 FastAPI `/docs` 与路由源码为准。

## 架构概览

QuantMind OSS 使用 Docker Compose 部署。核心 `quantmind` 容器通过 `backend/main_oss.py` 统一启动四个后端服务：

| 服务 | 端口 | 职责 |
| --- | --- | --- |
| API | 8000 | 认证、管理、策略、模型与资讯 API |
| Engine | 8001 | Qlib 回测、训练、推理、研究引擎 |
| Trade | 8002 | 订单、仓位、风控与模拟交易 |
| Stream | 8003 | 行情流与 WebSocket 推送 |

Compose 同时编排以下依赖与可选服务：

- `db`（PostgreSQL）与 `redis`
- `celery-worker`、`celery-beat`
- `web`、`data-gateway`、`dashboard`
- `huntly`、`rsshub`、`qwenpaw`

## 关键目录

```text
backend/                       FastAPI 服务与共享模块
backend/shared/                数据库、Redis、配置、日志和通用工具
backend/services/engine/       Qlib、训练、推理、研究任务
electron/                      Electron / React 客户端
db/qlib_data/                  Qlib 日历、标的和特征数据
data/                          业务运行数据与回测结果
models/                        模型产物与用户模型
docker-compose.yml             服务编排
deploy/                         在线、离线部署与更新入口
```

## 数据与任务流

```text
市场/QuantDB 数据 → PostgreSQL 与 Qlib 二进制数据
                       ↓
因子研究 → 训练任务 → 模型注册 → 批量推理 → 选股信号
                       ↓
                  Qlib 回测 → 风险指标与结果持久化
                       ↓
               模拟交易 → 订单、仓位、风控与回放
```

共享模块负责跨服务一致性：

- `backend/shared/strategy_storage.py`：策略 CRUD 的唯一入口
- `backend/shared/stock_utils.py`：股票代码规范化；内部统一使用 `SH600036` 前缀格式
- Redis：0=通用、1=认证、2=交易、3=行情、4=回测、5=缓存
- PostgreSQL：业务事实数据与任务/结果持久化

## 开发命令

```bash
# Compose 服务
docker compose up -d
docker compose ps
docker compose logs -f quantmind

# 后端测试
python backend/run_tests.py unit
ruff check backend/

# Electron 前端
cd electron
npm run dev
npm run typecheck
```

前端开发使用本地 Vite HMR；修改 `electron/src` 不需要重建服务器的 `web` 容器。修改后端后，应在测试通过后使用 `deploy/update.sh` 或受控的 Compose 重建同步到服务器。

## 运行约定

- 业务数据、模型和 Qlib 数据应与代码更新分离；更新脚本不会默认删除它们。
- Qlib 默认数据目录为 `db/qlib_data`，有效数据至少包含 `calendars/day.txt` 与 `features/`。
- 环境变量集中在 `.env` 和 `docker-compose.yml`；不得将密钥提交到仓库。
- API 变动应同步检查客户端调用、服务路由和 `/docs`。

## 排障入口

```bash
docker compose ps
docker compose logs --tail=200 quantmind
curl http://127.0.0.1:8000/health
```

若回测提示 Qlib 数据缺失，先检查 `db/qlib_data/calendars/day.txt`、`instruments/` 和 `features/` 是否存在，再检查容器中的挂载路径。
