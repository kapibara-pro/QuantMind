/**
 * 统一服务端口配置
 * 所有服务端口的唯一配置来源
 */
export const SERVICE_PORTS = {
  // 前端服务
  FRONTEND_DEV: 3000,

  // 后端服务 (统一通过网关 8000)
  API_GATEWAY: 8000,
  MARKET_DATA: 8000,    // 原 8002
  DATA_SERVICE: 8000,   // 原 8002
  USER_SERVICE: 8000,   // 原 8011
  AI_STRATEGY: 8000,    // 原 8007
  STOCK_QUERY: 8000,    // 原 8010
  TRADING: 8000,        // 原 8004
  QLIB_SERVICE: 8000, // Qlib快速回测服务（收敛至网关）

  // WebSocket服务
  WEBSOCKET_MARKET: 8003,

  // 数据库
  REDIS: 6379,
} as const;

const ENV: Record<string, any> = typeof import.meta !== 'undefined' ? (import.meta as any).env || {} : {};

// 动态服务器配置（桌面端与 Web 浏览器均可设置）
let dynamicServerUrl: string | null = null;
const SERVER_URL_STORAGE_KEY = 'quantmind_server_url_v2';
const LEGACY_SERVER_URL_STORAGE_KEY = 'quantmind_server_url';

// Electron 桌面端兜底地址：OSS 本地 Docker 后端（api 网关 8000）
const DEFAULT_ELECTRON_API_BASE = 'http://127.0.0.1:8000';

/**
 * 归一化用户输入的服务器地址。
 *
 * 支持 `example.com`、`example.com:3000`、`http://example.com:3000`、
 * `https://example.com/api/v1` 等写法，输出不含结尾斜杠的基础地址。
 * 无法解析时返回 null。
 */
export function normalizeServerUrl(input: string): string | null {
  const raw = String(input ?? '').trim();
  if (!raw) return null;

  const withProtocol = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(raw)
    ? raw
    : `http://${raw}`;

  try {
    const parsed = new URL(withProtocol);
    if (!parsed.hostname) return null;
    let path = parsed.pathname.replace(/\/+$/, '');
    if (path.endsWith('/api/v1')) {
      path = path.slice(0, -'/api/v1'.length);
    }
    return `${parsed.protocol}//${parsed.host}${path}`;
  } catch {
    return null;
  }
}

function readPersistedServerUrl(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    return localStorage.getItem(SERVER_URL_STORAGE_KEY)?.trim() || null;
  } catch {
    return null;
  }
}

function readLegacyPersistedServerUrl(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    return localStorage.getItem(LEGACY_SERVER_URL_STORAGE_KEY)?.trim() || null;
  } catch {
    return null;
  }
}

function persistServerUrl(url: string | null): void {
  if (typeof window === 'undefined') return;
  try {
    if (url) {
      localStorage.setItem(SERVER_URL_STORAGE_KEY, url);
      localStorage.removeItem(LEGACY_SERVER_URL_STORAGE_KEY);
      return;
    }
    localStorage.removeItem(SERVER_URL_STORAGE_KEY);
  } catch {
    // ignore storage failures
  }
}

/**
 * 检测是否为 Electron 桌面环境
 */
export function isElectronEnv(): boolean {
  return (
    typeof window !== 'undefined' &&
    typeof (window as any).electronAPI === 'object' &&
    typeof navigator !== 'undefined' &&
    /Electron\//i.test(navigator.userAgent || '')
  );
}

/**
 * 校验服务器地址是否可达。
 *
 * 使用 `no-cors` 探测：只要网络层能连上目标主机即视为可达，避免后端未开放
 * /health 的 CORS 响应头时把可用地址误判为失效而清除用户配置。
 */
export async function isServerReachable(url: string, timeoutMs = 8000): Promise<boolean> {
  if (!url) return false;
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    await fetch(`${url.replace(/\/+$/, '')}/health`, {
      signal: controller.signal,
      mode: 'no-cors',
      // 不携带凭据，仅做连通性探测
      cache: 'no-store',
    });
    clearTimeout(timer);
    return true;
  } catch {
    return false;
  }
}

/**
 * 清理失效的服务器配置（本地缓存 + 桌面端配置文件）
 */
async function clearStaleServerUrl(reason: string): Promise<void> {
  console.warn(`[services] 服务器地址失效，清除缓存配置: ${reason}`);
  persistServerUrl(null);
  if (typeof window !== 'undefined' && (window as any).electronAPI?.setServerUrl) {
    try {
      await (window as any).electronAPI.setServerUrl('');
    } catch (e) {
      console.warn('[services] 清除桌面配置文件失败（忽略）:', e);
    }
  }
}

/**
 * 初始化动态服务器配置（应用启动时调用，桌面端与 Web 通用）
 * 优先级：持久化配置 > Electron 配置文件 > 环境变量 / 当前站点
 *
 * Web 模式下未配置过地址时不做任何探测，直接沿用当前站点（相对路径 + 反向代理），
 * 因此默认部署行为保持不变；只有用户显式保存过地址才使用绝对后端地址。
 */
export async function initDynamicServerUrl(): Promise<void> {
  // 1. 持久化配置：若探测可达则采用；若确认不可达，清除缓存并回退本机默认，
  //    避免换 IP/克隆部署到新机器后始终连旧地址导致“验证身份”卡死。
  const persisted = readPersistedServerUrl();
  if (persisted) {
    const ok = await isServerReachable(persisted);
    if (ok) {
      dynamicServerUrl = persisted;
      return;
    }
    // Web 浏览器里的地址只可能来自用户在本机的显式设置，网络抖动或后端重启时
    // 不静默清空，保留配置并提示；用户可随时在登录页「服务器设置」里修正。
    if (!isElectronEnv()) {
      console.warn(`[services] 服务器 ${persisted} 探测未通过，保留浏览器中保存的地址`);
      dynamicServerUrl = persisted;
      return;
    }
    console.warn(`[services] 服务器 ${persisted} 探测未通过，清除配置并回退本地默认后端`);
    await clearStaleServerUrl(`换 IP 后旧地址 ${persisted} 不可达`);
  }

  // 2. 旧 key（quantmind_server_url）遗留缓存迁移：可达才采用，失效仅清旧 key（不动新 key）
  const legacy = readLegacyPersistedServerUrl();
  if (legacy) {
    const ok = await isServerReachable(legacy);
    if (ok) {
      dynamicServerUrl = legacy;
      persistServerUrl(legacy);
      return;
    }
    try {
      localStorage.removeItem(LEGACY_SERVER_URL_STORAGE_KEY);
    } catch { /* ignore */ }
    console.warn(`[services] 旧版服务器地址失效，清除缓存: ${legacy}`);
  }

  // 3. Web 浏览器：没有用户配置时保持“当前站点”语义，不做额外探测。
  if (!isElectronEnv()) {
    dynamicServerUrl = null;
    return;
  }

  // 4. Electron 配置文件：同样采用 + 后台探测日志，不清除
  try {
    const url = await (window as any).electronAPI.getServerUrl();
    if (url && typeof url === 'string') {
      const normalized = url.replace(/\/+$/, '');
      dynamicServerUrl = normalized;
      persistServerUrl(normalized);
      void isServerReachable(normalized).then((ok) => {
        if (!ok) console.warn(`[services] 配置文件服务器 ${normalized} 探测未通过（可能暂不可达），保留并继续使用`);
      });
      return;
    }
  } catch (e) {
    console.warn('[services] Failed to get server URL from config:', e);
  }

  // 5. 兜底：本地 OSS Docker 后端
  if (!dynamicServerUrl) {
    const ok = await isServerReachable(DEFAULT_ELECTRON_API_BASE);
    if (ok) {
      dynamicServerUrl = DEFAULT_ELECTRON_API_BASE;
      persistServerUrl(DEFAULT_ELECTRON_API_BASE);
    }
  }
}

/**
 * 设置动态服务器配置（用户设置后调用）。
 *
 * 传入空值时清除覆写：Web 回退到当前站点，桌面端回退到本机默认后端。
 */
export function setDynamicServerUrl(url: string): void {
  const trimmed = String(url ?? '').trim();
  if (!trimmed) {
    dynamicServerUrl = null;
    persistServerUrl(null);
    return;
  }
  dynamicServerUrl = normalizeServerUrl(trimmed) ?? trimmed.replace(/\/+$/, '');
  persistServerUrl(dynamicServerUrl);
}

/**
 * 获取当前动态服务器配置
 */
export function getDynamicServerUrl(): string | null {
  return dynamicServerUrl || readPersistedServerUrl();
}

const HOST = ENV.VITE_SERVICE_HOST || '';
const HTTP_PROTOCOL = ENV.VITE_HTTP_PROTOCOL || 'http';
const WS_PROTOCOL = HTTP_PROTOCOL === 'https' ? 'wss' : 'ws';

export function normalizeBaseUrl(url: string): string {
  if (!url) return url;
  let normalized = url.replace(/\/+$/, '');
  if (normalized.endsWith('/api/v1')) {
    normalized = normalized.slice(0, -'/api/v1'.length);
  }
  return normalized;
}

const API_BASE = normalizeBaseUrl(ENV.VITE_API_BASE_URL || '');

/**
 * 获取基础 URL（优先使用动态配置）
 */
function getBaseUrl(): string {
  // 用户显式配置的服务器地址优先级最高（桌面端与 Web 通用）
  const configured = dynamicServerUrl || readPersistedServerUrl();
  if (configured) {
    return configured;
  }
  if (API_BASE) {
    return API_BASE;
  }
  // Electron 桌面端兜底：本地 OSS Docker 后端（避免 file:// 下相对路径请求全部失败）
  if (isElectronEnv()) {
    return DEFAULT_ELECTRON_API_BASE;
  }
  // Web 部署保持当前站点，交由 Nginx / 反向代理转发 /api 与 /ws。
  return API_BASE;
}

// WebSocket URL 构建
const getWebSocketUrl = () => {
  const persisted = getDynamicServerUrl();
  // 桌面端使用动态配置
  if (persisted) {
    return `${persisted.replace(/^http/, 'ws')}/api/v1/ws/market`;
  }
  const gateway = getBaseUrl();
  if (gateway) {
    return `${gateway.replace(/^http/, 'ws')}/api/v1/ws/market`;
  }
  // Web 部署使用相对路径，通过 Nginx 代理
  if (typeof window !== 'undefined') {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}/ws/api/v1/ws/market`;
  }
  // 最后才回退到环境变量，避免开发环境配置压过用户保存的服务器地址
  if (ENV.VITE_WS_BASE_URL || ENV.VITE_WEBSOCKET_MARKET_URL) {
    return ENV.VITE_WS_BASE_URL || ENV.VITE_WEBSOCKET_MARKET_URL;
  }
  return '';
};

export const SERVICE_URLS = {
  get API_GATEWAY() { return normalizeBaseUrl(ENV.VITE_API_GATEWAY_URL) || getBaseUrl(); },
  get MARKET_DATA() { return normalizeBaseUrl(ENV.VITE_MARKET_DATA_API_URL) || getBaseUrl(); },
  get DATA_SERVICE() { return normalizeBaseUrl(ENV.VITE_DATA_SERVICE_API_URL) || getBaseUrl(); },
  get USER_SERVICE() { return normalizeBaseUrl(ENV.VITE_USER_API_URL) || getBaseUrl(); },
  get AI_STRATEGY() { return normalizeBaseUrl(ENV.VITE_AI_STRATEGY_API_URL) || getBaseUrl(); },
  get STOCK_QUERY() { return normalizeBaseUrl(ENV.VITE_STOCK_QUERY_API_URL) || getBaseUrl(); },
  get TRADING() { return normalizeBaseUrl(ENV.VITE_TRADING_API_URL) || getBaseUrl(); },
  get QLIB_SERVICE() { return normalizeBaseUrl(ENV.VITE_QLIB_SERVICE_URL) || getBaseUrl(); },
  get ENGINE_SERVICE() { return normalizeBaseUrl(ENV.VITE_ENGINE_SERVICE_URL) || getBaseUrl(); },
  get WEBSOCKET_MARKET() { return getWebSocketUrl(); },
} as const;

// API路径配置
export const API_PATHS = {
  V1: '/api/v1',
  HEALTH: '/health',
  STRATEGIES: '/strategies',
  MARKET_DATA: '/market-data',
  USER: '/user',
  FILES: '/files',
} as const;

// 完整的服务端点配置
export const SERVICE_ENDPOINTS = {
  get API_GATEWAY() { return `${SERVICE_URLS.API_GATEWAY}${API_PATHS.V1}`; },
  get AI_STRATEGY() { return `${SERVICE_URLS.AI_STRATEGY}${API_PATHS.V1}`; },
  get DATA_SERVICE() { return `${SERVICE_URLS.DATA_SERVICE}${API_PATHS.V1}`; },
  get USER_SERVICE() { return `${SERVICE_URLS.USER_SERVICE}${API_PATHS.V1}`; },
  get QLIB_SERVICE() { return `${SERVICE_URLS.QLIB_SERVICE}${API_PATHS.V1}`; },
  get STOCK_QUERY() { return `${SERVICE_URLS.STOCK_QUERY}${API_PATHS.V1}`; },
  get TRADING() { return `${SERVICE_URLS.TRADING}${API_PATHS.V1}`; },
} as const;

export default {
  PORTS: SERVICE_PORTS,
  URLS: SERVICE_URLS,
  PATHS: API_PATHS,
  ENDPOINTS: SERVICE_ENDPOINTS,
};
