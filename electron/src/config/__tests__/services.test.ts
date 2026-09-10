import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const chromeUserAgent =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/151.0.0.0 Safari/537.36';

function setUserAgent(userAgent: string): void {
  Object.defineProperty(window.navigator, 'userAgent', {
    configurable: true,
    value: userAgent,
  });
}

describe('service URL configuration', () => {
  beforeEach(() => {
    vi.resetModules();
    localStorage.clear();
    delete (window as any).electronAPI;
    setUserAgent(chromeUserAgent);
  });

  afterEach(() => {
    delete (window as any).electronAPI;
    localStorage.clear();
  });

  it('honours a configured server URL in a web browser', async () => {
    localStorage.setItem('quantmind_server_url_v2', 'http://38.76.181.214:3080');

    const services = await import('../services');

    expect(services.isElectronEnv()).toBe(false);
    expect(services.getDynamicServerUrl()).toBe('http://38.76.181.214:3080');
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('http://38.76.181.214:3080/api/v1');
  });

  it('falls back to the current origin when a web browser has no configured server URL', async () => {
    const services = await import('../services');

    expect(services.isElectronEnv()).toBe(false);
    expect(services.getDynamicServerUrl()).toBeNull();
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('/api/v1');
  });

  it('keeps a configured address in a web browser when the health probe fails', async () => {
    localStorage.setItem('quantmind_server_url_v2', 'http://10.0.0.9:8000');
    const services = await import('../services');
    const fetchSpy = vi
      .spyOn(globalThis, 'fetch')
      .mockRejectedValue(new Error('network unreachable'));

    try {
      await services.initDynamicServerUrl();

      expect(services.getDynamicServerUrl()).toBe('http://10.0.0.9:8000');
      expect(localStorage.getItem('quantmind_server_url_v2')).toBe('http://10.0.0.9:8000');
    } finally {
      fetchSpy.mockRestore();
    }
  });

  it('persists and normalises a user supplied address in a web browser', async () => {
    const services = await import('../services');

    services.setDynamicServerUrl('38.76.181.214:3080/');

    expect(services.getDynamicServerUrl()).toBe('http://38.76.181.214:3080');
    expect(localStorage.getItem('quantmind_server_url_v2')).toBe('http://38.76.181.214:3080');
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('http://38.76.181.214:3080/api/v1');

    // 清空配置后回退到当前站点（相对路径，交由 Nginx 转发）
    services.setDynamicServerUrl('');
    expect(services.getDynamicServerUrl()).toBeNull();
    expect(localStorage.getItem('quantmind_server_url_v2')).toBeNull();
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('/api/v1');
  });

  it('normalises the supported server address formats', async () => {
    const services = await import('../services');

    expect(services.normalizeServerUrl('192.168.1.100')).toBe('http://192.168.1.100');
    expect(services.normalizeServerUrl('192.168.1.100:8000')).toBe('http://192.168.1.100:8000');
    expect(services.normalizeServerUrl('https://quantmind.example.com/')).toBe('https://quantmind.example.com');
    expect(services.normalizeServerUrl('http://quantmind.example.com/api/v1')).toBe('http://quantmind.example.com');
    expect(services.normalizeServerUrl('   ')).toBeNull();
  });

  it('keeps a persisted API URL in the real Electron runtime', async () => {
    setUserAgent(`${chromeUserAgent} Electron/43.4.1`);
    (window as any).electronAPI = {};
    localStorage.setItem('quantmind_server_url_v2', 'http://38.76.181.214:8000');

    const services = await import('../services');

    expect(services.isElectronEnv()).toBe(true);
    expect(services.getDynamicServerUrl()).toBe('http://38.76.181.214:8000');
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('http://38.76.181.214:8000/api/v1');
  });
});
