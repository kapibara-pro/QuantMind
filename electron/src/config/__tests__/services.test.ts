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

  it('clears and ignores stale server URLs in a web browser', async () => {
    localStorage.setItem('quantmind_server_url_v2', 'http://38.76.181.214:3080');
    localStorage.setItem('quantmind_server_url', 'http://38.76.181.214:3080');

    const services = await import('../services');

    expect(services.isElectronEnv()).toBe(false);
    expect(localStorage.getItem('quantmind_server_url_v2')).toBeNull();
    expect(localStorage.getItem('quantmind_server_url')).toBeNull();
    expect(services.getDynamicServerUrl()).toBeNull();
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('/api/v1');

    services.setDynamicServerUrl('http://38.76.181.214:3080');
    expect(services.getDynamicServerUrl()).toBeNull();
    expect(services.SERVICE_ENDPOINTS.USER_SERVICE).toBe('/api/v1');
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
