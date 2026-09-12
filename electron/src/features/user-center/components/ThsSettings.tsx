import React, { useCallback, useEffect, useState } from 'react';
import {
  Activity, Database, Eye, EyeOff, Key, RefreshCw, Save, Settings2, ShieldCheck,
} from 'lucide-react';
import { Alert, Button, Input, Switch, Tag, message } from 'antd';
import {
  dataPlatformService,
  ThsSnapshotConfig,
} from '../../admin/services/dataPlatformService';

export const ThsSettings: React.FC = () => {
  const [config, setConfig] = useState<ThsSnapshotConfig | null>(null);
  const [apiKey, setApiKey] = useState('');
  const [showKey, setShowKey] = useState(false);
  const [enabled, setEnabled] = useState(false);
  const [baseUrl, setBaseUrl] = useState('');
  const [symbols, setSymbols] = useState('');
  const [indexCodes, setIndexCodes] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [verifyError, setVerifyError] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    setRefreshing(true);
    try {
      const next = await dataPlatformService.getThsSnapshotConfig();
      setConfig(next);
      setEnabled(next.snapshot_enabled);
      setBaseUrl(next.base_url || '');
      setSymbols(next.symbols || '');
      setIndexCodes(next.index_codes || '');
    } catch (error: any) {
      message.error(`加载同花顺配置失败: ${error?.message || '未知错误'}`);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleSave = async () => {
    const trimmedKey = apiKey.trim();
    if (trimmedKey && trimmedKey.length < 8) {
      message.warning('请输入完整的同花顺 API Key（至少 8 位）');
      return;
    }

    setSaving(true);
    setVerifyError(null);
    try {
      const result = await dataPlatformService.saveThsSnapshotConfig({
        ...(trimmedKey ? { api_key: trimmedKey } : {}),
        snapshot_enabled: enabled,
        base_url: baseUrl.trim(),
        symbols: symbols.trim(),
        index_codes: indexCodes.trim(),
      });
      if (result.verified === true) {
        message.success('同花顺 API Key 已保存并验证通过');
      } else if (result.verified === false) {
        setVerifyError(result.error ?? '连接验证失败，请检查 Key 或服务地址');
        message.warning('配置已保存，但连接验证未通过');
      } else {
        message.success('同花顺快照配置已保存');
      }
      setApiKey('');
      await loadData();
    } catch (error: any) {
      message.error(`保存同花顺配置失败: ${error?.message || '未知错误'}`);
    } finally {
      setSaving(false);
    }
  };

  const isConfigured = Boolean(config?.api_key_configured);

  return (
    <div className="w-full space-y-3">
      <div className="bg-white rounded-2xl border border-slate-200/80 px-5 py-3.5 shadow-xs flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-white shadow-sm">
            <Database className="w-4 h-4" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-black text-slate-800 m-0">同花顺快照</h3>
              <Tag color={isConfigured ? 'green' : 'default'} className="rounded-md text-[10px] font-bold m-0 border-0 px-1.5 py-0">
                {isConfigured ? '已授权' : '未授权'}
              </Tag>
              <Tag color={enabled ? 'blue' : 'default'} className="rounded-md text-[10px] font-bold m-0 border-0 px-1.5 py-0">
                {enabled ? '定时采集开启' : '定时采集关闭'}
              </Tag>
            </div>
            <p className="text-[11px] text-slate-400 m-0 leading-tight">
              配置选股、市场情绪、集合竞价与板块每日历史快照
            </p>
          </div>
        </div>
        <Button
          size="small"
          icon={<RefreshCw className={`w-3 h-3 ${refreshing ? 'animate-spin' : ''}`} />}
          onClick={loadData}
          loading={refreshing || loading}
          className="rounded-lg font-bold text-xs h-7 px-3"
        >
          刷新
        </Button>
      </div>

      <div className="bg-white rounded-2xl border border-slate-200/80 p-4 shadow-xs space-y-3">
        <div className="flex items-center justify-between border-b border-slate-100 pb-2">
          <div>
            <h4 className="text-xs font-bold text-slate-800 m-0">API Key 与采集策略</h4>
            <p className="text-[11px] text-slate-400 m-0">密钥保存到运行时密钥文件，页面只显示脱敏指纹</p>
          </div>
          <div className="flex items-center gap-2 text-[11px] text-slate-500">
            <Activity className="w-3.5 h-3.5 text-cyan-600" />
            <span>启用定时采集</span>
            <Switch checked={enabled} onChange={setEnabled} size="small" />
          </div>
        </div>

        <div className="flex items-center gap-2">
          <div className="relative flex-1">
            <Input
              type={showKey ? 'text' : 'password'}
              placeholder={isConfigured ? `已配置 ${config?.api_key_masked}（输入新 Key 覆盖）` : '粘贴同花顺 API Key'}
              value={apiKey}
              onChange={(event) => setApiKey(event.target.value)}
              onPressEnter={handleSave}
              className="rounded-xl h-9 font-mono text-xs pr-10"
              autoComplete="off"
              prefix={<Key className="w-3.5 h-3.5 text-slate-400" />}
            />
            <button
              type="button"
              onClick={() => setShowKey(!showKey)}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 p-1"
              title={showKey ? '隐藏明文' : '显示明文'}
            >
              {showKey ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
            </button>
          </div>
          <Button
            type="primary"
            icon={<Save className="w-3.5 h-3.5" />}
            onClick={handleSave}
            loading={saving}
            className="rounded-xl h-9 px-4 font-bold bg-cyan-600 shadow-sm text-xs shrink-0"
          >
            保存并验证
          </Button>
        </div>

        {verifyError && (
          <Alert
            type="warning"
            showIcon
            message="配置已写入，但同花顺连接验证未通过"
            description={verifyError}
            closable
            onClose={() => setVerifyError(null)}
            className="rounded-xl text-xs py-1"
          />
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5">
          <label className="text-[11px] text-slate-500">
            服务地址
            <Input value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} className="mt-1 rounded-lg h-8 font-mono text-xs" />
          </label>
          <label className="text-[11px] text-slate-500">
            指数代码（逗号分隔）
            <Input value={indexCodes} onChange={(event) => setIndexCodes(event.target.value)} className="mt-1 rounded-lg h-8 font-mono text-xs" placeholder="000300.SH,000001.SH" />
          </label>
          <label className="text-[11px] text-slate-500 md:col-span-2">
            股票范围（留空采集全市场）
            <Input.TextArea value={symbols} onChange={(event) => setSymbols(event.target.value)} className="mt-1 rounded-lg font-mono text-xs" autoSize={{ minRows: 1, maxRows: 3 }} placeholder="SH600519,SZ000001" />
          </label>
        </div>

        <div className="flex items-center gap-1.5 text-[11px] text-slate-400 bg-slate-50 px-3 py-1.5 rounded-lg border border-slate-100">
          <ShieldCheck className="w-3.5 h-3.5 text-emerald-600 shrink-0" />
          <span className="truncate">
            密钥文件：<code className="text-slate-600 font-mono">{config?.runtime_env_file || 'config/runtime.env'}</code>；定时任务由 Celery 执行，修改采集开关后需重启 beat 才会更新调度。
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="px-3 py-2 bg-white rounded-xl border border-slate-200/80 shadow-2xs flex items-center justify-between">
          <span className="text-[11px] text-slate-400 font-medium">连接状态</span>
          <span className={`font-bold text-[11px] ${isConfigured ? 'text-emerald-600' : 'text-slate-400'}`}>
            {isConfigured ? `已配置 ${config?.api_key_masked}` : '未配置'}
          </span>
        </div>
        <div className="px-3 py-2 bg-white rounded-xl border border-slate-200/80 shadow-2xs flex items-center justify-between">
          <span className="text-[11px] text-slate-400 font-medium">运行模式</span>
          <span className="font-bold text-[11px] text-slate-700 flex items-center gap-1">
            <Settings2 className="w-3 h-3" />
            {enabled ? '每日盘后 + 09:35 竞价' : '手动采集'}
          </span>
        </div>
      </div>
    </div>
  );
};

export default ThsSettings;
