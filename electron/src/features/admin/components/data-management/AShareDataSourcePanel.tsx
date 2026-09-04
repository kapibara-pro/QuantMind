import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
    Alert,
    Button,
    InputNumber,
    Progress,
    Segmented,
    Select,
    Space,
    Table,
    Tag,
    Typography,
    message,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
    CloudDownloadOutlined,
    CloudSyncOutlined,
    ReloadOutlined,
    StopOutlined,
    SwapOutlined,
} from '@ant-design/icons';
import {
    dataPlatformService,
    DataSourceDataset,
    DataSourceSyncJob,
    DataSyncSource,
    EasyTdxServer,
} from '../../services/dataPlatformService';

const { Text } = Typography;
const JOB_POLL_INTERVAL_MS = 2500;

const SOURCE_FALLBACKS: DataSyncSource[] = [
    {
        source_id: 'quantdb',
        label: 'QuantDB',
        adapter_name: 'quantdb_local',
        category: 'research_data',
        transport: 'parquet',
        markets: ['A'],
        delivery_modes: ['batch'],
        capabilities: [],
        datasets: [],
        configurable: true,
        managed_service: false,
        sync_supported: true,
        registered: true,
        notes: '正式研究数据与因子数据中枢',
    },
    {
        source_id: 'easy_tdx',
        label: 'easy_tdx 通达信行情',
        adapter_name: 'easy_tdx',
        category: 'market_data',
        transport: 'tcp',
        markets: ['A'],
        delivery_modes: ['batch', 'realtime_pull'],
        capabilities: [],
        datasets: [],
        configurable: true,
        managed_service: true,
        sync_supported: true,
        registered: false,
        notes: '行情影子数据源',
    },
];

export const AShareDataSourcePanel: React.FC = () => {
    const [sources, setSources] = useState<DataSyncSource[]>(SOURCE_FALLBACKS);
    const [sourceId, setSourceId] = useState<'quantdb' | 'easy_tdx'>('quantdb');
    const [sourceEnabled, setSourceEnabled] = useState<Record<string, boolean>>({});
    const [datasets, setDatasets] = useState<DataSourceDataset[]>([]);
    const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
    const [days, setDays] = useState(5);
    const [checking, setChecking] = useState(false);
    const [syncing, setSyncing] = useState(false);
    const [diff, setDiff] = useState<any>(null);
    const [activeJob, setActiveJob] = useState<DataSourceSyncJob | null>(null);
    const [channel, setChannel] = useState<'standard' | 'mac'>('mac');
    const [serverInfo, setServerInfo] = useState<{
        available: boolean;
        version?: string | null;
        channels: Record<'standard' | 'mac', EasyTdxServer[]>;
    } | null>(null);
    const [testingServers, setTestingServers] = useState(false);

    const loadDatasets = useCallback(async (nextSource: 'quantdb' | 'easy_tdx') => {
        const response = await dataPlatformService.getSourceDatasets(nextSource);
        setDatasets(response.datasets);
        setSelectedDatasets(
            response.datasets.filter((item) => item.default).map((item) => item.dataset),
        );
    }, []);

    const loadServers = useCallback(async () => {
        try {
            const response = await dataPlatformService.getEasyTdxServers();
            setServerInfo(response);
        } catch (error: unknown) {
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`加载通达信节点失败: ${detail}`);
        }
    }, []);

    useEffect(() => {
        let cancelled = false;

        const initialize = async () => {
            const [sourceResponse, configResponse] = await Promise.all([
                dataPlatformService.listSources().catch(() => null),
                dataPlatformService.getMarketDataSources('quantdb').catch(() => null),
            ]);
            if (cancelled) return;

            const selectable = (sourceResponse?.sync_sources || []).filter(
                (item) => item.markets.includes('A') && item.sync_supported,
            );
            if (selectable.length) setSources(selectable);

            const configured = configResponse?.sources || [];
            setSourceEnabled(Object.fromEntries(
                configured.map((item) => [item.source, item.enabled]),
            ));
            const quantdbEnabled = configured.find((item) => item.source === 'quantdb')?.enabled ?? true;
            const easyTdxAvailable = selectable.some(
                (item) => item.source_id === 'easy_tdx' && item.registered,
            );
            const nextSource: 'quantdb' | 'easy_tdx' =
                !quantdbEnabled && easyTdxAvailable ? 'easy_tdx' : 'quantdb';
            setSourceId(nextSource);
            try {
                await loadDatasets(nextSource);
            } catch (error) {
                console.error('[AShareDataSourcePanel] load initial datasets failed', error);
            }
        };

        initialize();
        return () => { cancelled = true; };
    }, [loadDatasets]);

    useEffect(() => {
        if (sourceId === 'easy_tdx') loadServers();
    }, [loadServers, sourceId]);

    useEffect(() => {
        if (!activeJob || !['queued', 'running', 'cancelling'].includes(activeJob.status)) {
            return;
        }
        const timer = window.setInterval(async () => {
            try {
                const response = await dataPlatformService.getDataSourceSyncJob(activeJob.job_id);
                setActiveJob(response.job);
                if (!['queued', 'running', 'cancelling'].includes(response.job.status)) {
                    window.clearInterval(timer);
                    setSyncing(false);
                    if (response.job.status === 'completed') {
                        message.success(`${response.job.source_id} 同步完成`);
                        loadDatasets(sourceId);
                    } else if (response.job.status === 'failed') {
                        message.error(`同步失败: ${response.job.error || '请查看后端日志'}`);
                    }
                }
            } catch (error) {
                console.error('[AShareDataSourcePanel] poll job failed', error);
            }
        }, JOB_POLL_INTERVAL_MS);
        return () => window.clearInterval(timer);
    }, [activeJob?.job_id, activeJob?.status, loadDatasets, sourceId]);

    const handleSourceChange = async (value: 'quantdb' | 'easy_tdx') => {
        setSourceId(value);
        setDiff(null);
        setActiveJob(null);
        try {
            await loadDatasets(value);
        } catch (error: unknown) {
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`加载数据集失败: ${detail}`);
        }
    };

    const checkUpdates = async () => {
        if (sourceId === 'quantdb' && sourceEnabled.quantdb === false) {
            message.warning('QuantDB 数据源未启用，请选择 easy_tdx 或先启用 QuantDB');
            return;
        }
        setChecking(true);
        try {
            const response = await dataPlatformService.checkSourceUpdates(
                sourceId,
                selectedDatasets,
            );
            setDiff(response);
        } catch (error: unknown) {
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`检查更新失败: ${detail}`);
        } finally {
            setChecking(false);
        }
    };

    const startSync = async () => {
        if (sourceId === 'quantdb' && sourceEnabled.quantdb === false) {
            message.warning('QuantDB 数据源未启用，请选择 easy_tdx 或先启用 QuantDB');
            return;
        }
        if (!selectedDatasets.length) {
            message.warning('请至少选择一个数据集');
            return;
        }
        setSyncing(true);
        try {
            const response = await dataPlatformService.createDataSourceSyncJob({
                source_id: sourceId,
                market: 'A',
                datasets: selectedDatasets,
                days,
                publish_mode: sourceId === 'easy_tdx' ? 'shadow' : 'official',
            });
            setActiveJob(response.job);
            message.success(`同步任务已提交: ${response.job.job_id}`);
        } catch (error: unknown) {
            setSyncing(false);
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`提交同步失败: ${detail}`);
        }
    };

    const cancelSync = async () => {
        if (!activeJob) return;
        try {
            await dataPlatformService.cancelDataSourceSyncJob(activeJob.job_id);
            setActiveJob({ ...activeJob, status: 'cancelling' });
        } catch (error: unknown) {
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`取消失败: ${detail}`);
        }
    };

    const testServers = async () => {
        setTestingServers(true);
        try {
            await dataPlatformService.testEasyTdxServers(channel);
            await loadServers();
            message.success(`${channel === 'mac' ? 'MAC' : '标准'}节点测速完成`);
        } catch (error: unknown) {
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`节点测速失败: ${detail}`);
        } finally {
            setTestingServers(false);
        }
    };

    const switchServer = async (server: EasyTdxServer) => {
        try {
            await dataPlatformService.switchEasyTdxServer(channel, server.host);
            await loadServers();
            message.success(`已切换到 ${server.host}:${server.port}`);
        } catch (error: unknown) {
            const detail = error instanceof Error ? error.message : '未知错误';
            message.error(`切换节点失败: ${detail}`);
        }
    };

    const serverRows = serverInfo?.channels?.[channel] || [];
    const source = sources.find((item) => item.source_id === sourceId);
    const progress = activeJob?.total
        ? Math.round(((activeJob.done || 0) / activeJob.total) * 100)
        : 0;

    const serverColumns = useMemo<ColumnsType<EasyTdxServer>>(
        () => [
            {
                title: '节点',
                dataIndex: 'host',
                render: (host: string, row) => (
                    <Space size="small">
                        <Text code>{host}:{row.port}</Text>
                        {row.selected && <Tag color="blue">当前</Tag>}
                    </Space>
                ),
            },
            {
                title: '状态',
                dataIndex: 'status',
                width: 90,
                render: (status: EasyTdxServer['status']) => (
                    <Tag color={status === 'online' ? 'green' : status === 'offline' ? 'red' : 'default'}>
                        {status === 'online' ? '在线' : status === 'offline' ? '离线' : '未检测'}
                    </Tag>
                ),
            },
            {
                title: '延迟',
                dataIndex: 'latency_ms',
                width: 90,
                render: (value?: number) => (value == null ? '—' : `${value.toFixed(0)} ms`),
            },
            {
                title: '操作',
                width: 90,
                render: (_, row) => (
                    <Button
                        type="text"
                        size="small"
                        icon={<SwapOutlined />}
                        disabled={row.selected || row.status === 'offline'}
                        onClick={() => switchServer(row)}
                    >
                        切换
                    </Button>
                ),
            },
        ],
        [channel],
    );

    return (
        <section className="border border-slate-200 bg-white p-4 rounded-lg">
            <div className="flex flex-wrap items-center justify-between gap-3 mb-3">
                <Space wrap>
                    <Text strong>A 股数据更新与同步</Text>
                    <Select
                        value={sourceId}
                        onChange={handleSourceChange}
                        disabled={syncing}
                        style={{ width: 220 }}
                        options={sources.map((item) => ({
                            label: item.label,
                            value: item.source_id,
                            disabled: (item.source_id === 'easy_tdx' && !item.registered)
                                || (item.source_id === 'quantdb' && sourceEnabled.quantdb === false),
                        }))}
                    />
                    <Tag color={sourceId === 'easy_tdx' ? 'orange' : 'blue'}>
                        {sourceId === 'easy_tdx' ? '影子数据' : '正式数据'}
                    </Tag>
                </Space>
                <Space wrap>
                    <InputNumber
                        min={1}
                        max={3650}
                        value={days}
                        onChange={(value) => setDays(value || 5)}
                        disabled={syncing}
                        addonAfter="交易日"
                        style={{ width: 130 }}
                    />
                    <Button
                        icon={<CloudDownloadOutlined />}
                        loading={checking}
                        disabled={syncing || !selectedDatasets.length}
                        onClick={checkUpdates}
                    >
                        检查更新
                    </Button>
                    <Button
                        type="primary"
                        icon={<CloudSyncOutlined />}
                        loading={syncing}
                        disabled={checking || !selectedDatasets.length}
                        onClick={startSync}
                    >
                        同步数据
                    </Button>
                </Space>
            </div>

            <Select
                mode="multiple"
                value={selectedDatasets}
                onChange={setSelectedDatasets}
                disabled={syncing || checking}
                className="w-full"
                options={datasets.map((item) => ({
                    label: `${item.label || item.dataset}${item.end_date ? ` (${item.end_date})` : ''}`,
                    value: item.dataset,
                }))}
                placeholder="选择要检查和同步的数据集"
                maxTagCount="responsive"
            />

            <div className="mt-2 text-xs text-slate-500">{source?.notes}</div>
            {sourceId === 'easy_tdx' && (
                <Alert
                    className="mt-3"
                    type="warning"
                    showIcon
                    message="easy_tdx 只提供行情，不包含 QuantDB 的财务、估值及 L1/L2 因子；本阶段不会写入 PG 或 Qlib。"
                />
            )}

            {diff?.summary && (
                <div className="flex flex-wrap gap-2 mt-3">
                    <Tag color="green">已最新 {diff.summary.up_to_date || 0}</Tag>
                    <Tag color="orange">有更新 {diff.summary.updates_available || 0}</Tag>
                    <Tag>未同步 {diff.summary.not_synced || 0}</Tag>
                    {diff.remote_latest_trade_date && (
                        <Text type="secondary" className="text-xs">
                            上游最新交易日 {diff.remote_latest_trade_date}
                        </Text>
                    )}
                </div>
            )}

            {activeJob && (
                <div className="mt-3 border-t border-slate-100 pt-3">
                    <div className="flex items-center justify-between gap-3">
                        <Space wrap>
                            <Tag color={activeJob.status === 'failed' ? 'red' : 'blue'}>
                                {activeJob.status}
                            </Tag>
                            <Text className="text-xs">{activeJob.current || activeJob.stage}</Text>
                        </Space>
                        {['queued', 'running'].includes(activeJob.status) && (
                            <Button
                                size="small"
                                danger
                                icon={<StopOutlined />}
                                onClick={cancelSync}
                            >
                                取消
                            </Button>
                        )}
                    </div>
                    <Progress percent={progress} size="small" status={activeJob.status === 'failed' ? 'exception' : 'active'} />
                </div>
            )}

            {sourceId === 'easy_tdx' && (
                <div className="mt-4 border-t border-slate-200 pt-3">
                    <div className="flex items-center justify-between gap-3 mb-2">
                        <Space wrap>
                            <Text strong className="text-sm">通达信服务器健康</Text>
                            {serverInfo?.version && <Tag>easy-tdx {serverInfo.version}</Tag>}
                            <Segmented
                                size="small"
                                value={channel}
                                onChange={(value) => setChannel(value as 'standard' | 'mac')}
                                options={[
                                    { label: 'MAC 行情', value: 'mac' },
                                    { label: '标准行情', value: 'standard' },
                                ]}
                            />
                        </Space>
                        <Button
                            size="small"
                            icon={<ReloadOutlined />}
                            loading={testingServers}
                            onClick={testServers}
                        >
                            全部测速
                        </Button>
                    </div>
                    {serverInfo?.available === false ? (
                        <Alert type="error" showIcon message="后端尚未安装 easy-tdx 依赖" />
                    ) : (
                        <Table
                            rowKey={(row) => `${row.channel}-${row.host}-${row.port}`}
                            size="small"
                            columns={serverColumns}
                            dataSource={serverRows}
                            pagination={false}
                            scroll={{ y: 240 }}
                        />
                    )}
                </div>
            )}
        </section>
    );
};
