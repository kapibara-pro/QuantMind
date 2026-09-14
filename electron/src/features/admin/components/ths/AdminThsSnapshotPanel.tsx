import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
    Alert, Button, Collapse, DatePicker, Empty, Input, InputNumber, Modal,
    Space, Table, Tag, Tooltip, Typography, message,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
    ClockCircleOutlined, DatabaseOutlined, EyeOutlined, PlayCircleOutlined, ReloadOutlined,
    SearchOutlined,
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';

import {
    dataPlatformService, ThsSnapshotDataset, ThsSnapshotGroup,
    ThsSnapshotPreview,
} from '../../services/dataPlatformService';
import { describeError } from '../quantdb/utils';

const { Text } = Typography;
const DEFAULT_LIMIT = 50;

const GROUP_COLORS: Record<string, string> = {
    selection: 'blue',
    emotion: 'volcano',
    auction: 'gold',
    sector: 'cyan',
};

function formatDateTime(value?: string | null): string {
    if (!value) return '-';
    const parsed = dayjs(value);
    return parsed.isValid() ? parsed.format('YYYY-MM-DD HH:mm:ss') : value;
}

function formatPayloadValue(value: unknown): React.ReactNode {
    if (value === null || value === undefined) {
        return <Text type="secondary">null</Text>;
    }
    if (typeof value === 'object') {
        return JSON.stringify(value, null, 2);
    }
    return String(value);
}

const STANDARD_COLUMN_LABELS: Record<string, string> = {
    symbol: '代码', name: '名称', index_code: '指数代码', exchange: '市场', category: '分类',
    rank: '排名', price: '价格', change_pct: '涨跌幅', change_amount: '涨跌额',
    volume: '成交量', amount: '成交额', turnover_pct: '换手率', market_cap: '总市值',
    pe_ttm: '市盈率TTM', pe_mrq: '市盈率MRQ', pb_mrq: '市净率MRQ', ps_ttm: '市销率TTM',
    pcf_ttm: '市现率TTM', limit_up_count: '涨停数', limit_down_count: '跌停数',
    consecutive_limit_count: '连板数', seal_amount: '封单金额', sentiment_score: '情绪分',
    label: '标签', weight: '权重', metric_value: '指标值',
    event_date: '业务日期', heat: '热度', rank_change: '排名变化', rank_trend: '排名趋势',
    board_name: '板位', board_num: '连板数', sign_level: '晋级信号', seal_nextday: '次日封板',
    auction_price: '竞价价格', auction_pct: '竞价涨跌幅', auction_volume: '竞价成交量',
    auction_amount: '竞价成交额', auction_turnover_pct: '竞价换手率',
    auction_unmatched: '未匹配量', auction_volume_ratio: '竞价量比',
    auction_yesterday_ratio_pct: '较昨日成交量', float_market_cap: '流通市值',
    last_price: '最新价', open_price: '开盘价', pre_close_price: '昨收价', tags: '标签',
};

function formatStandardValue(value: unknown): React.ReactNode {
    if (value === null || value === undefined || value === '') {
        return <Text type="secondary">-</Text>;
    }
    if (typeof value === 'number') {
        return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(4);
    }
    if (typeof value === 'boolean') {
        return value ? '是' : '否';
    }
    if (typeof value === 'object') {
        return JSON.stringify(value);
    }
    return String(value);
}

interface PreviewModalProps {
    dataset: ThsSnapshotDataset | null;
    onClose: () => void;
}

function ThsSnapshotPreviewModal({ dataset, onClose }: PreviewModalProps) {
    const [preview, setPreview] = useState<ThsSnapshotPreview | null>(null);
    const [snapshotDate, setSnapshotDate] = useState<Dayjs | null>(null);
    const [symbol, setSymbol] = useState('');
    const [limit, setLimit] = useState(DEFAULT_LIMIT);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const load = useCallback(async (
        selectedDataset: ThsSnapshotDataset,
        dateValue: Dayjs | null,
        symbolValue: string,
        rowLimit: number,
    ) => {
        setLoading(true);
        setError(null);
        try {
            const result = await dataPlatformService.previewThsSnapshot({
                dataset: selectedDataset.dataset,
                snapshot_date: dateValue?.format('YYYY-MM-DD'),
                symbol: symbolValue.trim() || undefined,
                limit: rowLimit,
            });
            setPreview(result);
            if (!dateValue && result.snapshot_date) {
                setSnapshotDate(dayjs(result.snapshot_date));
            }
        } catch (loadError: unknown) {
            setPreview(null);
            setError(describeError(loadError));
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        if (!dataset) {
            setPreview(null);
            return;
        }
        const latestDate = dataset.end_date ? dayjs(dataset.end_date) : null;
        setSnapshotDate(latestDate);
        setSymbol('');
        setLimit(DEFAULT_LIMIT);
        load(dataset, latestDate, '', DEFAULT_LIMIT);
    }, [dataset, load]);

    const rows = preview?.data ?? [];
    const columns: ColumnsType<(typeof rows)[number]> = (preview?.columns ?? []).map((column) => ({
        title: (
            <Space direction="vertical" size={0}>
                <Text strong>{STANDARD_COLUMN_LABELS[column.name] ?? column.name}</Text>
                <Text type="secondary" className="text-xs">{column.name} · {column.dtype}</Text>
            </Space>
        ),
        dataIndex: column.name,
        key: column.name,
        width: 150,
        ellipsis: true,
        render: formatStandardValue,
    }));

    return (
        <Modal
            open={dataset !== null}
            onCancel={onClose}
            width="90%"
            title={dataset ? `${dataset.name} · ${dataset.dataset}` : ''}
            footer={null}
            destroyOnHidden
        >
            <Space direction="vertical" className="w-full" size="middle">
                <Space wrap>
                    <DatePicker
                        value={snapshotDate}
                        onChange={setSnapshotDate}
                        allowClear
                        placeholder="最新快照"
                    />
                    <Input
                        value={symbol}
                        onChange={(event) => setSymbol(event.target.value)}
                        onPressEnter={() => dataset && load(dataset, snapshotDate, symbol, limit)}
                        prefix={<SearchOutlined />}
                        placeholder="代码或名称"
                        allowClear
                        style={{ width: 200 }}
                    />
                    <InputNumber
                        min={1}
                        max={200}
                        value={limit}
                        onChange={(value) => setLimit(value ?? DEFAULT_LIMIT)}
                        addonBefore="行数"
                        style={{ width: 145 }}
                    />
                    <Button
                        type="primary"
                        icon={<SearchOutlined />}
                        loading={loading}
                        onClick={() => dataset && load(dataset, snapshotDate, symbol, limit)}
                    >
                        查询
                    </Button>
                </Space>

                {preview && (
                    <Space wrap size="small">
                        <Tag color="green">PostgreSQL 标准化表</Tag>
                        <Tag>{preview.snapshot_date || '无快照日期'}</Tag>
                        <Tag>{preview.rows_total.toLocaleString()} 条记录</Tag>
                        <Tag>{preview.column_count} 个标准字段</Tag>
                    </Space>
                )}

                {error && <Alert type="error" showIcon message="加载失败" description={error} />}

                {rows.length > 0 ? (
                    <Table
                        dataSource={rows}
                        columns={columns}
                        rowKey="id"
                        size="small"
                        loading={loading}
                        pagination={{ pageSize: 20, size: 'small', showSizeChanger: true }}
                        scroll={{ x: 'max-content', y: 440 }}
                        expandable={{
                            expandedRowRender: (row) => (
                                <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-8 gap-y-2 p-3 bg-slate-50">
                                    <div className="grid grid-cols-[150px_minmax(0,1fr)] gap-3 text-xs">
                                        <Text type="secondary">内部范围键</Text>
                                        <pre className="m-0 whitespace-pre-wrap break-all font-mono text-slate-700">
                                            {row.scope_key}
                                        </pre>
                                    </div>
                                    {Object.entries(row.extra ?? {}).map(([key, value]) => (
                                        <div key={key} className="grid grid-cols-[150px_minmax(0,1fr)] gap-3 text-xs">
                                            <Text type="secondary" className="break-all">{key}</Text>
                                            <pre className="m-0 whitespace-pre-wrap break-all font-mono text-slate-700">
                                                {formatPayloadValue(value)}
                                            </pre>
                                        </div>
                                    ))}
                                </div>
                            ),
                        }}
                    />
                ) : (
                    !loading && !error && <Empty description="该日期暂无快照" />
                )}
            </Space>
        </Modal>
    );
}

export function AdminThsSnapshotPanel() {
    const [groups, setGroups] = useState<ThsSnapshotGroup[]>([]);
    const [datasets, setDatasets] = useState<ThsSnapshotDataset[]>([]);
    const [tableReady, setTableReady] = useState(false);
    const [standardizedTableReady, setStandardizedTableReady] = useState(false);
    const [apiKeyConfigured, setApiKeyConfigured] = useState(false);
    const [scheduleEnabled, setScheduleEnabled] = useState(false);
    const [loading, setLoading] = useState(false);
    const [previewDataset, setPreviewDataset] = useState<ThsSnapshotDataset | null>(null);
    const [collectingMode, setCollectingMode] = useState<'daily' | 'auction' | null>(null);
    const [collectTaskId, setCollectTaskId] = useState<string | null>(null);
    const [collectError, setCollectError] = useState<string | null>(null);

    const loadCatalog = useCallback(async () => {
        setLoading(true);
        try {
            const result = await dataPlatformService.getThsSnapshotCatalog();
            setGroups(result.groups ?? []);
            setDatasets(result.datasets ?? []);
            setTableReady(result.table_ready);
            setStandardizedTableReady(Boolean(result.standardized_table_ready));
            setApiKeyConfigured(result.api_key_configured);
            setScheduleEnabled(result.schedule_enabled);
        } catch (loadError: unknown) {
            message.error(`加载同花顺快照目录失败: ${describeError(loadError)}`);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        loadCatalog();
    }, [loadCatalog]);

    const startCollection = useCallback(async (mode: 'daily' | 'auction') => {
        setCollectingMode(mode);
        setCollectTaskId(null);
        setCollectError(null);
        try {
            const result = await dataPlatformService.startThsSnapshotCollection(mode);
            setCollectTaskId(result.task_id);
            message.info(mode === 'auction' ? '竞价快照任务已加入队列' : '盘后快照任务已加入队列');
        } catch (error: unknown) {
            setCollectingMode(null);
            setCollectError(describeError(error));
            message.error(`立即采集失败: ${describeError(error)}`);
        }
    }, []);

    useEffect(() => {
        if (!collectTaskId) return undefined;
        let active = true;
        const poll = async () => {
            try {
                const result = await dataPlatformService.getThsSnapshotCollectionStatus(collectTaskId);
                if (!active) return;
                if (result.status === 'success') {
                    message.success('同花顺快照采集完成，目录已刷新');
                    setCollectingMode(null);
                    setCollectTaskId(null);
                    await loadCatalog();
                } else if (['failure', 'revoked'].includes(result.status)) {
                    setCollectError(result.error || '采集任务执行失败');
                    setCollectingMode(null);
                    setCollectTaskId(null);
                }
            } catch (error: unknown) {
                if (active) {
                    setCollectError(describeError(error));
                    setCollectingMode(null);
                    setCollectTaskId(null);
                }
            }
        };
        poll();
        const timer = window.setInterval(poll, 2000);
        return () => {
            active = false;
            window.clearInterval(timer);
        };
    }, [collectTaskId, loadCatalog]);

    const datasetsByGroup = useMemo(() => {
        const grouped = new Map<string, ThsSnapshotDataset[]>();
        for (const item of datasets) {
            grouped.set(item.group, [...(grouped.get(item.group) ?? []), item]);
        }
        return grouped;
    }, [datasets]);

    const columns: ColumnsType<ThsSnapshotDataset> = [
        {
            title: '数据集',
            dataIndex: 'name',
            width: 190,
            render: (name: string, row) => (
                <Space direction="vertical" size={0}>
                    <Text strong>{name}</Text>
                    <Text type="secondary" className="text-xs">{row.dataset}</Text>
                </Space>
            ),
        },
        {
            title: '状态',
            dataIndex: 'available',
            width: 90,
            align: 'center',
            render: (available: boolean) => (
                <Tag color={available ? 'green' : 'default'}>
                    {available ? '有数据' : '待采集'}
                </Tag>
            ),
        },
        {
            title: '历史区间',
            key: 'range',
            width: 190,
            render: (_, row) => (
                row.start_date ? `${row.start_date} 至 ${row.end_date}` : '-'
            ),
        },
        {
            title: '快照日',
            dataIndex: 'snapshot_days',
            width: 90,
            align: 'right',
            render: (value: number) => value.toLocaleString(),
        },
        {
            title: '最新记录',
            dataIndex: 'latest_rows',
            width: 100,
            align: 'right',
            render: (value: number) => value.toLocaleString(),
        },
        {
            title: '累计记录',
            dataIndex: 'rows_total',
            width: 110,
            align: 'right',
            render: (value: number) => value.toLocaleString(),
        },
        {
            title: '最近采集',
            dataIndex: 'updated_at',
            width: 170,
            render: formatDateTime,
        },
        {
            title: '说明',
            dataIndex: 'note',
            ellipsis: true,
            render: (note: string) => <Text type="secondary" className="text-xs">{note}</Text>,
        },
        {
            title: '',
            key: 'action',
            width: 54,
            fixed: 'right',
            render: (_, row) => (
                <Tooltip title="预览快照">
                    <Button
                        type="text"
                        icon={<EyeOutlined />}
                        disabled={!row.available}
                        aria-label={`预览${row.name}`}
                        onClick={() => setPreviewDataset(row)}
                    />
                </Tooltip>
            ),
        },
    ];

    const availableCount = datasets.filter((item) => item.available).length;
    const totalRows = datasets.reduce((sum, item) => sum + item.rows_total, 0);

    return (
        <div className="space-y-4">
            <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4 px-1">
                <Space size="middle" wrap>
                    <Space size="small">
                        <DatabaseOutlined className="text-cyan-600" />
                        <Text strong>同花顺每日快照</Text>
                    </Space>
                    <Tag color={tableReady ? 'green' : 'default'}>
                        {tableReady ? '存储已就绪' : '尚未建表'}
                    </Tag>
                    <Tag color={standardizedTableReady ? 'green' : 'default'}>
                        {standardizedTableReady ? '标准化表已就绪' : '标准化表待初始化'}
                    </Tag>
                    <Tag color={apiKeyConfigured ? 'blue' : 'default'}>
                        {apiKeyConfigured ? '密钥已配置' : '密钥未配置'}
                    </Tag>
                    <Tag color={scheduleEnabled ? 'gold' : 'default'} icon={<ClockCircleOutlined />}>
                        {scheduleEnabled ? '定时采集中' : '定时采集关闭'}
                    </Tag>
                </Space>
                <Space wrap>
                    <Button
                        type="primary"
                        icon={<PlayCircleOutlined />}
                        onClick={() => startCollection('daily')}
                        loading={collectingMode === 'daily'}
                        disabled={collectingMode !== null || !apiKeyConfigured}
                    >
                        立即采集盘后
                    </Button>
                    <Button
                        icon={<PlayCircleOutlined />}
                        onClick={() => startCollection('auction')}
                        loading={collectingMode === 'auction'}
                        disabled={collectingMode !== null || !apiKeyConfigured}
                    >
                        立即采集竞价
                    </Button>
                    <Button icon={<ReloadOutlined />} onClick={loadCatalog} loading={loading}>
                        刷新
                    </Button>
                </Space>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-3 border border-slate-200 bg-slate-50">
                <div className="px-4 py-3 border-b sm:border-b-0 sm:border-r border-slate-200">
                    <Text type="secondary" className="text-xs block">已采集数据集</Text>
                    <Text strong className="text-lg">{availableCount} / {datasets.length}</Text>
                </div>
                <div className="px-4 py-3 border-b sm:border-b-0 sm:border-r border-slate-200">
                    <Text type="secondary" className="text-xs block">累计快照记录</Text>
                    <Text strong className="text-lg">{totalRows.toLocaleString()}</Text>
                </div>
                <div className="px-4 py-3">
                    <Text type="secondary" className="text-xs block">存储类型</Text>
                    <Text strong className="text-lg">标准化表 + 原始 JSONB</Text>
                </div>
            </div>

            {!apiKeyConfigured && (
                <Alert type="warning" showIcon message="同花顺 API Key 尚未配置" />
            )}
            {collectError && (
                <Alert
                    type="error"
                    showIcon
                    closable
                    onClose={() => setCollectError(null)}
                    message="同花顺即时采集失败"
                    description={collectError}
                />
            )}
            {collectingMode && collectTaskId && (
                <Alert
                    type="info"
                    showIcon
                    message={collectingMode === 'auction' ? '竞价快照采集中' : '盘后快照采集中'}
                    description="任务在后台执行，完成后目录会自动刷新。"
                />
            )}

            <Collapse
                defaultActiveKey={groups.map((group) => group.id)}
                items={groups.map((group) => ({
                    key: group.id,
                    label: (
                        <Space>
                            <Text strong>{group.name}</Text>
                            <Tag color={GROUP_COLORS[group.id]}>
                                {group.available_count}/{group.dataset_count} 有数据
                            </Tag>
                            <Text type="secondary" className="text-xs">
                                {group.rows_total.toLocaleString()} 条
                            </Text>
                        </Space>
                    ),
                    children: (
                        <Table
                            dataSource={datasetsByGroup.get(group.id) ?? []}
                            columns={columns}
                            rowKey="dataset"
                            size="small"
                            pagination={false}
                            loading={loading}
                            scroll={{ x: 1180 }}
                        />
                    ),
                }))}
            />

            {!loading && datasets.length === 0 && <Empty description="目录暂无数据" />}

            <ThsSnapshotPreviewModal
                dataset={previewDataset}
                onClose={() => setPreviewDataset(null)}
            />
        </div>
    );
}

export default AdminThsSnapshotPanel;
