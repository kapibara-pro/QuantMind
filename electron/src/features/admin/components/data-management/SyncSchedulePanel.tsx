import React, { useEffect, useState } from 'react';
import {
    Alert,
    Button,
    InputNumber,
    message,
    Progress,
    Select,
    Space,
    Switch,
    Tag,
    TimePicker,
    Typography,
} from 'antd';
import dayjs, { Dayjs } from 'dayjs';
import {
    ClockCircleOutlined,
    StopOutlined,
    ThunderboltOutlined,
} from '@ant-design/icons';
import { adminService } from '../../services/adminService';
import {
    dataPlatformService,
    DataSourceSyncJob,
} from '../../services/dataPlatformService';

const { Text } = Typography;
const JOB_POLL_INTERVAL_MS = 2500;
const ACTIVE_JOB_STATUSES = ['queued', 'running', 'cancelling'];

function describeError(error: unknown): string {
    const candidate = error as {
        message?: string;
        response?: { data?: { detail?: string } };
    };
    return candidate.response?.data?.detail || candidate.message || '未知错误';
}

export interface MarketSyncSchedule {
    market: string;
    label: string;
    enabled: boolean;
    time: string;
    days: number;
    datasets: string[];
    source_id: 'quantdb' | 'easy_tdx';
    publish_mode: 'shadow' | 'official';
}

interface SyncSchedulePanelProps {
    /** 市场标识: A / US / HK / BC / FUTURES */
    market: string;
    /** 该市场当前勾选的数据集（用于默认填充） */
    selectedDatasets?: string[];
    defaultDays?: number;
}

/** 每市场定时同步配置面板 — 每天 HH:MM 定时同步上游数据（精确到分钟，建议次日 00:00 以后按需错峰）。 */
export const SyncSchedulePanel: React.FC<SyncSchedulePanelProps> = ({
    market,
    selectedDatasets = [],
    defaultDays = 5,
}) => {
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [running, setRunning] = useState(false);
    const [enabled, setEnabled] = useState(false);
    const [time, setTime] = useState<Dayjs>(dayjs('00:30', 'HH:mm'));
    const [days, setDays] = useState(defaultDays);
    const [datasets, setDatasets] = useState<string[]>([]);
    const [sourceId, setSourceId] = useState<'quantdb' | 'easy_tdx'>('quantdb');
    const [datasetOptions, setDatasetOptions] = useState<Array<{ label: string; value: string }>>([]);
    const [activeJob, setActiveJob] = useState<DataSourceSyncJob | null>(null);
    const [cancelling, setCancelling] = useState(false);

    useEffect(() => {
        loadSchedule();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [market]);

    useEffect(() => {
        if (market !== 'A') {
            setActiveJob(null);
            return;
        }
        let cancelled = false;
        const recoverActiveJob = async () => {
            try {
                const response = await dataPlatformService.listDataSourceSyncJobs();
                const job = response.jobs.find(
                    (item) => item.market === market
                        && item.source_id === sourceId
                        && ACTIVE_JOB_STATUSES.includes(item.status),
                );
                if (!cancelled) setActiveJob(job ?? null);
            } catch (error) {
                console.error('[SyncSchedulePanel] recover active job failed', error);
            }
        };
        recoverActiveJob();
        return () => { cancelled = true; };
    }, [market, sourceId]);

    const activeJobId = activeJob?.job_id;
    const activeJobStatus = activeJob?.status;

    useEffect(() => {
        if (!activeJobId || !activeJobStatus || !ACTIVE_JOB_STATUSES.includes(activeJobStatus)) {
            return;
        }
        const timer = window.setInterval(async () => {
            try {
                const response = await dataPlatformService.getDataSourceSyncJob(activeJobId);
                setActiveJob(response.job);
                if (!ACTIVE_JOB_STATUSES.includes(response.job.status)) {
                    window.clearInterval(timer);
                    if (response.job.status === 'completed') {
                        const errorCount = Number(response.job.result?.error_count || 0);
                        if (errorCount > 0) {
                            message.warning(`同步完成，${errorCount} 个标的失败`);
                        } else {
                            message.success('同步完成');
                        }
                    } else if (response.job.status === 'cancelled') {
                        message.warning('同步已取消');
                    } else {
                        message.error(`同步失败: ${response.job.error || '请查看后端日志'}`);
                    }
                }
            } catch (error) {
                console.error('[SyncSchedulePanel] poll job failed', error);
            }
        }, JOB_POLL_INTERVAL_MS);
        return () => window.clearInterval(timer);
    }, [activeJobId, activeJobStatus]);

    const loadSchedule = async () => {
        setLoading(true);
        try {
            const resp = await adminService.getSyncSchedule(market);
            if (resp?.data) {
                const s = resp.data;
                setEnabled(!!s.enabled);
                setTime(dayjs(s.time, 'HH:mm').isValid() ? dayjs(s.time, 'HH:mm') : dayjs('00:30', 'HH:mm'));
                setDays(s.days ?? defaultDays);
                const nextSource = s.source_id === 'easy_tdx' ? 'easy_tdx' : 'quantdb';
                setSourceId(nextSource);
                await loadSourceDatasets(nextSource, s.datasets);
            }
        } catch (err: unknown) {
            message.error(`加载定时配置失败: ${describeError(err)}`);
        } finally {
            setLoading(false);
        }
    };

    const loadSourceDatasets = async (
        source: 'quantdb' | 'easy_tdx',
        savedDatasets?: string[],
    ) => {
        if (market !== 'A') {
            setDatasets(savedDatasets?.length ? savedDatasets : [...selectedDatasets]);
            return;
        }
        try {
            const response = await dataPlatformService.getSourceDatasets(source);
            const options = response.datasets.map((item) => ({
                label: item.label || item.dataset,
                value: item.dataset,
            }));
            setDatasetOptions(options);
            setDatasets(
                savedDatasets?.length
                    ? savedDatasets
                    : response.datasets.filter((item) => item.default).map((item) => item.dataset),
            );
        } catch (error) {
            setDatasetOptions([]);
            setDatasets(savedDatasets?.length ? savedDatasets : [...selectedDatasets]);
            throw error;
        }
    };

    const handleSourceChange = async (value: 'quantdb' | 'easy_tdx') => {
        setSourceId(value);
        setActiveJob(null);
        try {
            await loadSourceDatasets(value);
        } catch (err: unknown) {
            message.error(`加载数据集失败: ${describeError(err)}`);
        }
    };

    const handleSave = async () => {
        setSaving(true);
        try {
            await adminService.saveSyncSchedule(market, {
                enabled,
                time: time.format('HH:mm'),
                days,
                datasets,
                source_id: sourceId,
                publish_mode: sourceId === 'easy_tdx' ? 'shadow' : 'official',
            });
            message.success('定时同步配置已保存');
        } catch (err: unknown) {
            message.error(`保存定时配置失败: ${describeError(err)}`);
        } finally {
            setSaving(false);
        }
    };

    const handleRunNow = async () => {
        setRunning(true);
        try {
            const response = await adminService.runSyncScheduleNow(market, {
                enabled,
                time: time.format('HH:mm'),
                days,
                datasets,
                source_id: sourceId,
                publish_mode: sourceId === 'easy_tdx' ? 'shadow' : 'official',
            });
            if (response?.data?.job) {
                setActiveJob(response.data.job);
                message.success(`同步任务已提交: ${response.data.job.job_id}`);
            } else {
                message.success('已派发同步任务（后台执行）');
            }
        } catch (err: unknown) {
            message.error(`触发同步失败: ${describeError(err)}`);
        } finally {
            setRunning(false);
        }
    };

    const handleCancel = async () => {
        if (!activeJob) return;
        setCancelling(true);
        try {
            await dataPlatformService.cancelDataSourceSyncJob(activeJob.job_id);
            setActiveJob({ ...activeJob, status: 'cancelling' });
            message.info('取消信号已发送，当前标的完成后将停止');
        } catch (err: unknown) {
            message.error(`取消失败: ${describeError(err)}`);
        } finally {
            setCancelling(false);
        }
    };

    const isJobActive = !!activeJob && ACTIVE_JOB_STATUSES.includes(activeJob.status);
    const progress = activeJob?.status === 'completed'
        ? 100
        : activeJob?.total
            ? Math.round(((activeJob.done || 0) / activeJob.total) * 100)
            : 0;

    return (
        <div className="mt-4 p-3 rounded-lg border border-dashed border-amber-400/60 bg-amber-50/40">
            <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-semibold text-amber-700 flex items-center">
                    <ClockCircleOutlined className="mr-1" />
                    定时同步（每天自动同步上游数据，建议设置到次日 00:00 以后）
                </span>
                <Switch
                    size="small"
                    checked={enabled}
                    onChange={setEnabled}
                    loading={loading}
                    checkedChildren="开"
                    unCheckedChildren="关"
                />
            </div>
            {enabled && (
                <>
                    {market === 'A' && (
                        <div className="flex flex-wrap items-center gap-2 mb-2">
                            <span className="text-xs text-gray-600">数据中枢</span>
                            <Select
                                size="small"
                                value={sourceId}
                                onChange={handleSourceChange}
                                style={{ width: 210 }}
                                options={[
                                    { label: 'QuantDB', value: 'quantdb' },
                                    { label: 'easy_tdx 通达信行情', value: 'easy_tdx' },
                                ]}
                            />
                            {sourceId === 'easy_tdx' && <Tag color="orange">影子落盘</Tag>}
                        </div>
                    )}
                    <div className="flex flex-wrap items-center gap-2">
                        <Space size="small">
                            <span className="text-xs text-gray-600">每天</span>
                            <TimePicker
                                size="small"
                                format="HH:mm"
                                minuteStep={5}
                                value={time}
                                onChange={(v) => v && setTime(v)}
                                style={{ width: 90 }}
                            />
                            <span className="text-xs text-gray-600">同步最近</span>
                            <InputNumber
                                size="small"
                                min={1}
                                max={365}
                                value={days}
                                onChange={(v) => setDays(v ?? defaultDays)}
                                style={{ width: 70 }}
                            />
                            <span className="text-xs text-gray-600">
                                {market === 'BC' ? '个自然日' : '个交易日'}
                            </span>
                        </Space>
                    </div>
                    <div className="text-xs text-gray-500 mt-1">
                        {datasets.length > 0
                            ? `定时同步数据集: ${datasets.join(', ')}（来自当前勾选）`
                            : '未指定数据集时按各市场默认全量同步'}
                    </div>
                    {market === 'A' && datasetOptions.length > 0 && (
                        <Select
                            mode="multiple"
                            size="small"
                            className="w-full mt-2"
                            value={datasets}
                            onChange={setDatasets}
                            options={datasetOptions}
                            placeholder="选择定时同步的数据集"
                            maxTagCount="responsive"
                        />
                    )}
                    <Alert
                        className="mt-2"
                        type="info"
                        showIcon
                        message={
                            <span className="text-xs">
                                {sourceId === 'easy_tdx'
                                    ? 'easy_tdx 仅写入独立影子目录，不会覆盖 QuantDB 或直接更新训练因子。'
                                    : '同步在后台执行（Celery），到点自动触发，时区 Asia/Shanghai。'}
                            </span>
                        }
                    />
                </>
            )}
            <div className="flex gap-2 mt-2">
                <Button size="small" type="primary" ghost onClick={handleSave} loading={saving}>
                    保存定时配置
                </Button>
                <Button
                    size="small"
                    icon={<ThunderboltOutlined />}
                    onClick={handleRunNow}
                    loading={running}
                    disabled={!enabled || isJobActive}
                >
                    {isJobActive ? '同步任务执行中' : '立即同步一次'}
                </Button>
                {isJobActive && activeJob?.status !== 'cancelling' && (
                    <Button
                        size="small"
                        danger
                        icon={<StopOutlined />}
                        onClick={handleCancel}
                        loading={cancelling}
                    >
                        取消
                    </Button>
                )}
            </div>
            {activeJob && market === 'A' && (
                <div className="mt-3 border-t border-amber-200 pt-3">
                    <div className="flex items-center justify-between gap-3 mb-1">
                        <Space wrap size="small">
                            <Tag color={activeJob.status === 'failed'
                                ? 'red'
                                : activeJob.status === 'completed'
                                    ? 'green'
                                    : activeJob.status === 'cancelled'
                                        ? 'orange'
                                        : 'blue'}
                            >
                                {activeJob.status}
                            </Tag>
                            <Text className="text-xs">
                                {activeJob.current || activeJob.stage}
                            </Text>
                        </Space>
                        <Text type="secondary" className="text-xs">
                            {activeJob.done || 0}/{activeJob.total ?? '待计算'}
                        </Text>
                    </div>
                    <Progress
                        percent={progress}
                        size="small"
                        status={activeJob.status === 'failed'
                            ? 'exception'
                            : activeJob.status === 'completed'
                                ? 'success'
                                : 'active'}
                    />
                    <Text type="secondary" className="text-xs">
                        任务编号 {activeJob.job_id}
                    </Text>
                    {activeJob.error && (
                        <Alert className="mt-2" type="error" showIcon message={activeJob.error} />
                    )}
                </div>
            )}
        </div>
    );
};
