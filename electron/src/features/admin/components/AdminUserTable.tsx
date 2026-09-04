import React, { useEffect, useMemo, useState } from 'react';
import {
    Button,
    Form,
    Input,
    message,
    Modal,
    Popconfirm,
    Space,
    Switch,
    Table,
    Tag,
    Tooltip,
    Typography,
} from 'antd';
import type { TablePaginationConfig } from 'antd/es/table';
import {
    EditOutlined,
    KeyOutlined,
    PlusOutlined,
    SearchOutlined,
} from '@ant-design/icons';
import type { AxiosError } from 'axios';
import { adminService } from '../services/adminService';
import type {
    AdminUser,
    AdminUserCreateRequest,
    AdminUserUpdateRequest,
} from '../types';

const { Text } = Typography;

type UserFormValues = AdminUserCreateRequest & { confirm_password?: string };
type PasswordFormValues = { new_password: string; confirm_password: string };

const PASSWORD_HELP = '至少 8 位，且包含大写字母、小写字母和数字';

const getErrorMessage = (error: unknown, fallback: string): string => {
    const apiError = error as AxiosError<{ detail?: string; message?: string }>;
    return apiError.response?.data?.detail || apiError.response?.data?.message || fallback;
};

export const AdminUserTable: React.FC = () => {
    const [users, setUsers] = useState<AdminUser[]>([]);
    const [loading, setLoading] = useState(true);
    const [submitting, setSubmitting] = useState(false);
    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const [searchText, setSearchText] = useState('');
    const [editingUser, setEditingUser] = useState<AdminUser | null>(null);
    const [passwordUser, setPasswordUser] = useState<AdminUser | null>(null);
    const [userModalOpen, setUserModalOpen] = useState(false);
    const [userForm] = Form.useForm<UserFormValues>();
    const [passwordForm] = Form.useForm<PasswordFormValues>();

    const currentUserId = useMemo(() => {
        try {
            const storedUser = JSON.parse(localStorage.getItem('user') || '{}');
            return String(storedUser.user_id ?? storedUser.id ?? '');
        } catch {
            return '';
        }
    }, []);

    const loadUsers = async (query?: string, targetPage = page) => {
        setLoading(true);
        try {
            const data = await adminService.listUsers(query, targetPage, 12);
            setUsers(data.items);
            setTotal(data.total);
            setPage(targetPage);
        } catch (error) {
            message.error(getErrorMessage(error, '加载用户列表失败'));
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void loadUsers(undefined, 1);
    }, []);

    useEffect(() => {
        if (!userModalOpen) return;

        userForm.resetFields();
        if (editingUser) {
            userForm.setFieldsValue({
                username: editingUser.username,
                email: editingUser.email || '',
                is_active: editingUser.is_active,
                is_admin: editingUser.is_admin,
            });
            return;
        }
        userForm.setFieldsValue({ is_active: true, is_admin: false });
    }, [editingUser, userForm, userModalOpen]);

    const handleToggleStatus = async (record: AdminUser) => {
        try {
            const success = await adminService.toggleUserStatus(record.user_id);
            if (success) {
                message.success(record.is_active ? '用户已禁用' : '用户已启用');
                await loadUsers(searchText || undefined, page);
            }
        } catch (error) {
            message.error(getErrorMessage(error, '状态更新失败'));
        }
    };

    const handleSearch = (value: string) => {
        const query = value.trim();
        setSearchText(query);
        void loadUsers(query || undefined, 1);
    };

    const openCreateModal = () => {
        setEditingUser(null);
        setUserModalOpen(true);
    };

    const openEditModal = (user: AdminUser) => {
        setEditingUser(user);
        setUserModalOpen(true);
    };

    const closeUserModal = () => {
        if (submitting) return;
        setUserModalOpen(false);
        setEditingUser(null);
        userForm.resetFields();
    };

    const submitUser = async () => {
        try {
            const values = await userForm.validateFields();
            setSubmitting(true);
            const payload: AdminUserUpdateRequest = {
                username: values.username.trim(),
                email: values.email.trim(),
                is_active: values.is_active,
                is_admin: values.is_admin,
            };

            if (editingUser) {
                await adminService.updateUser(editingUser.user_id, payload);
                message.success('用户信息已更新');
            } else {
                await adminService.createUser({ ...payload, password: values.password });
                message.success('用户已创建');
            }
            setUserModalOpen(false);
            setEditingUser(null);
            userForm.resetFields();
            await loadUsers(searchText || undefined, editingUser ? page : 1);
        } catch (error) {
            if ((error as { errorFields?: unknown[] })?.errorFields) return;
            message.error(getErrorMessage(error, editingUser ? '更新用户失败' : '创建用户失败'));
        } finally {
            setSubmitting(false);
        }
    };

    const openPasswordModal = (user: AdminUser) => {
        setPasswordUser(user);
        passwordForm.resetFields();
    };

    const closePasswordModal = () => {
        if (submitting) return;
        setPasswordUser(null);
        passwordForm.resetFields();
    };

    const submitPassword = async () => {
        if (!passwordUser) return;
        try {
            const values = await passwordForm.validateFields();
            setSubmitting(true);
            await adminService.resetUserPassword(passwordUser.user_id, values.new_password);
            message.success(
                passwordUser.user_id === currentUserId
                    ? '密码已修改，请使用新密码重新登录'
                    : '密码已重置，用户需要重新登录',
            );
            const resetCurrentUser = passwordUser.user_id === currentUserId;
            setPasswordUser(null);
            passwordForm.resetFields();
            if (resetCurrentUser) {
                window.setTimeout(() => {
                    localStorage.removeItem('access_token');
                    localStorage.removeItem('refresh_token');
                    localStorage.removeItem('user');
                    localStorage.removeItem('remember_login');
                    window.location.hash = '#/auth/login';
                    window.location.reload();
                }, 800);
            }
        } catch (error) {
            if ((error as { errorFields?: unknown[] })?.errorFields) return;
            message.error(getErrorMessage(error, '重置密码失败'));
        } finally {
            setSubmitting(false);
        }
    };

    const handleTableChange = (pagination: TablePaginationConfig) => {
        const nextPage = pagination.current || 1;
        if (nextPage !== page) {
            void loadUsers(searchText || undefined, nextPage);
        }
    };

    const columns = [
        {
            title: '用户ID',
            dataIndex: 'user_id',
            key: 'user_id',
            width: 130,
            render: (id: string) => <code className="text-xs text-slate-500">{id}</code>,
        },
        {
            title: '用户名',
            dataIndex: 'username',
            key: 'username',
            render: (name: string, record: AdminUser) => (
                <div>
                    <div className="font-semibold text-slate-800">
                        {name}
                        {record.user_id === currentUserId && (
                            <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>当前账号</Text>
                        )}
                    </div>
                    <div className="text-xs text-slate-500">{record.email || '未设置邮箱'}</div>
                </div>
            ),
        },
        {
            title: '身份',
            dataIndex: 'is_admin',
            key: 'is_admin',
            width: 120,
            render: (isAdmin: boolean) => (
                <Tag color={isAdmin ? 'blue' : 'default'}>
                    {isAdmin ? '管理员' : '普通用户'}
                </Tag>
            ),
        },
        {
            title: '状态',
            dataIndex: 'is_active',
            key: 'is_active',
            width: 100,
            render: (isActive: boolean) => (
                <Space size="small">
                    <span className={`w-2 h-2 rounded-full ${isActive ? 'bg-emerald-500' : 'bg-rose-500'}`} />
                    <span className="text-xs">{isActive ? '正常' : '禁用'}</span>
                </Space>
            ),
        },
        {
            title: '操作',
            key: 'action',
            width: 210,
            align: 'right' as const,
            render: (_: unknown, record: AdminUser) => {
                const isSelf = record.user_id === currentUserId;
                return (
                    <Space size={4} wrap>
                        <Tooltip title="编辑用户">
                            <Button
                                type="text"
                                size="small"
                                icon={<EditOutlined />}
                                aria-label={`编辑用户 ${record.username}`}
                                onClick={() => openEditModal(record)}
                            >
                                编辑
                            </Button>
                        </Tooltip>
                        <Tooltip title="重置密码">
                            <Button
                                type="text"
                                size="small"
                                icon={<KeyOutlined />}
                                aria-label={`重置用户 ${record.username} 的密码`}
                                onClick={() => openPasswordModal(record)}
                            >
                                改密
                            </Button>
                        </Tooltip>
                        <Popconfirm
                            title={record.is_active ? '确定禁用该用户吗？' : '确定启用该用户吗？'}
                            description={record.is_active ? '禁用后，该用户的现有登录会话将失效。' : undefined}
                            onConfirm={() => handleToggleStatus(record)}
                            okText="确定"
                            cancelText="取消"
                            disabled={isSelf}
                        >
                            <Tooltip title={isSelf ? '不能禁用当前账号' : undefined}>
                                <Button
                                    size="small"
                                    type="link"
                                    danger={record.is_active}
                                    disabled={isSelf}
                                >
                                    {record.is_active ? '禁用' : '启用'}
                                </Button>
                            </Tooltip>
                        </Popconfirm>
                    </Space>
                );
            },
        },
    ];

    return (
        <div className="space-y-4">
            <div className="flex flex-col gap-3 mb-6 sm:flex-row sm:justify-between sm:items-center">
                <div>
                    <h3 className="text-lg font-bold text-slate-800 m-0">用户管理</h3>
                    <Text type="secondary">账号由管理员统一创建和维护</Text>
                </div>
                <div className="flex w-full flex-col items-stretch gap-2 sm:w-auto sm:flex-row sm:items-center">
                    <Input.Search
                        placeholder="搜索用户名、邮箱或ID"
                        value={searchText}
                        onChange={(event) => setSearchText(event.target.value)}
                        onSearch={handleSearch}
                        enterButton={<SearchOutlined />}
                        allowClear
                        className="w-full sm:!w-[300px]"
                    />
                    <Button
                        type="primary"
                        icon={<PlusOutlined />}
                        onClick={openCreateModal}
                        className="self-start sm:self-auto"
                    >
                        添加用户
                    </Button>
                </div>
            </div>

            <Table
                columns={columns}
                dataSource={users}
                rowKey="user_id"
                loading={loading}
                pagination={{ current: page, pageSize: 12, total, showSizeChanger: false }}
                onChange={handleTableChange}
                scroll={{ x: 860 }}
                locale={{ emptyText: '暂无用户' }}
                className="border border-slate-100 rounded-lg overflow-hidden shadow-sm"
            />

            <Modal
                title={editingUser ? `编辑用户：${editingUser.username}` : '添加用户'}
                open={userModalOpen}
                onOk={() => void submitUser()}
                onCancel={closeUserModal}
                confirmLoading={submitting}
                okText={editingUser ? '保存修改' : '创建用户'}
                cancelText="取消"
                centered
                destroyOnHidden
                styles={{
                    body: {
                        maxHeight: 'calc(var(--app-h) - 180px)',
                        overflowY: 'auto',
                        paddingRight: 4,
                    },
                }}
            >
                <Form<UserFormValues>
                    form={userForm}
                    layout="vertical"
                    requiredMark="optional"
                    style={{ marginTop: 20 }}
                >
                    <Form.Item
                        name="username"
                        label="用户名"
                        rules={[
                            { required: true, message: '请输入用户名' },
                            { min: 3, max: 128, message: '用户名长度为 3 到 128 位' },
                            { pattern: /^[A-Za-z0-9]+$/, message: '用户名只能包含字母和数字' },
                        ]}
                    >
                        <Input autoComplete="off" placeholder="请输入用户名" />
                    </Form.Item>
                    <Form.Item
                        name="email"
                        label="邮箱"
                        rules={[
                            { required: true, message: '请输入邮箱' },
                            { type: 'email', message: '请输入有效的邮箱地址' },
                        ]}
                    >
                        <Input type="email" autoComplete="off" placeholder="name@example.com" />
                    </Form.Item>
                    {!editingUser && (
                        <>
                            <Form.Item
                                name="password"
                                label="初始密码"
                                extra={PASSWORD_HELP}
                                rules={[
                                    { required: true, message: '请输入初始密码' },
                                    { min: 8, max: 128, message: '密码长度为 8 到 128 位' },
                                    { pattern: /[A-Z]/, message: '密码需包含大写字母' },
                                    { pattern: /[a-z]/, message: '密码需包含小写字母' },
                                    { pattern: /\d/, message: '密码需包含数字' },
                                ]}
                            >
                                <Input.Password autoComplete="new-password" placeholder="请输入初始密码" />
                            </Form.Item>
                            <Form.Item
                                name="confirm_password"
                                label="确认密码"
                                dependencies={['password']}
                                rules={[
                                    { required: true, message: '请再次输入密码' },
                                    ({ getFieldValue }) => ({
                                        validator(_, value) {
                                            if (!value || getFieldValue('password') === value) return Promise.resolve();
                                            return Promise.reject(new Error('两次输入的密码不一致'));
                                        },
                                    }),
                                ]}
                            >
                                <Input.Password autoComplete="new-password" placeholder="请再次输入密码" />
                            </Form.Item>
                        </>
                    )}
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                        <Form.Item name="is_active" label="账号状态" valuePropName="checked">
                            <Switch
                                checkedChildren="启用"
                                unCheckedChildren="禁用"
                                disabled={editingUser?.user_id === currentUserId}
                            />
                        </Form.Item>
                        <Form.Item name="is_admin" label="用户身份" valuePropName="checked">
                            <Switch
                                checkedChildren="管理员"
                                unCheckedChildren="普通用户"
                                disabled={editingUser?.user_id === currentUserId}
                            />
                        </Form.Item>
                    </div>
                </Form>
            </Modal>

            <Modal
                title={`修改密码：${passwordUser?.username || ''}`}
                open={Boolean(passwordUser)}
                onOk={() => void submitPassword()}
                onCancel={closePasswordModal}
                confirmLoading={submitting}
                okText="确认修改"
                cancelText="取消"
                centered
                destroyOnHidden
                styles={{
                    body: {
                        maxHeight: 'calc(var(--app-h) - 180px)',
                        overflowY: 'auto',
                        paddingRight: 4,
                    },
                }}
            >
                <Text type="secondary">
                    修改后该用户的现有登录会话将失效，需要使用新密码重新登录。
                </Text>
                <Form<PasswordFormValues>
                    form={passwordForm}
                    layout="vertical"
                    requiredMark="optional"
                    style={{ marginTop: 20 }}
                >
                    <Form.Item
                        name="new_password"
                        label="新密码"
                        extra={PASSWORD_HELP}
                        rules={[
                            { required: true, message: '请输入新密码' },
                            { min: 8, max: 128, message: '密码长度为 8 到 128 位' },
                            { pattern: /[A-Z]/, message: '密码需包含大写字母' },
                            { pattern: /[a-z]/, message: '密码需包含小写字母' },
                            { pattern: /\d/, message: '密码需包含数字' },
                        ]}
                    >
                        <Input.Password autoComplete="new-password" placeholder="请输入新密码" />
                    </Form.Item>
                    <Form.Item
                        name="confirm_password"
                        label="确认新密码"
                        dependencies={['new_password']}
                        rules={[
                            { required: true, message: '请再次输入新密码' },
                            ({ getFieldValue }) => ({
                                validator(_, value) {
                                    if (!value || getFieldValue('new_password') === value) return Promise.resolve();
                                    return Promise.reject(new Error('两次输入的密码不一致'));
                                },
                            }),
                        ]}
                    >
                        <Input.Password autoComplete="new-password" placeholder="请再次输入新密码" />
                    </Form.Item>
                </Form>
            </Modal>
        </div>
    );
};
