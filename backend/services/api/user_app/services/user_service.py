"""
User Service - 用户业务逻辑
"""

import logging
import uuid
from datetime import datetime
from typing import List, Optional

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError

from backend.services.api.user_app.config import settings
from backend.services.api.user_app.models.rbac import Role, user_roles
from backend.services.api.user_app.models.user import User, UserProfile, UserSession
from backend.services.api.user_app.schemas.user import (
    UserDetailResponse,
    UserProfileResponse,
    UserResponse,
)
from backend.shared.database_manager_v2 import get_session
from backend.shared.redis_sentinel_client import get_redis_sentinel_client

logger = logging.getLogger(__name__)


class UserService:
    """用户服务"""

    def __init__(self):
        self.redis_client = get_redis_sentinel_client()

    async def get_user_by_id(self, user_id: str, tenant_id: str, use_cache: bool = True) -> User | None:
        """根据ID获取用户"""
        # 尝试从缓存获取
        if use_cache:
            cache_key = f"user:{tenant_id}:{user_id}"
            cached = self.redis_client.get(cache_key, use_slave=True)
            if cached:
                logger.info(f"User cache hit: {user_id}")
                # 这里简化处理，实际应反序列化为User对象

        # 从数据库查询（使用从库）
        async with get_session(read_only=True) as session:
            result = await session.execute(
                select(User).where(
                    User.user_id == user_id,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            user = result.scalar_one_or_none()

            if user and use_cache:
                # 缓存用户信息
                cache_key = f"user:{tenant_id}:{user_id}"
                # 简化：实际应序列化整个对象
                self.redis_client.setex(cache_key, settings.CACHE_TTL_USER_PROFILE, user.username.encode())

            return user

    async def get_user_by_username(self, username: str, tenant_id: str) -> User | None:
        """根据用户名获取用户"""
        async with get_session(read_only=True) as session:
            result = await session.execute(
                select(User).where(
                    User.username == username,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            return result.scalar_one_or_none()

    async def get_user_by_email(self, email: str, tenant_id: str) -> User | None:
        """根据邮箱获取用户"""
        async with get_session(read_only=True) as session:
            result = await session.execute(
                select(User).where(
                    User.email == email,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            return result.scalar_one_or_none()

    async def get_user_by_phone(self, phone: str, tenant_id: str) -> User | None:
        """根据手机号获取用户"""
        async with get_session(read_only=True) as session:
            result = await session.execute(
                select(User).where(
                    User.phone_number == phone,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            return result.scalar_one_or_none()

    async def search_users(
        self,
        tenant_id: str,
        query: str | None = None,
        is_active: bool | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[User], int]:
        """搜索用户"""
        async with get_session(read_only=True) as session:
            # 构建查询
            stmt = select(User).where(
                User.is_deleted == False,
                User.tenant_id == tenant_id,
            )

            if query:
                stmt = stmt.where(
                    or_(
                        User.user_id.ilike(f"%{query}%"),
                        User.username.ilike(f"%{query}%"),
                        User.email.ilike(f"%{query}%"),
                    )
                )

            if is_active is not None:
                stmt = stmt.where(User.is_active == is_active)

            # 计算总数
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total = await session.scalar(count_stmt)

            # 分页
            stmt = stmt.offset((page - 1) * page_size).limit(page_size)
            result = await session.execute(stmt)
            users = result.scalars().all()

            return list(users), total or 0

    async def create_user(
        self,
        tenant_id: str,
        username: str,
        email: str,
        password_hash: str,
        *,
        is_admin: bool = False,
        is_active: bool = True,
    ) -> User:
        """由管理员创建用户及其基础档案。"""
        async with get_session(read_only=False) as session:
            await self._ensure_identity_available(session, tenant_id, username, email)
            user_id = await self._generate_user_id(session)
            user = User(
                user_id=user_id,
                tenant_id=tenant_id,
                username=username,
                email=email,
                password_hash=password_hash,
                is_admin=is_admin,
                is_active=is_active,
                is_verified=True,
                is_deleted=False,
            )
            session.add(user)
            session.add(
                UserProfile(
                    user_id=user_id,
                    tenant_id=tenant_id,
                    display_name=username,
                )
            )
            try:
                await session.flush()
                await self._sync_base_role(session, user_id, is_admin)
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                raise ValueError("用户名或邮箱已存在") from exc
            await session.refresh(user)

        self._clear_user_cache(tenant_id, user_id)
        logger.info("User created by administrator: %s", user_id)
        return user

    async def update_user_by_admin(
        self,
        user_id: str,
        tenant_id: str,
        **updates,
    ) -> User | None:
        """更新管理员允许维护的用户字段。"""
        allowed_fields = {"username", "email", "is_active", "is_admin"}
        invalid_fields = set(updates) - allowed_fields
        if invalid_fields:
            raise ValueError("包含不允许修改的用户字段")

        revoke_sessions = False
        async with get_session(read_only=False) as session:
            result = await session.execute(
                select(User).where(
                    User.user_id == user_id,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            user = result.scalar_one_or_none()
            if not user:
                return None

            username = updates.get("username", user.username)
            email = updates.get("email", user.email)
            await self._ensure_identity_available(
                session,
                tenant_id,
                username,
                email,
                exclude_user_id=user_id,
            )

            old_is_active = bool(user.is_active)
            old_is_admin = bool(user.is_admin)
            for key, value in updates.items():
                setattr(user, key, value)
            user.updated_at = datetime.now()

            if "is_admin" in updates:
                await self._sync_base_role(session, user_id, bool(user.is_admin))
            revoke_sessions = (
                old_is_active != bool(user.is_active)
                or old_is_admin != bool(user.is_admin)
            )
            session_jtis = (
                await self._revoke_sessions(session, user_id, tenant_id)
                if revoke_sessions
                else []
            )
            try:
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                raise ValueError("用户名或邮箱已存在") from exc
            await session.refresh(user)

        self._clear_user_cache(tenant_id, user_id, session_jtis)
        logger.info("User updated by administrator: %s", user_id)
        return user

    async def reset_password_by_admin(
        self,
        user_id: str,
        tenant_id: str,
        password_hash: str,
    ) -> User | None:
        """重置指定用户密码，并撤销该用户的全部既有会话。"""
        async with get_session(read_only=False) as session:
            result = await session.execute(
                select(User).where(
                    User.user_id == user_id,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            user = result.scalar_one_or_none()
            if not user:
                return None

            user.password_hash = password_hash
            user.updated_at = datetime.now()
            session_jtis = await self._revoke_sessions(session, user_id, tenant_id)
            await session.commit()
            await session.refresh(user)

        self._clear_user_cache(tenant_id, user_id, session_jtis)
        logger.info("User password reset by administrator: %s", user_id)
        return user

    async def count_active_admins(self, tenant_id: str) -> int:
        """从主库读取当前租户的有效管理员数量。"""
        async with get_session(read_only=False) as session:
            stmt = select(func.count()).select_from(User).where(
                User.tenant_id == tenant_id,
                User.is_admin == True,
                User.is_active == True,
                User.is_deleted == False,
            )
            return int(await session.scalar(stmt) or 0)

    async def _ensure_identity_available(
        self,
        session,
        tenant_id: str,
        username: str,
        email: str | None,
        exclude_user_id: str | None = None,
    ) -> None:
        username_stmt = select(User.user_id).where(
            User.tenant_id == tenant_id,
            User.username == username,
            User.is_deleted == False,
        )
        if exclude_user_id:
            username_stmt = username_stmt.where(User.user_id != exclude_user_id)

        if (await session.execute(username_stmt)).scalar_one_or_none():
            raise ValueError("用户名已存在")
        # 历史账号允许没有邮箱；状态切换不应因其他空邮箱账号而失败。
        if email:
            email_stmt = select(User.user_id).where(
                User.tenant_id == tenant_id,
                User.email == email,
                User.is_deleted == False,
            )
            if exclude_user_id:
                email_stmt = email_stmt.where(User.user_id != exclude_user_id)
            if (await session.execute(email_stmt)).scalar_one_or_none():
                raise ValueError("邮箱已被使用")

    async def _generate_user_id(self, session) -> str:
        for _ in range(50):
            candidate = f"{uuid.uuid4().int % 10**8:08d}"
            result = await session.execute(
                select(User.user_id).where(User.user_id == candidate)
            )
            if result.scalar_one_or_none() is None:
                return candidate
        raise ValueError("无法生成唯一的用户ID，请重试")

    async def _sync_base_role(self, session, user_id: str, is_admin: bool) -> None:
        role_codes = ["admin", "user"]
        result = await session.execute(select(Role).where(Role.code.in_(role_codes)))
        roles = {role.code: role for role in result.scalars().all()}

        admin_role = roles.get("admin")
        user_role = roles.get("user")
        assigned_result = await session.execute(
            select(user_roles.c.role_id).where(user_roles.c.user_id == user_id)
        )
        assigned_role_ids = set(assigned_result.scalars().all())

        if admin_role and is_admin and admin_role.id not in assigned_role_ids:
            await session.execute(
                user_roles.insert().values(user_id=user_id, role_id=admin_role.id)
            )
        if admin_role and not is_admin and admin_role.id in assigned_role_ids:
            await session.execute(
                user_roles.delete().where(
                    user_roles.c.user_id == user_id,
                    user_roles.c.role_id == admin_role.id,
                )
            )
        if user_role and not is_admin and user_role.id not in assigned_role_ids:
            await session.execute(
                user_roles.insert().values(user_id=user_id, role_id=user_role.id)
            )

    async def _revoke_sessions(
        self,
        session,
        user_id: str,
        tenant_id: str,
    ) -> list[str]:
        result = await session.execute(
            select(UserSession).where(
                UserSession.user_id == user_id,
                UserSession.tenant_id == tenant_id,
                UserSession.is_revoked == False,
            )
        )
        sessions = list(result.scalars().all())
        for user_session in sessions:
            user_session.is_revoked = True
            user_session.is_active = False
        return [item.token_jti for item in sessions if item.token_jti]

    def _clear_user_cache(
        self,
        tenant_id: str,
        user_id: str,
        session_jtis: list[str] | None = None,
    ) -> None:
        keys = [
            f"user:{tenant_id}:{user_id}",
            f"user:roles:{user_id}",
            f"user:permissions:{user_id}",
        ]
        keys.extend(f"session:{tenant_id}:{jti}" for jti in session_jtis or [])
        for key in keys:
            try:
                self.redis_client.delete(key)
            except Exception as exc:
                logger.warning("Failed to clear user cache %s: %s", key, exc)

    async def update_user(self, user_id: str, tenant_id: str, **updates) -> User | None:
        """更新用户信息"""
        async with get_session(read_only=False) as session:
            result = await session.execute(
                select(User).where(
                    User.user_id == user_id,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            user = result.scalar_one_or_none()

            if not user:
                return None

            # 更新字段
            for key, value in updates.items():
                if hasattr(user, key) and value is not None:
                    setattr(user, key, value)

            user.updated_at = datetime.now()
            await session.commit()
            await session.refresh(user)

            # 清除缓存
            cache_key = f"user:{tenant_id}:{user_id}"
            self.redis_client.delete(cache_key)

            logger.info(f"User updated: {user_id}")
            return user

    async def deactivate_user(self, user_id: str, tenant_id: str) -> bool:
        """停用用户"""
        async with get_session(read_only=False) as session:
            result = await session.execute(
                update(User)
                .where(User.user_id == user_id, User.tenant_id == tenant_id)
                .values(is_active=False, updated_at=datetime.now())
            )
            await session.commit()

            # 清除缓存
            cache_key = f"user:{tenant_id}:{user_id}"
            self.redis_client.delete(cache_key)

            return result.rowcount > 0

    async def soft_delete_user(self, user_id: str, tenant_id: str) -> bool:
        """软删除用户"""
        async with get_session(read_only=False) as session:
            result = await session.execute(
                update(User)
                .where(User.user_id == user_id, User.tenant_id == tenant_id)
                .values(
                    is_deleted=True,
                    deleted_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            await session.commit()

            # 清除缓存
            cache_key = f"user:{tenant_id}:{user_id}"
            self.redis_client.delete(cache_key)

            logger.info(f"User soft deleted: {user_id}")
            return result.rowcount > 0

    async def get_user_detail(self, user_id: str, tenant_id: str) -> UserDetailResponse | None:
        """获取用户详细信息（包含档案）"""
        async with get_session(read_only=True) as session:
            # 获取用户
            user_result = await session.execute(
                select(User).where(
                    User.user_id == user_id,
                    User.tenant_id == tenant_id,
                    User.is_deleted == False,
                )
            )
            user = user_result.scalar_one_or_none()

            if not user:
                return None

            # 获取档案
            profile_result = await session.execute(
                select(UserProfile).where(
                    UserProfile.user_id == user_id,
                    UserProfile.tenant_id == tenant_id,
                )
            )
            profile = profile_result.scalar_one_or_none()

            if not profile:
                return None

            return UserDetailResponse(
                user=UserResponse.from_orm(user),
                profile=UserProfileResponse.from_orm(profile),
            )
