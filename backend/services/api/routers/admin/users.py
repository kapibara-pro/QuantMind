import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, EmailStr, Field, field_validator

from backend.services.api.user_app.middleware.auth import require_admin
from backend.services.api.user_app.schemas.user import UserResponse
from backend.services.api.user_app.services.auth_service import AuthService
from backend.services.api.user_app.services.user_service import UserService

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_admin)])


class AdminUserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=128)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    is_admin: bool = False
    is_active: bool = True

    @field_validator("username")
    @classmethod
    def validate_username(cls, value: str) -> str:
        value = value.strip()
        if not value.isalnum():
            raise ValueError("用户名只能包含字母和数字")
        return value


class AdminUserUpdate(BaseModel):
    username: str = Field(..., min_length=3, max_length=128)
    email: EmailStr
    is_admin: bool
    is_active: bool

    @field_validator("username")
    @classmethod
    def validate_username(cls, value: str) -> str:
        value = value.strip()
        if not value.isalnum():
            raise ValueError("用户名只能包含字母和数字")
        return value


class AdminPasswordReset(BaseModel):
    new_password: str = Field(..., min_length=8, max_length=128)


def get_user_service() -> UserService:
    return UserService()


def get_auth_service() -> AuthService:
    return AuthService()


def _serialize_user(user) -> dict:
    return UserResponse.model_validate(user).model_dump()


@router.get("/")
async def list_users(
    query: str | None = Query(None, description="搜索关键词"),
    is_active: bool | None = Query(None, description="是否激活"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: dict = Depends(require_admin),
    user_service: UserService = Depends(get_user_service),
):
    """管理员获取当前租户的用户列表。"""
    tenant_id = current_user.get("tenant_id", "default")
    users, total = await user_service.search_users(
        tenant_id=tenant_id,
        query=query,
        is_active=is_active,
        page=page,
        page_size=page_size,
    )

    return {
        "success": True,
        "code": 200,
        "message": "success",
        "data": [_serialize_user(user) for user in users],
        "meta": {"total": total, "page": page, "page_size": page_size},
    }


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: AdminUserCreate,
    current_user: dict = Depends(require_admin),
    user_service: UserService = Depends(get_user_service),
    auth_service: AuthService = Depends(get_auth_service),
):
    """管理员创建用户；租户信息只取自管理员令牌。"""
    tenant_id = current_user.get("tenant_id", "default")
    try:
        auth_service.validate_password(payload.password)
        user = await user_service.create_user(
            tenant_id=tenant_id,
            username=payload.username,
            email=str(payload.email).lower(),
            password_hash=auth_service.get_password_hash(payload.password),
            is_admin=payload.is_admin,
            is_active=payload.is_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    logger.info(
        "Administrator created user: actor=%s target=%s tenant=%s",
        current_user.get("user_id"),
        user.user_id,
        tenant_id,
    )
    return {"code": 201, "message": "用户已创建", "data": _serialize_user(user)}


@router.put("/{user_id}")
async def update_user(
    user_id: str,
    payload: AdminUserUpdate,
    current_user: dict = Depends(require_admin),
    user_service: UserService = Depends(get_user_service),
):
    """管理员编辑用户的登录标识、身份和状态。"""
    tenant_id = current_user.get("tenant_id", "default")
    existing = await user_service.get_user_by_id(user_id, tenant_id, use_cache=False)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")

    is_self = user_id == current_user.get("user_id")
    if is_self and (not payload.is_admin or not payload.is_active):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="不能取消当前账号的管理员身份或禁用当前账号",
        )

    removes_active_admin = (
        bool(existing.is_admin)
        and bool(existing.is_active)
        and (not payload.is_admin or not payload.is_active)
    )
    if removes_active_admin and await user_service.count_active_admins(tenant_id) <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="至少需要保留一个启用的管理员账号",
        )

    try:
        user = await user_service.update_user_by_admin(
            user_id,
            tenant_id,
            username=payload.username,
            email=str(payload.email).lower(),
            is_admin=payload.is_admin,
            is_active=payload.is_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")
    logger.info(
        "Administrator updated user: actor=%s target=%s tenant=%s",
        current_user.get("user_id"),
        user_id,
        tenant_id,
    )
    return {"code": 200, "message": "用户信息已更新", "data": _serialize_user(user)}


@router.post("/{user_id}/password")
async def reset_user_password(
    user_id: str,
    payload: AdminPasswordReset,
    current_user: dict = Depends(require_admin),
    user_service: UserService = Depends(get_user_service),
    auth_service: AuthService = Depends(get_auth_service),
):
    """管理员重置用户密码，并撤销该用户的已有会话。"""
    tenant_id = current_user.get("tenant_id", "default")
    try:
        auth_service.validate_password(payload.new_password)
        user = await user_service.reset_password_by_admin(
            user_id,
            tenant_id,
            auth_service.get_password_hash(payload.new_password),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")
    logger.info(
        "Administrator reset user password: actor=%s target=%s tenant=%s",
        current_user.get("user_id"),
        user_id,
        tenant_id,
    )
    return {"code": 200, "message": "密码已重置，用户需要重新登录"}


@router.post("/{user_id}/toggle-status")
async def toggle_user_status(
    user_id: str,
    current_user: dict = Depends(require_admin),
    user_service: UserService = Depends(get_user_service),
):
    """切换用户启用/禁用状态。"""
    tenant_id = current_user.get("tenant_id", "default")
    user = await user_service.get_user_by_id(user_id, tenant_id, use_cache=False)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")
    if user_id == current_user.get("user_id"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="不能禁用当前账号")
    if user.is_admin and user.is_active and await user_service.count_active_admins(tenant_id) <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="至少需要保留一个启用的管理员账号",
        )

    try:
        updated_user = await user_service.update_user_by_admin(
            user_id,
            tenant_id,
            username=user.username,
            email=user.email,
            is_admin=bool(user.is_admin),
            is_active=not bool(user.is_active),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not updated_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")

    return {
        "code": 200,
        "message": "状态已更新",
        "data": {"is_active": updated_user.is_active},
    }
