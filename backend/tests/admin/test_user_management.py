import importlib.util
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

USERS_ROUTER_PATH = Path(__file__).parents[2] / "services/api/routers/admin/users.py"
USERS_ROUTER_SPEC = importlib.util.spec_from_file_location(
    "admin_users_under_test",
    USERS_ROUTER_PATH,
)
assert USERS_ROUTER_SPEC and USERS_ROUTER_SPEC.loader
users_router = importlib.util.module_from_spec(USERS_ROUTER_SPEC)
USERS_ROUTER_SPEC.loader.exec_module(users_router)


def make_user(**overrides):
    values = {
        "user_id": "00000002",
        "tenant_id": "default",
        "username": "testuser",
        "email": "test@example.com",
        "is_active": True,
        "is_verified": True,
        "is_admin": False,
        "created_at": datetime(2026, 1, 1),
        "last_login_at": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class StubAuthService:
    def __init__(self):
        self.validated_password = None

    def validate_password(self, password: str) -> None:
        self.validated_password = password
        if password == "weakpass":
            raise ValueError("密码需包含至少一个大写字母")

    def get_password_hash(self, password: str) -> str:
        return f"hashed:{password}"


@pytest.mark.asyncio
async def test_create_user_uses_admin_tenant_and_hashes_password():
    service = SimpleNamespace(create_user=AsyncMock(return_value=make_user()))
    auth_service = StubAuthService()
    payload = users_router.AdminUserCreate(
        username="testuser",
        email="TEST@example.com",
        password="Secure123",
        is_admin=False,
        is_active=True,
    )

    response = await users_router.create_user(
        payload,
        current_user={"user_id": "admin", "tenant_id": "default"},
        user_service=service,
        auth_service=auth_service,
    )

    assert response["code"] == 201
    assert auth_service.validated_password == "Secure123"
    service.create_user.assert_awaited_once_with(
        tenant_id="default",
        username="testuser",
        email="test@example.com",
        password_hash="hashed:Secure123",
        is_admin=False,
        is_active=True,
    )


@pytest.mark.asyncio
async def test_create_user_returns_safe_password_validation_error():
    service = SimpleNamespace(create_user=AsyncMock())
    auth_service = StubAuthService()
    payload = users_router.AdminUserCreate(
        username="testuser",
        email="test@example.com",
        password="weakpass",
    )

    with pytest.raises(HTTPException) as exc_info:
        await users_router.create_user(
            payload,
            current_user={"user_id": "admin", "tenant_id": "default"},
            user_service=service,
            auth_service=auth_service,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "密码需包含至少一个大写字母"
    service.create_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_user_prevents_self_demotion():
    service = SimpleNamespace(get_user_by_id=AsyncMock(return_value=make_user(user_id="admin", is_admin=True)))
    payload = users_router.AdminUserUpdate(
        username="admin",
        email="admin@example.com",
        is_admin=False,
        is_active=True,
    )

    with pytest.raises(HTTPException) as exc_info:
        await users_router.update_user(
            "admin",
            payload,
            current_user={"user_id": "admin", "tenant_id": "default"},
            user_service=service,
        )

    assert exc_info.value.status_code == 400
    assert "不能取消当前账号" in exc_info.value.detail


@pytest.mark.asyncio
async def test_update_user_keeps_last_active_admin():
    service = SimpleNamespace(
        get_user_by_id=AsyncMock(return_value=make_user(is_admin=True)),
        count_active_admins=AsyncMock(return_value=1),
    )
    payload = users_router.AdminUserUpdate(
        username="otheradmin",
        email="other@example.com",
        is_admin=True,
        is_active=False,
    )

    with pytest.raises(HTTPException) as exc_info:
        await users_router.update_user(
            "00000002",
            payload,
            current_user={"user_id": "admin", "tenant_id": "default"},
            user_service=service,
        )

    assert exc_info.value.status_code == 400
    assert "至少需要保留一个" in exc_info.value.detail


@pytest.mark.asyncio
async def test_reset_password_hashes_value_for_target_user():
    service = SimpleNamespace(reset_password_by_admin=AsyncMock(return_value=make_user()))
    auth_service = StubAuthService()

    response = await users_router.reset_user_password(
        "00000002",
        users_router.AdminPasswordReset(new_password="NewPass123"),
        current_user={"user_id": "admin", "tenant_id": "default"},
        user_service=service,
        auth_service=auth_service,
    )

    assert response["code"] == 200
    service.reset_password_by_admin.assert_awaited_once_with(
        "00000002",
        "default",
        "hashed:NewPass123",
    )


@pytest.mark.asyncio
async def test_toggle_status_prevents_disabling_current_user():
    service = SimpleNamespace(
        get_user_by_id=AsyncMock(return_value=make_user(user_id="admin", is_admin=True)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await users_router.toggle_user_status(
            "admin",
            current_user={"user_id": "admin", "tenant_id": "default"},
            user_service=service,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "不能禁用当前账号"


@pytest.mark.asyncio
async def test_identity_check_allows_legacy_user_without_email():
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(scalar_one_or_none=lambda: None),
        )
    )
    service = users_router.UserService.__new__(users_router.UserService)

    await service._ensure_identity_available(
        session,
        tenant_id="default",
        username="legacyuser",
        email=None,
        exclude_user_id="00000003",
    )

    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_refreshed_admin_token_keeps_admin_claim(monkeypatch):
    from jose import jwt

    from backend.services.api.user_app.config import settings
    from backend.services.api.user_app.services import auth_service as auth_module

    service = auth_module.AuthService.__new__(auth_module.AuthService)
    service.redis_client = SimpleNamespace(setex=lambda *_args: None)
    refresh_token = service._create_refresh_token("admin", "default")
    user = make_user(
        user_id="admin",
        username="admin",
        email="admin@example.com",
        is_admin=True,
    )
    user_session = SimpleNamespace(
        is_revoked=False,
        is_active=True,
        token_jti="old-jti",
        refresh_token=refresh_token,
        expires_at=None,
        last_active_at=None,
        ip_address=None,
        user_agent=None,
    )
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(first=lambda: (user, user_session)),
        ),
        add=lambda _value: None,
        commit=AsyncMock(),
    )

    @asynccontextmanager
    async def fake_get_session(*, read_only=False):
        del read_only
        yield session

    class StubRBACService:
        def __init__(self, _session):
            pass

        async def get_user_roles(self, _user_id):
            return [SimpleNamespace(code="admin")]

    monkeypatch.setattr(auth_module, "get_session", fake_get_session)
    monkeypatch.setattr(auth_module, "RBACService", StubRBACService)

    response = await service.refresh_tokens(refresh_token)
    payload = jwt.decode(
        response.access_token,
        settings.SECRET_KEY,
        algorithms=[settings.ALGORITHM],
    )

    assert payload["roles"] == ["admin"]
    assert payload["is_admin"] is True
