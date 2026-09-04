from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from backend.services.api.routers import auth as auth_router
from backend.services.api.user_app.schemas.user import UserRegister


@pytest.mark.asyncio
async def test_public_registration_is_disabled_by_default(monkeypatch):
    monkeypatch.setattr(auth_router.settings, "enable_public_registration", False)
    service = SimpleAuthService()

    with pytest.raises(HTTPException) as exc_info:
        await auth_router.register(
            UserRegister(
                tenant_id="default",
                username="testuser",
                email="test@example.com",
                password="Secure123",
            ),
            auth_service=service,
        )

    assert exc_info.value.status_code == 403
    assert "联系管理员" in exc_info.value.detail
    service.register.assert_not_awaited()


class SimpleAuthService:
    def __init__(self):
        self.register = AsyncMock()
