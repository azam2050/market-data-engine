"""HTTP Basic auth for the dashboard.

One operator, one password, set only through Railway Variables — never in
code. Comparison is timing-safe so a slow string compare cannot leak how
much of the password an attacker has guessed.
"""

from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from qqq_alpha.config import Settings

_security = HTTPBasic()


def require_login(settings: Settings):
    """Build a FastAPI dependency bound to this run's configured credentials."""

    def _check(credentials: HTTPBasicCredentials = Depends(_security)) -> str:
        valid_user = secrets.compare_digest(credentials.username, settings.admin_username)
        valid_pass = secrets.compare_digest(credentials.password, settings.admin_password)
        if not (valid_user and valid_pass):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="بيانات الدخول غير صحيحة",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    return _check
