"""
邮箱服务适配器模块
复用 chatgpt_register_v2 的邮箱客户端实现
"""

import os
import sys
from typing import Any, Dict, Optional

from .constants import EmailServiceType
from .proxy_utils import normalize_proxy_url


_v2_lib_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "chatgpt_register_v2", "lib")
if os.path.exists(_v2_lib_path) and _v2_lib_path not in sys.path:
    sys.path.insert(0, _v2_lib_path)


class EmailServiceError(Exception):
    """邮箱服务异常"""
    pass


class EmailServiceStatus:
    """邮箱服务状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class BaseEmailService:
    """邮箱服务抽象基类"""

    def __init__(self, service_type: EmailServiceType, name: str = None):
        self.service_type = service_type
        self.name = name or f"{service_type.value}_service"
        self._status = EmailServiceStatus.HEALTHY
        self._last_error = None

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        raise NotImplementedError

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = r"(?<!\d)(\d{6})(?!\d)",
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        raise NotImplementedError

    def list_emails(self, **kwargs) -> list:
        raise NotImplementedError

    def delete_email(self, email_id: str) -> bool:
        raise NotImplementedError

    def check_health(self) -> bool:
        raise NotImplementedError


def create_email_service(
    config: Dict[str, Any],
    proxy_url: Optional[str] = None
) -> BaseEmailService:
    """创建邮箱服务实例"""
    normalized_proxy = normalize_proxy_url(proxy_url)

    try:
        from skymail_client import init_skymail_client
    except ImportError:
        from chatgpt_register_v2.lib.skymail_client import init_skymail_client

    merged_config = dict(config)
    if normalized_proxy:
        merged_config["proxy"] = normalized_proxy
    service = init_skymail_client(merged_config)
    provider = str(merged_config.get("mail_provider", "skymail")).lower().replace("-", "_")
    try:
        service.service_type = EmailServiceType(provider)
    except ValueError:
        service.service_type = EmailServiceType.SKYMAIL
    return service
