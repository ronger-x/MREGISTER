from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

from chatgpt_register_v3.lib.email_service import create_email_service

from .context import RunContext


@dataclass
class GeneratedEmail:
    address: str
    email_id: str | None = None
    raw: dict[str, Any] | None = None


class EmailClient:
    def __init__(self, context: RunContext):
        self.context = context
        self.service = None

    def is_configured(self) -> bool:
        return self.context.mail_provider == "gptmail" and bool(self.context.gptmail_api_key)

    def create_service(self):
        if self.service is not None:
            return self.service
        if not self.is_configured():
            raise RuntimeError("GPTMail credential is not configured for this task")
        config = {
            "mail_provider": "gptmail",
            "gptmail_api_key": self.context.gptmail_api_key or "",
            "gptmail_base_url": self.context.gptmail_base_url or "https://mail.chatgpt.org.uk",
            "gptmail_prefix": self.context.gptmail_prefix or "",
            "gptmail_domain": self.context.gptmail_domain or "",
        }
        self.service = create_email_service(config, proxy_url=self.context.proxy)
        return self.service

    def generate_email(self) -> GeneratedEmail:
        service = self.create_service()
        if hasattr(service, "create_email"):
            payload = service.create_email({})
            address = str(payload.get("email") or payload.get("mail") or payload.get("address") or "").strip()
            email_id = payload.get("id") or payload.get("email_id")
        elif hasattr(service, "create_temp_email"):
            address, email_id = service.create_temp_email()
            payload = {"email": address, "email_id": email_id}
        else:
            raise RuntimeError(f"Email service does not support email creation: {type(service).__name__}")
        if not address:
            raise RuntimeError(f"Email provider returned an invalid payload: {payload}")
        return GeneratedEmail(address=address, email_id=str(email_id) if email_id else None, raw=payload)

    def get_verification_code(self, email: GeneratedEmail, timeout: int = 180, pattern: str = r"(?<!\d)(\d{6})(?!\d)") -> str | None:
        service = self.create_service()
        started = time.time()
        while time.time() - started <= timeout:
            if hasattr(service, "get_verification_code"):
                code = service.get_verification_code(
                    email.address,
                    email_id=email.email_id,
                    timeout=30,
                    pattern=pattern,
                    otp_sent_at=started,
                )
            elif hasattr(service, "wait_for_verification_code"):
                code = service.wait_for_verification_code(email.address, timeout=30)
            else:
                raise RuntimeError(f"Email service does not support verification polling: {type(service).__name__}")
            if code:
                match = re.search(pattern, str(code))
                return match.group(1) if match else str(code)
            time.sleep(5)
        return None


def mask_email(value: str | None) -> str | None:
    if not value or "@" not in value:
        return value
    local, domain = value.split("@", 1)
    if len(local) <= 2:
        masked = local[:1] + "*"
    else:
        masked = local[:2] + "***"
    return f"{masked}@{domain}"
