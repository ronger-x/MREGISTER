from __future__ import annotations

import email
import imaplib
import json
import random
import re
import secrets
import string
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests

from .proxy_utils import normalize_proxy_url


def _build_session(proxy: str | None = None) -> requests.Session:
    session = requests.Session()
    normalized_proxy = normalize_proxy_url(proxy)
    if normalized_proxy:
        session.proxies = {"http": normalized_proxy, "https": normalized_proxy}
    return session


def _generate_local_part(prefix: str | None = None, length: int = 8) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    if not prefix:
        return suffix
    normalized = re.sub(r"[^a-z0-9]", "", prefix.lower())
    if not normalized:
        return suffix
    return f"{normalized}{suffix}"


def _coerce_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_extra_config(extra: object) -> dict[str, Any]:
    if isinstance(extra, dict):
        return {str(key): value for key, value in extra.items()}
    text = str(extra or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return {str(key): value for key, value in payload.items()}
    return {}


def _response_excerpt(response: requests.Response, limit: int = 200) -> str:
    try:
        text = response.text or ""
    except Exception:
        text = ""
    text = " ".join(text.split())
    if not text:
        return ""
    return text[:limit]


def _retry_after_seconds(response: requests.Response, default: float = 5.0) -> float:
    try:
        raw_value = str(response.headers.get("Retry-After") or "").strip()
    except Exception:
        raw_value = ""
    if raw_value:
        try:
            return max(0.0, float(raw_value))
        except (TypeError, ValueError):
            pass
    return default


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def _extract_message_text(msg: Message) -> str:
    texts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type not in {"text/plain", "text/html"}:
                continue
            if part.get_content_disposition() == "attachment":
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                raw = part.get_payload()
                if isinstance(raw, str):
                    texts.append(raw)
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                texts.append(payload.decode(charset, errors="ignore"))
            except Exception:
                texts.append(payload.decode("utf-8", errors="ignore"))
    else:
        payload = msg.get_payload(decode=True)
        if payload is None:
            raw = msg.get_payload()
            if isinstance(raw, str):
                texts.append(raw)
        else:
            charset = msg.get_content_charset() or "utf-8"
            try:
                texts.append(payload.decode(charset, errors="ignore"))
            except Exception:
                texts.append(payload.decode("utf-8", errors="ignore"))
    return "\n".join(filter(None, texts))


class BasePollingMailClient:
    """Shared helpers for REST/IMAP mail providers."""

    def __init__(self) -> None:
        self._used_codes: set[str] = set()

    @staticmethod
    def extract_verification_code(content: str | None) -> str | None:
        if not content:
            return None

        patterns = [
            r"Verification code:?\s*(\d{6})",
            r"code is\s*(\d{6})",
            r"代码为[:：]?\s*(\d{6})",
            r"验证码[:：]?\s*(\d{6})",
            r">\s*(\d{6})\s*<",
            r"(?<![#&])\b(\d{6})\b",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for code in matches:
                if code == "177010":
                    continue
                return code
        return None

    def wait_for_verification_code(
        self, email: str, timeout: int = 30, exclude_codes: set[str] | None = None
    ) -> str | None:
        if exclude_codes is None:
            exclude_codes = set()

        all_excluded = exclude_codes | self._used_codes
        seen_message_ids: set[str] = set()

        print(f" ⏳ 等待验证码 (最大 {timeout}s)...")
        start = time.time()
        while time.time() - start < timeout:
            messages = self.fetch_emails(email)
            for item in messages:
                if not isinstance(item, dict):
                    continue

                message_id = str(item.get("emailId") or item.get("id") or "").strip()
                if not message_id or message_id in seen_message_ids:
                    continue
                seen_message_ids.add(message_id)

                candidates = [
                    str(item.get("subject") or ""),
                    str(item.get("content") or ""),
                    str(item.get("text") or ""),
                ]
                for content in candidates:
                    code = self.extract_verification_code(content)
                    if code and code not in all_excluded:
                        print(f" ✅ 验证码: {code}")
                        self._used_codes.add(code)
                        return code

            if time.time() - start < 10:
                time.sleep(0.5)
            else:
                time.sleep(2)

        print(" ⏰ 等待验证码超时")
        return None

    def fetch_emails(self, email: str) -> list[dict[str, str]]:
        return []

    def report_registration_result(self, email: str, success: bool, reason: str = "") -> None:
        return None


class MailTmAdapter(BasePollingMailClient):
    def __init__(
        self,
        *,
        base_url: str = "https://api.mail.tm",
        api_key: str = "",
        proxy: str | None = None,
        prefix: str | None = None,
        domain: str | None = None,
        timeout: float = 30.0,
        provider_name: str = "mailtm",
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.api_base = base_url.rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.prefix = (prefix or "").strip() or None
        self.domain = (domain or "").strip() or None
        self.timeout = timeout
        self.provider_name = str(provider_name or "mailtm").strip().lower().replace("-", "_")
        self.extra_config = extra_config or {}
        self.session = _build_session(proxy)
        self._accounts: dict[str, dict[str, str]] = {}
        self._lock = threading.Lock()
        self._create_lock = threading.Lock()
        self._rate_limited_until = 0.0
        self._domains: list[str] = []
        self._private_domains: set[str] = set()
        self._domain_index = 0
        self._domain_failures: dict[str, int] = {}
        self._domain_cooldowns: dict[str, float] = {}
        self._domain_lock = threading.Lock()
        self._domain_failure_threshold = 2
        self._domain_cooldown_seconds = 1800.0

    def _headers(self, *, token: str | None = None, use_json: bool = False, use_api_key: bool = False) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if use_json:
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif use_api_key and self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _supports_optional_api_key(self) -> bool:
        return self.provider_name == "duckmail"

    def _fetch_domains(self) -> tuple[list[str], set[str]]:
        response = self.session.get(
            f"{self.api_base}/domains",
            headers=self._headers(use_api_key=self._supports_optional_api_key()),
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            items = payload
        else:
            items = payload.get("hydra:member") or payload.get("items") or []
        domains: list[str] = []
        private_domains: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain") or "").strip()
            if not domain:
                continue
            if self.provider_name == "duckmail":
                if not item.get("isVerified", True):
                    continue
                owner_id = item.get("ownerId")
                if owner_id not in (None, "", "null"):
                    private_domains.add(domain)
            else:
                if not item.get("isActive", True) or item.get("isPrivate", False):
                    continue
            domains.append(domain)
        if not domains:
            raise RuntimeError("Mail.tm 未返回可用域名")
        return domains, private_domains

    def _resolve_domain(self) -> str:
        with self._domain_lock:
            if not self._domains:
                fetched_domains, private_domains = self._fetch_domains()
                self._private_domains = set(private_domains)
                ordered_domains: list[str] = []
                if self.domain and self.domain in fetched_domains:
                    ordered_domains.append(self.domain)
                if self.provider_name == "duckmail":
                    ordered_domains.extend(
                        domain for domain in fetched_domains
                        if domain in self._private_domains and domain not in ordered_domains
                    )
                    ordered_domains.extend(
                        domain for domain in fetched_domains
                        if domain not in self._private_domains and domain not in ordered_domains
                    )
                else:
                    ordered_domains.extend(domain for domain in fetched_domains if domain not in ordered_domains)
                self._domains = ordered_domains

            now_ts = time.time()
            available_domains = [
                domain
                for domain in self._domains
                if self._domain_cooldowns.get(domain, 0.0) <= now_ts
            ]
            if not available_domains:
                cooldown_until = min(self._domain_cooldowns.get(domain, now_ts) for domain in self._domains)
                wait_seconds = max(0.0, cooldown_until - now_ts)
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                now_ts = time.time()
                available_domains = [
                    domain
                    for domain in self._domains
                    if self._domain_cooldowns.get(domain, 0.0) <= now_ts
                ] or list(self._domains)

            if not available_domains:
                raise RuntimeError("Mail.tm 未返回可用域名")

            if self.domain and self.domain in available_domains:
                candidate_domains = [self.domain]
            elif self.provider_name == "duckmail":
                private_available = [domain for domain in available_domains if domain in self._private_domains]
                candidate_domains = private_available or available_domains
            else:
                candidate_domains = available_domains

            selected = candidate_domains[self._domain_index % len(candidate_domains)]
            self._domain_index = (self._domain_index + 1) % max(1, len(candidate_domains))
            return selected

    @staticmethod
    def _extract_domain(email: str) -> str:
        _, _, domain = str(email or "").partition("@")
        return domain.strip().lower()

    @staticmethod
    def _is_registration_disallowed(reason: str) -> bool:
        normalized = str(reason or "").strip().lower()
        return "registration_disallowed" in normalized or "cannot create your account with the given information" in normalized

    def report_registration_result(self, email: str, success: bool, reason: str = "") -> None:
        domain = self._extract_domain(email)
        if not domain:
            return
        with self._domain_lock:
            if success:
                self._domain_failures[domain] = 0
                self._domain_cooldowns.pop(domain, None)
                return
            if not self._is_registration_disallowed(reason):
                return
            failures = int(self._domain_failures.get(domain, 0)) + 1
            self._domain_failures[domain] = failures
            if failures < self._domain_failure_threshold:
                return
            self._domain_failures[domain] = 0
            self._domain_cooldowns[domain] = time.time() + self._domain_cooldown_seconds
            print(
                f" ⚠️ Mail.tm 域名 {domain} 连续触发 registration_disallowed，"
                f"已冷却 {int(self._domain_cooldown_seconds)}s，后续自动切换其它域名"
            )

    def _login_token(self, email: str, password: str) -> str:
        response = self.session.post(
            f"{self.api_base}/token",
            headers=self._headers(use_json=True),
            json={"address": email, "password": password},
            timeout=self.timeout,
        )
        if not response.ok:
            detail = _response_excerpt(response)
            suffix = f" - {detail}" if detail else ""
            raise RuntimeError(f"Mail.tm 获取 token 失败: HTTP {response.status_code}{suffix}")
        token = str(response.json().get("token") or "").strip()
        if not token:
            raise RuntimeError("Mail.tm 未返回 token")
        return token

    def _ensure_token(self, email: str) -> str | None:
        with self._lock:
            account = dict(self._accounts.get(email) or {})
        if not account:
            return None
        token = str(account.get("token") or "").strip()
        if token:
            return token
        password = str(account.get("password") or "").strip()
        if not password:
            return None
        token = self._login_token(email, password)
        with self._lock:
            current = dict(self._accounts.get(email) or {})
            current["token"] = token
            self._accounts[email] = current
        return token

    def create_temp_email(self) -> tuple[str, str]:
        domain = self._resolve_domain()
        last_error: Exception | None = None
        last_error_message = ""
        for _ in range(5):
            address = f"{_generate_local_part(self.prefix, length=10)}@{domain}"
            password = secrets.token_urlsafe(18)
            try:
                with self._create_lock:
                    wait_seconds = max(0.0, self._rate_limited_until - time.time())
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)

                    response = self.session.post(
                        f"{self.api_base}/accounts",
                        headers=self._headers(
                            use_json=True,
                            use_api_key=self._supports_optional_api_key(),
                        ),
                        json={
                            **({"expiresIn": int(self.extra_config["expiresIn"])} if self.provider_name == "duckmail" and str(self.extra_config.get("expiresIn", "")).strip() else {}),
                            "address": address,
                            "password": password,
                        },
                        timeout=self.timeout,
                    )
                    if response.status_code == 429:
                        retry_after = _retry_after_seconds(response, default=5.0)
                        self._rate_limited_until = time.time() + retry_after
                    else:
                        self._rate_limited_until = 0.0

                    if response.status_code not in {200, 201}:
                        detail = _response_excerpt(response)
                        last_error_message = f"HTTP {response.status_code}"
                        if detail:
                            last_error_message = f"{last_error_message} - {detail}"
                        continue

                    token = self._login_token(address, password)
                with self._lock:
                    self._accounts[address] = {
                        "token": token,
                        "password": password,
                    }
                return address, password
            except Exception as exc:
                last_error = exc
                last_error_message = str(exc)
                if "HTTP 429" in last_error_message:
                    time.sleep(1.0)
                continue
        raise RuntimeError(f"Mail.tm 创建邮箱失败: {last_error_message or last_error or '未知错误'}")

    def fetch_emails(self, email: str) -> list[dict[str, str]]:
        token = self._ensure_token(email)
        if not token:
            return []
        try:
            response = self.session.get(
                f"{self.api_base}/messages",
                headers=self._headers(token=token),
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException:
            return []

        if isinstance(payload, list):
            summaries = payload
        else:
            summaries = payload.get("hydra:member") or payload.get("messages") or []

        messages: list[dict[str, str]] = []
        for item in summaries:
            if not isinstance(item, dict):
                continue
            message_id = str(item.get("id") or "").strip()
            detail = {}
            if message_id:
                try:
                    detail_response = self.session.get(
                        f"{self.api_base}/messages/{message_id}",
                        headers=self._headers(token=token),
                        timeout=self.timeout,
                    )
                    if detail_response.ok:
                        detail = detail_response.json()
                except requests.RequestException:
                    detail = {}

            html = detail.get("html") or item.get("html") or ""
            if isinstance(html, list):
                html = "\n".join(str(part) for part in html if part)
            content = "\n".join(
                part
                for part in [
                    str(item.get("subject") or detail.get("subject") or ""),
                    str(detail.get("intro") or item.get("intro") or ""),
                    str(detail.get("text") or item.get("text") or ""),
                    str(html or ""),
                ]
                if part
            )
            messages.append(
                {
                    "id": message_id,
                    "emailId": message_id,
                    "subject": str(item.get("subject") or detail.get("subject") or ""),
                    "content": content,
                    "text": content,
                }
            )
        return messages


class TempMailLolAdapter(BasePollingMailClient):
    def __init__(
        self,
        *,
        base_url: str = "https://api.tempmail.lol",
        proxy: str | None = None,
        prefix: str | None = None,
        domain: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__()
        self.api_base = base_url.rstrip("/")
        self.prefix = (prefix or "").strip() or None
        self.domain = (domain or "").strip() or None
        self.timeout = timeout
        self.session = _build_session(proxy)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
            }
        )
        self._tokens: dict[str, str] = {}
        self._lock = threading.Lock()

    def create_temp_email(self) -> tuple[str, str]:
        payload: dict[str, str] = {}
        if self.prefix and self.domain:
            payload["address"] = f"{_generate_local_part(self.prefix, length=10)}@{self.domain}"
        response = self.session.post(f"{self.api_base}/v2/inbox/create", json=payload, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        address = str(data.get("address") or data.get("email") or "").strip()
        token = str(data.get("token") or "").strip()
        if not address or not token:
            raise RuntimeError("TempMail.lol 创建邮箱失败: 响应缺少 address/token")
        with self._lock:
            self._tokens[address] = token
        return address, token

    def fetch_emails(self, email: str) -> list[dict[str, str]]:
        with self._lock:
            token = str(self._tokens.get(email) or "").strip()
        if not token:
            return []
        try:
            response = self.session.get(f"{self.api_base}/v2/inbox", params={"token": token}, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException:
            return []

        emails = payload.get("emails") if isinstance(payload, dict) else []
        messages: list[dict[str, str]] = []
        for item in emails or []:
            if not isinstance(item, dict):
                continue
            subject = str(item.get("subject") or "")
            body = str(item.get("body") or "")
            html = item.get("html") or ""
            if isinstance(html, list):
                html = "\n".join(str(part) for part in html if part)
            content = "\n".join(part for part in [subject, body, str(html or "")] if part)
            message_id = str(item.get("id") or item.get("_id") or uuid.uuid4().hex)
            messages.append(
                {
                    "id": message_id,
                    "emailId": message_id,
                    "subject": subject,
                    "content": content,
                    "text": content,
                }
            )
        return messages


class TemporamAdapter(BasePollingMailClient):
    def __init__(
        self,
        *,
        base_url: str = "https://temporam.com",
        proxy: str | None = None,
        prefix: str | None = None,
        domain: str | None = None,
        secret: str | None = None,
        timeout: float = 30.0,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.api_base = base_url.rstrip("/")
        self.proxy = normalize_proxy_url(proxy)
        self.prefix = (prefix or "").strip() or None
        self.domain = (domain or "").strip() or None
        self.secret = (secret or "").strip()
        self.timeout = timeout
        self.extra_config = extra_config or {}
        self.session = _build_session(proxy)
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0",
                "Referer": f"{self.api_base}/zh",
            }
        )
        configured_domains = self.extra_config.get("domains")
        if isinstance(configured_domains, list):
            self.domains = [str(item).strip() for item in configured_domains if str(item).strip()]
        else:
            self.domains = []
        if self.domain:
            self.domains.insert(0, self.domain)
        if not self.domains:
            self.domains = ["nooboy.com"]
        self._cookie_cache: dict[str, str] = self._parse_cookie_text(self.secret)
        self._cookie_lock = threading.Lock()
        self._sessions: dict[str, dict[str, str]] = {}
        self._session_lock = threading.Lock()

    @staticmethod
    def _parse_cookie_text(value: str | dict[str, Any] | None) -> dict[str, str]:
        if isinstance(value, dict):
            return {str(key): str(item) for key, item in value.items() if str(key).strip()}
        text = str(value or "").strip()
        if not text:
            return {}
        if text.startswith("{"):
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = {}
            if isinstance(payload, dict):
                return {str(key): str(item) for key, item in payload.items() if str(key).strip()}
        cookies: dict[str, str] = {}
        for chunk in text.split(";"):
            part = chunk.strip()
            if not part or "=" not in part:
                continue
            name, cookie_value = part.split("=", 1)
            cookies[name.strip()] = cookie_value.strip()
        return cookies

    def _bootstrap_cookies(self) -> dict[str, str]:
        try:
            from DrissionPage import Chromium, ChromiumOptions
        except ImportError as exc:
            raise RuntimeError(
                "Temporam 需要已配置 mail_secret Cookie，或先安装 DrissionPage 后再自动获取 Cookie"
            ) from exc

        options = ChromiumOptions()
        if hasattr(options, "headless"):
            options.headless(_coerce_bool(self.extra_config.get("bootstrap_headless"), default=True))
        if self.proxy and hasattr(options, "set_proxy"):
            options.set_proxy(self.proxy)
        if hasattr(options, "set_argument"):
            options.set_argument("--no-sandbox")
            options.set_argument("--disable-gpu")
            options.set_argument("--disable-dev-shm-usage")
        browser = Chromium(options)
        try:
            tab = browser.latest_tab
            tab.get(f"{self.api_base}/zh")
            time.sleep(float(self.extra_config.get("bootstrap_wait_seconds") or 5))
            cookies: object = {}
            if hasattr(tab, "cookies"):
                try:
                    cookies = tab.cookies(as_dict=True)
                except TypeError:
                    cookies = tab.cookies()
            if not cookies and hasattr(browser, "cookies"):
                try:
                    cookies = browser.cookies(as_dict=True)
                except TypeError:
                    cookies = browser.cookies()
            if isinstance(cookies, dict):
                return {str(key): str(value) for key, value in cookies.items() if str(key).strip()}
            if isinstance(cookies, list):
                return {
                    str(item.get("name") or ""): str(item.get("value") or "")
                    for item in cookies
                    if isinstance(item, dict) and str(item.get("name") or "").strip()
                }
            raise RuntimeError("Temporam Cookie 获取失败")
        finally:
            try:
                browser.quit()
            except Exception:
                pass

    def _ensure_cookies(self) -> dict[str, str]:
        with self._cookie_lock:
            if self._cookie_cache:
                return dict(self._cookie_cache)
        cookies_from_extra = self._parse_cookie_text(self.extra_config.get("cookies"))
        if cookies_from_extra:
            with self._cookie_lock:
                self._cookie_cache = cookies_from_extra
                return dict(self._cookie_cache)
        cookies = self._bootstrap_cookies()
        with self._cookie_lock:
            self._cookie_cache = cookies
            return dict(self._cookie_cache)

    def create_temp_email(self) -> tuple[str, str]:
        self._ensure_cookies()
        address = f"{_generate_local_part(self.prefix, length=10)}@{random.choice(self.domains)}"
        with self._session_lock:
            self._sessions[address] = {"email": address}
        return address, address

    def fetch_emails(self, email: str) -> list[dict[str, str]]:
        try:
            cookies = self._ensure_cookies()
        except RuntimeError:
            return []

        response = self.session.get(
            f"{self.api_base}/api/email/messages",
            params={"email": email},
            cookies=cookies,
            timeout=self.timeout,
        )
        if response.status_code == 403:
            with self._cookie_lock:
                self._cookie_cache = {}
            try:
                cookies = self._ensure_cookies()
            except RuntimeError:
                return []
            response = self.session.get(
                f"{self.api_base}/api/email/messages",
                params={"email": email},
                cookies=cookies,
                timeout=self.timeout,
            )
        if not response.ok:
            return []
        try:
            payload = response.json()
        except ValueError:
            payload = [{"content": response.text}]

        if isinstance(payload, dict):
            items = payload.get("messages") or payload.get("results") or []
        else:
            items = payload

        messages: list[dict[str, str]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            subject = str(item.get("subject") or "")
            content = str(item.get("content") or "")
            summary = str(item.get("summary") or "")
            message_id = str(item.get("id") or item.get("_id") or uuid.uuid4().hex)
            merged = "\n".join(part for part in [subject, summary, content] if part)
            messages.append(
                {
                    "id": message_id,
                    "emailId": message_id,
                    "subject": subject,
                    "content": merged,
                    "text": merged,
                }
            )
        return messages


class Custom2925Adapter(BasePollingMailClient):
    def __init__(
        self,
        *,
        proxy: str | None = None,
        prefix: str | None = None,
        domain: str | None = None,
        api_key: str | None = None,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        del proxy
        self.api_base = "imap"
        self.extra_config = extra_config or {}
        self.alias_prefix = (prefix or self.extra_config.get("alias_prefix") or "").strip()
        self.domain = (domain or self.extra_config.get("domain") or "").strip()
        self.base_email = str(self.extra_config.get("base_email") or self.extra_config.get("imap_user") or "").strip()
        self.alias_separator = str(self.extra_config.get("alias_separator") or "b")
        self.start_index = int(self.extra_config.get("start_index") or 1)
        self.imap_host = str(self.extra_config.get("imap_host") or "").strip()
        self.imap_port = int(self.extra_config.get("imap_port") or 993)
        self.imap_ssl = _coerce_bool(self.extra_config.get("imap_ssl"), default=True)
        self.imap_user = str(self.extra_config.get("imap_user") or self.base_email or "").strip()
        self.imap_password = str(self.extra_config.get("imap_password") or api_key or "").strip()
        self.mailbox = str(self.extra_config.get("mailbox") or "INBOX").strip() or "INBOX"
        self.lookback_seconds = max(int(self.extra_config.get("lookback_seconds") or 300), 0)
        self.counter_file = str(self.extra_config.get("counter_file") or "custom2925_counter.json").strip() or "custom2925_counter.json"
        self._counter_lock = threading.Lock()
        self._session_lock = threading.Lock()
        self._sessions: dict[str, dict[str, Any]] = {}

    def _resolve_counter_path(self) -> Path:
        path = Path(self.counter_file)
        if path.is_absolute():
            return path
        return Path.cwd() / path

    def _ensure_counter_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(json.dumps({"next_index": self.start_index}, ensure_ascii=False, indent=2), encoding="utf-8")

    def _next_alias_index(self) -> int:
        counter_path = self._resolve_counter_path()
        with self._counter_lock:
            self._ensure_counter_file(counter_path)
            try:
                payload = json.loads(counter_path.read_text(encoding="utf-8") or "{}")
            except Exception:
                payload = {}
            current = int(payload.get("next_index", self.start_index))
            payload["next_index"] = current + 1
            counter_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return current

    def _build_alias(self, index: int) -> str:
        if not self.alias_prefix or not self.domain:
            raise RuntimeError("2925 自有邮箱缺少 alias_prefix 或 domain 配置")
        return f"{self.alias_prefix}{self.alias_separator}{index}@{self.domain}"

    def _connect_imap(self):
        if not self.imap_host or not self.imap_user or not self.imap_password:
            raise RuntimeError("2925 IMAP 配置不完整，请检查 host/user/password")
        client = imaplib.IMAP4_SSL(self.imap_host, self.imap_port) if self.imap_ssl else imaplib.IMAP4(self.imap_host, self.imap_port)
        client.login(self.imap_user, self.imap_password)
        status, _ = client.select(self.mailbox)
        if status != "OK":
            client.logout()
            raise RuntimeError(f"选择邮箱文件夹失败: {self.mailbox}")
        return client

    @staticmethod
    def _message_timestamp(msg: Message) -> datetime | None:
        raw_date = msg.get("Date")
        if not raw_date:
            return None
        try:
            dt = parsedate_to_datetime(raw_date)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _message_matches_alias(self, msg: Message, alias_email: str, created_after: datetime) -> bool:
        alias_lower = alias_email.lower()
        for field in ("Delivered-To", "X-Original-To", "To"):
            value = _decode_header_value(msg.get(field))
            if alias_lower in value.lower():
                return True

        subject = _decode_header_value(msg.get("Subject"))
        body = _extract_message_text(msg)
        haystack = f"{subject}\n{body}".lower()
        if alias_lower not in haystack:
            return False

        msg_time = self._message_timestamp(msg)
        if msg_time is None:
            return True
        return msg_time >= created_after

    @staticmethod
    def _message_looks_like_openai(msg: Message, body: str) -> bool:
        keywords = ("openai", "noreply@openai", "chatgpt")
        from_header = _decode_header_value(msg.get("From")).lower()
        subject = _decode_header_value(msg.get("Subject")).lower()
        body_lower = body.lower()
        return any(keyword in from_header or keyword in subject or keyword in body_lower for keyword in keywords)

    def _iter_recent_message_ids(self, client, limit: int = 40) -> list[tuple[str, bytes | str, bool]]:
        try:
            status, data = client.uid("search", None, "ALL")
            if status == "OK" and data and data[0]:
                uids = [uid for uid in data[0].split() if uid]
                if uids:
                    return [(uid.decode(errors="ignore"), uid, True) for uid in reversed(uids[-limit:])]
        except Exception:
            pass

        try:
            message_count = int(client.select()[1][0])
        except Exception as exc:
            raise RuntimeError(f"无法获取收件箱邮件数量: {exc}") from exc

        return [
            (str(seq), str(seq), False)
            for seq in range(message_count, max(0, message_count - limit), -1)
        ]

    def _fetch_rfc822(self, client, fetch_id: bytes | str, use_uid_fetch: bool):
        if use_uid_fetch:
            return client.uid("fetch", fetch_id, "(RFC822)")
        return client.fetch(str(fetch_id), "(RFC822)")

    def _fetch_matching_messages(
        self, alias_email: str, created_after: datetime, seen_uids: set[str]
    ) -> list[tuple[str, Message, str]]:
        client = self._connect_imap()
        try:
            identifiers = self._iter_recent_message_ids(client, limit=40)
            matched: list[tuple[str, Message, str]] = []
            for display_id, fetch_id, use_uid_fetch in identifiers:
                if display_id in seen_uids:
                    continue
                status, msg_data = self._fetch_rfc822(client, fetch_id, use_uid_fetch)
                if status != "OK":
                    continue
                raw_message = None
                for item in msg_data:
                    if isinstance(item, tuple) and len(item) >= 2:
                        raw_message = item[1]
                        break
                if not raw_message:
                    continue
                msg = email.message_from_bytes(raw_message)
                msg_time = self._message_timestamp(msg)
                if msg_time and msg_time < created_after:
                    continue
                body = _extract_message_text(msg)
                if not self._message_looks_like_openai(msg, body):
                    continue
                if not self._message_matches_alias(msg, alias_email, created_after):
                    continue
                matched.append((display_id, msg, body))
            return matched
        finally:
            try:
                client.close()
            except Exception:
                pass
            try:
                client.logout()
            except Exception:
                pass

    def create_temp_email(self) -> tuple[str, str]:
        alias_email = self._build_alias(self._next_alias_index())
        created_at = datetime.now(timezone.utc) - timedelta(seconds=min(self.lookback_seconds, 30))
        with self._session_lock:
            self._sessions[alias_email] = {
                "alias_email": alias_email,
                "created_at": created_at,
                "seen_uids": set(),
            }
        return alias_email, alias_email

    def fetch_emails(self, email: str) -> list[dict[str, str]]:
        with self._session_lock:
            session = self._sessions.get(email)
        if not session:
            created_at = datetime.now(timezone.utc) - timedelta(seconds=min(self.lookback_seconds, 30))
            session = {
                "alias_email": email,
                "created_at": created_at,
                "seen_uids": set(),
            }
            with self._session_lock:
                self._sessions[email] = session

        try:
            messages = self._fetch_matching_messages(
                str(session["alias_email"]),
                session["created_at"],
                set(session["seen_uids"]),
            )
        except Exception:
            return []

        items: list[dict[str, str]] = []
        for uid, msg, body in messages:
            subject = _decode_header_value(msg.get("Subject"))
            items.append(
                {
                    "id": uid,
                    "emailId": uid,
                    "subject": subject,
                    "content": "\n".join(part for part in [subject, body] if part),
                    "text": body,
                }
            )
        return items


def create_extended_mail_client(
    provider: str,
    *,
    api_key: str = "",
    base_url: str = "",
    proxy: str | None = None,
    prefix: str | None = None,
    domain: str | None = None,
    secret: str | None = None,
    timeout: float = 30.0,
    extra_config: dict[str, Any] | None = None,
):
    normalized = str(provider or "").strip().lower().replace("-", "_")
    if normalized == "duckmail":
        return MailTmAdapter(
            base_url=base_url or "https://api.duckmail.sbs",
            api_key=api_key,
            proxy=proxy,
            prefix=prefix,
            domain=domain,
            timeout=timeout,
            provider_name=normalized,
            extra_config=extra_config,
        )
    if normalized == "tempmail_lol":
        return TempMailLolAdapter(
            base_url=base_url or "https://api.tempmail.lol",
            proxy=proxy,
            prefix=prefix,
            domain=domain,
            timeout=timeout,
        )
    if normalized == "temporam":
        return TemporamAdapter(
            base_url=base_url or "https://temporam.com",
            proxy=proxy,
            prefix=prefix,
            domain=domain,
            secret=secret,
            timeout=timeout,
            extra_config=extra_config,
        )
    if normalized in {"custom2925", "mail_2925", "mail2925"}:
        return Custom2925Adapter(
            proxy=proxy,
            prefix=prefix,
            domain=domain,
            api_key=api_key,
            extra_config=extra_config,
        )
    return None


__all__ = [
    "BasePollingMailClient",
    "MailTmAdapter",
    "TempMailLolAdapter",
    "TemporamAdapter",
    "Custom2925Adapter",
    "create_extended_mail_client",
    "_load_extra_config",
]
