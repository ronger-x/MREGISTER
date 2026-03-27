"""
邮箱服务类（改为 Mail.tm）
"""
import random
import secrets
import string
from typing import Any, Dict, List

from curl_cffi import requests

MAILTM_BASE = "https://api.mail.tm"


def _mailtm_headers(*, token: str = "", use_json: bool = False) -> Dict[str, Any]:
    headers = {"Accept": "application/json"}
    if use_json:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _mailtm_domains(proxies: Any = None) -> List[str]:
    resp = requests.get(
        f"{MAILTM_BASE}/domains",
        headers=_mailtm_headers(),
        proxies=proxies,
        impersonate="chrome",
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"获取 Mail.tm 域名失败，状态码: {resp.status_code}")

    data = resp.json()
    domains = []
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("hydra:member") or data.get("items") or []

    for item in items:
        if not isinstance(item, dict):
            continue
        domain = str(item.get("domain") or "").strip()
        is_active = item.get("isActive", True)
        is_private = item.get("isPrivate", False)
        if domain and is_active and not is_private:
            domains.append(domain)

    return domains


class EmailService:
    """使用 Mail.tm 的邮箱服务"""

    def __init__(self, proxies: Any = None):
        self.proxies = proxies

    def create_email(self):
        """创建 Mail.tm 邮箱，返回 (token, email)"""
        try:
            domains = _mailtm_domains(self.proxies)
            if not domains:
                print("[Error] Mail.tm 没有可用域名")
                return None, None
            domain = random.choice(domains)

            for _ in range(5):
                local = f"oc{secrets.token_hex(5)}"
                email = f"{local}@{domain}"
                password = secrets.token_urlsafe(18)

                create_resp = requests.post(
                    f"{MAILTM_BASE}/accounts",
                    headers=_mailtm_headers(use_json=True),
                    json={"address": email, "password": password},
                    proxies=self.proxies,
                    impersonate="chrome",
                    timeout=15,
                )

                if create_resp.status_code not in (200, 201):
                    continue

                token_resp = requests.post(
                    f"{MAILTM_BASE}/token",
                    headers=_mailtm_headers(use_json=True),
                    json={"address": email, "password": password},
                    proxies=self.proxies,
                    impersonate="chrome",
                    timeout=15,
                )

                if token_resp.status_code == 200:
                    token = str(token_resp.json().get("token") or "").strip()
                    if token:
                        return token, email

            print("[Error] Mail.tm 邮箱创建成功但获取 Token 失败")
            return None, None
        except Exception as e:
            print(f"[Error] 请求 Mail.tm API 出错: {e}")
            return None, None

    def fetch_first_email(self, token):
        """轮询获取第一封邮件内容（文本+标题+HTML 拼接）"""
        try:
            url_list = f"{MAILTM_BASE}/messages"
            resp = requests.get(
                url_list,
                headers=_mailtm_headers(token=token),
                proxies=self.proxies,
                impersonate="chrome",
                timeout=15,
            )
            if resp.status_code != 200:
                return None

            data = resp.json()
            messages = []
            if isinstance(data, list):
                messages = data
            elif isinstance(data, dict):
                messages = data.get("hydra:member") or data.get("messages") or []

            if not messages:
                return None

            msg = messages[0]
            msg_id = str(msg.get("id") or "").strip()
            if not msg_id:
                return None

            read_resp = requests.get(
                f"{MAILTM_BASE}/messages/{msg_id}",
                headers=_mailtm_headers(token=token),
                proxies=self.proxies,
                impersonate="chrome",
                timeout=15,
            )
            if read_resp.status_code != 200:
                return None

            mail_data = read_resp.json()
            subject = str(mail_data.get("subject") or "")
            intro = str(mail_data.get("intro") or "")
            text = str(mail_data.get("text") or "")
            html = mail_data.get("html") or ""
            if isinstance(html, list):
                html = "\n".join(str(x) for x in html)
            return "\n".join([subject, intro, text, str(html)])
        except Exception as e:
            print(f"获取邮件失败: {e}")
            return None
