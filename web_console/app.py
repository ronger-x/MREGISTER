from __future__ import annotations

import hashlib
import hmac
import json
import math
import mimetypes
import os
import secrets
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile
from calendar import monthrange
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field


ROOT_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = WEB_DIR / "runtime"
TASKS_DIR = RUNTIME_DIR / "tasks"
DB_PATH = RUNTIME_DIR / "app.db"
SESSION_COOKIE = "register_console_session"
SESSION_TTL_HOURS = max(1, int(os.getenv("WEB_CONSOLE_SESSION_TTL_HOURS", "24")))
MAX_CONCURRENT_TASKS = max(1, int(os.getenv("WEB_CONSOLE_MAX_CONCURRENT_TASKS", "2")))
POLL_INTERVAL_SECONDS = max(1.0, float(os.getenv("WEB_CONSOLE_POLL_INTERVAL", "2.0")))

PLATFORMS = {
    "browser-automation-local": {
        "label": "Browser Automation Local",
        "requires_email_credential": False,
        "requires_captcha_credential": False,
        "supports_proxy": True,
        "default_concurrency": 1,
        "notes": "Runs a local browser automation adapter from browser_automation/runner.py inside the MREGISTER task framework.",
    },
    "chatgpt-register-v2": {
        "label": "ChatGPT Register v2",
        "requires_email_credential": True,
        "requires_captcha_credential": False,
        "supports_proxy": True,
        "default_concurrency": 1,
        "notes": "Uses GPTMail via the chatgpt_register_v2 mail adapter and writes account/token files into the task directory.",
    },
    "chatgpt-register-v3": {
        "label": "ChatGPT Register v3",
        "requires_email_credential": True,
        "requires_captcha_credential": False,
        "supports_proxy": True,
        "default_concurrency": 1,
        "notes": "Uses Codex registration flow with MoeMail/Cloudflare Temp Email support via the chatgpt_register_v3 adapter.",
    },
    "grok-register": {
        "label": "Grok Register",
        "requires_email_credential": False,
        "requires_captcha_credential": True,
        "supports_proxy": False,
        "default_concurrency": 4,
        "notes": "Uses YesCaptcha. The worker feeds the original CLI with a concurrency value via stdin.",
    },
}

BROWSER_AUTOMATION_DEFAULTS = {
    "adapter_path": "browser_automation/runner.py",
    "headless": True,
    "browser_path": "",
    "extra_args": "",
    "env_json": "{}",
}

BROWSER_AUTOMATION_TEMPLATES = {
    "generic-form": {
        "label": "Generic Form",
        "description": "Generic email + verification form flow template for self-owned websites or test pages.",
        "fields": [
            {"key": "start_url", "label": "Start URL", "type": "text", "required": True, "placeholder": "https://example.com/signup"},
            {"key": "email_input_selectors", "label": "Email selectors", "type": "list", "placeholder": "@name=email"},
            {"key": "continue_button_selectors", "label": "Continue selectors", "type": "list", "placeholder": "Continue"},
            {"key": "password_input_selectors", "label": "Password selectors", "type": "list", "placeholder": "@name=password"},
            {"key": "password_button_selectors", "label": "Password submit selectors", "type": "list", "placeholder": "Create account"},
            {"key": "code_input_selectors", "label": "Code selectors", "type": "list", "placeholder": "@name=code"},
            {"key": "verify_button_selectors", "label": "Verify selectors", "type": "list", "placeholder": "Verify"},
            {"key": "success_url_contains", "label": "Success URL contains", "type": "list", "placeholder": "/dashboard"},
            {"key": "code_timeout", "label": "Code timeout", "type": "number", "default": 180},
            {"key": "capture_screenshots", "label": "Capture screenshots", "type": "boolean", "default": True},
        ],
        "defaults": {
            "start_url": "",
            "email_input_selectors": ["@name=email", "@type=email", "tag:input"],
            "continue_button_selectors": ["Continue", "继续", "@type=submit"],
            "password_input_selectors": ["@name=password", "@type=password"],
            "password_button_selectors": ["Create account", "注册", "创建账号", "@type=submit"],
            "code_input_selectors": ["@name=code", "@type=text", "tag:input"],
            "verify_button_selectors": ["Verify", "验证", "@type=submit"],
            "success_url_contains": ["/dashboard", "/welcome"],
            "code_timeout": 180,
            "capture_screenshots": True,
        },
    },
    "gemini-inspired": {
        "label": "Gemini-Inspired Demo",
        "description": "A UI preset modeled after the structure of the downloaded Gemini sample, for local maintenance of a generic multi-step signup flow.",
        "fields": [
            {"key": "start_url", "label": "Start URL", "type": "text", "required": True, "placeholder": "https://example.com/signup"},
            {"key": "cookie_accept_selectors", "label": "Cookie button selectors", "type": "list", "placeholder": "OK, got it"},
            {"key": "step1_button_selectors", "label": "Step 1 button selectors", "type": "list", "placeholder": "开始使用"},
            {"key": "step2_button_selectors", "label": "Step 2 button selectors", "type": "list", "placeholder": "开始 30 天试用"},
            {"key": "email_input_selectors", "label": "Email selectors", "type": "list", "placeholder": "@name=email"},
            {"key": "continue_button_selectors", "label": "Continue selectors", "type": "list", "placeholder": "使用邮箱继续"},
            {"key": "password_input_selectors", "label": "Password selectors", "type": "list", "placeholder": "@name=password"},
            {"key": "password_button_selectors", "label": "Password submit selectors", "type": "list", "placeholder": "下一步"},
            {"key": "code_input_selectors", "label": "Code selectors", "type": "list", "placeholder": "@name=code"},
            {"key": "verify_button_selectors", "label": "Verify selectors", "type": "list", "placeholder": "验证"},
            {"key": "display_name_input_selectors", "label": "Display name selectors", "type": "list", "placeholder": "@name=displayName"},
            {"key": "display_name_button_selectors", "label": "Final submit selectors", "type": "list", "placeholder": "同意并开始使用"},
            {"key": "success_url_contains", "label": "Success URL contains", "type": "list", "placeholder": "/home/cid/"},
            {"key": "code_timeout", "label": "Code timeout", "type": "number", "default": 180},
            {"key": "capture_screenshots", "label": "Capture screenshots", "type": "boolean", "default": True},
        ],
        "defaults": {
            "start_url": "https://example.com/signup",
            "cookie_accept_selectors": ["OK, got it"],
            "step1_button_selectors": ["开始使用", "Get started"],
            "step2_button_selectors": ["开始 30 天试用", "开始试用", "免费试用", "试用"],
            "email_input_selectors": ["#email-input", "@id=email-input", "@name=loginHint", "@aria-label=邮箱", "tag:input"],
            "continue_button_selectors": ["使用邮箱继续", "继续", "Continue", "@type=submit"],
            "password_input_selectors": ["@name=password", "@type=password", "#password", "tag:input"],
            "password_button_selectors": ["下一步", "继续", "Continue", "@type=submit"],
            "code_input_selectors": ["@name=code", "@type=text", "tag:input"],
            "verify_button_selectors": ["验证", "提交", "继续", "下一步", "Verify", "Continue", "@type=submit"],
            "display_name_input_selectors": ["@name=displayName", "@placeholder=全名", "tag:input"],
            "display_name_button_selectors": ["同意并开始使用", "开始使用", "同意并继续", "创建账号"],
            "success_url_contains": ["/home/cid/", "/dashboard", "/welcome"],
            "code_timeout": 180,
            "capture_screenshots": True,
        },
    },
}

UI_TRANSLATIONS = {
    "zh-CN": {
        "site_title": "MREGISTER",
        "request_failed": "请求失败",
        "brand_console": "Register Console",
        "brand_name": "MREGISTER",
        "topbar_workspace": "工作区",
        "auth_setup_title": "首次打开先设置管理员密码",
        "auth_setup_desc": "密码会保存为本地哈希值。未设置密码前，任务、凭据、代理和 API 都不会开放。",
        "auth_login_title": "输入管理员密码进入控制台",
        "auth_login_desc": "当前站点已经启用密码保护，登录后才可查看任务、下载压缩包和操作 API Key。",
        "auth_password": "管理员密码",
        "auth_setup_submit": "保存并进入后台",
        "auth_login_submit": "登录",
        "nav_dashboard": "首页",
        "nav_credentials": "凭据",
        "nav_proxies": "代理",
        "nav_task_center": "任务中心",
        "nav_create_task": "新建任务",
        "nav_task_detail": "任务详情",
        "nav_success_accounts": "成功账号",
        "nav_schedules": "定时任务",
        "nav_api_keys": "API 接口",
        "nav_docs": "API文档",
        "nav_logout": "退出登录",
        "toggle_sidebar": "收起或展开侧边栏",
        "open_sidebar": "打开侧边栏",
        "close_sidebar": "关闭侧边栏",
        "section_overview": "总览与默认配置",
        "section_task_center": "任务中心",
        "section_success_accounts": "成功账号",
        "panel_defaults_title": "默认设置",
        "panel_defaults_desc": "API 创建任务时会优先使用这里的默认凭据和默认代理。",
        "default_gptmail": "默认邮件凭据",
        "default_yescaptcha": "默认 YesCaptcha",
        "default_proxy": "默认代理",
        "save_defaults": "保存默认设置",
        "panel_recent_tasks_title": "最近任务",
        "panel_recent_tasks_desc": "点任意任务可直接跳到详情页查看控制台输出。",
        "section_credentials": "凭据管理",
        "credentials_create_title": "新增凭据",
        "credentials_create_desc": "支持 GPTMail、DuckMail、TempMail.lol、Temporam、2925、MoeMail、Cloudflare Temp Email 与 YesCaptcha。",
        "gptmail_optional_hint": "GPTMail 的 Base URL、邮箱前缀、邮箱域名都有默认值，可直接留空不填写。",
        "mail_provider_optional_hint": "大部分邮件服务只需要填写基础字段；像 2925/Temporam 这类高级配置可放到 Extra JSON。",
        "credentials_saved_title": "已保存凭据",
        "credentials_saved_desc": "支持删除、查看备注、设为默认。",
        "credential_exhausted_badge": "已耗尽",
        "credential_exhausted_reason": "此 GPTMail 凭据已标记为耗尽，系统会自动跳过并切换其他可用凭据。",
        "field_name": "名称",
        "field_kind": "类型",
        "field_api_key": "API Key",
        "field_api_key_optional": "API Key / 密码（可选）",
        "field_base_url": "Base URL",
        "field_prefix": "邮箱前缀",
        "field_domain": "邮箱域名",
        "field_secret": "Secret / Cookie",
        "field_extra_json": "Extra JSON",
        "field_base_url_placeholder": "留空使用默认 Base URL",
        "field_prefix_placeholder": "留空使用默认邮箱前缀",
        "field_domain_placeholder": "留空使用默认邮箱域名",
        "field_secret_placeholder": "例如站点密钥、Cookie 字符串或访问密码",
        "field_extra_json_placeholder": "例如 {\"imap_host\":\"imap.2925.com\",\"imap_user\":\"you@2925.com\"}",
        "field_notes": "备注",
        "save_credential": "保存凭据",
        "copy_api_key": "复制 API Key",
        "copy_api_key_done": "已复制 {name} 的 API Key",
        "copy_api_key_failed": "复制 API Key 失败，请检查浏览器权限",
        "section_proxies": "代理管理",
        "proxies_create_title": "新增代理",
        "proxies_create_desc": "支持保存多个代理，并可指定为站点默认代理。",
        "proxies_saved_title": "已保存代理",
        "proxies_saved_desc": "任务可选择默认代理、指定代理或不使用代理。",
        "field_proxy_url": "代理地址",
        "save_proxy": "保存代理",
        "section_tasks": "新建任务",
        "field_task_name": "任务名称",
        "field_platform": "驱动",
        "field_quantity": "目标数量",
        "field_concurrency": "并发数",
        "field_email_credential": "邮件凭据",
        "field_captcha_credential": "验证码凭据",
        "field_proxy_mode": "代理模式",
        "field_proxy_select": "指定代理",
        "browser_automation_options_title": "浏览器自动化配置",
        "browser_automation_options_desc": "仅对 Browser Automation Local 生效。可覆盖适配器入口、浏览器模式、浏览器路径和附加环境变量。",
        "field_adapter_path": "适配器路径",
        "field_headless": "无头模式",
        "field_browser_path": "浏览器路径",
        "field_extra_args": "附加参数",
        "field_env_json": "环境变量 JSON",
        "field_adapter_path_placeholder": "留空使用默认适配器路径",
        "field_browser_path_placeholder": "留空使用系统默认浏览器",
        "field_extra_args_placeholder": "例如 --lang zh-CN --slow 1",
        "field_env_json_placeholder": "例如 {\"TASK_LABEL\":\"demo\"}",
        "browser_automation_headless_hint": "勾选后以无头模式运行浏览器。",
        "proxy_mode_none": "不使用代理",
        "proxy_mode_default": "使用默认代理",
        "proxy_mode_custom": "指定代理",
        "save_task": "创建并加入队列",
        "created_task_opened": "任务 #{id} 已创建，已打开任务中心详情。",
        "section_task_detail": "任务详情",
        "task_detail_note": "关闭网页不会停止任务，控制台输出会保存到任务目录，重新打开时会继续显示。",
        "task_failure_reason": "失败原因",
        "task_gptmail_quota_exhausted": "因 GPTMail 配额或调用次数耗尽，任务已停止重试。",
        "task_list_title": "任务列表",
        "task_list_desc": "左侧筛选后只显示对应状态的任务。",
        "task_filter_status": "状态筛选",
        "task_filter_all": "全部状态",
        "task_list_mode": "列表类型",
        "task_list_mode_task": "普通任务",
        "task_list_mode_schedule": "定时任务",
        "console_title": "实时控制台",
        "loading": "加载中...",
        "success_accounts_title": "成功账号",
        "success_accounts_empty": "当前还没有可提取的成功账号。",
        "preview_success_accounts": "预览成功账号",
        "download_success_accounts": "下载成功账号",
        "regenerate_oauth_token": "重新获取 OAuth Token",
        "batch_regenerate_oauth_token": "批量重新登录",
        "regenerate_oauth_prompt": "将为 {email} 重新执行 OAuth 登录并写入 tokens 目录。继续吗？",
        "regenerate_oauth_result_title": "OAuth Token 获取结果",
        "regenerate_oauth_success": "已为 {email} 重新获取 OAuth Token。",
        "regenerate_oauth_cpamc_success": "并已导入到 CPAMC。",
        "regenerate_oauth_cpamc_skipped": "CPAMC 已存在相同内容，已跳过重复导入。",
        "regenerate_oauth_cpamc_failed": "CPAMC 导入失败：{error}",
        "batch_regenerate_oauth_prompt": "将为 {count} 个成功账号重新执行 OAuth 登录并写入 tokens 目录。继续吗？",
        "batch_regenerate_oauth_result_title": "批量重新登录结果",
        "batch_regenerate_oauth_result_summary": "共 {total} 个账号，成功 {succeeded} 个，失败 {failed} 个。",
        "batch_regenerate_oauth_result_failures": "失败项：\n{value}",
        "batch_regenerate_oauth_empty": "当前没有可批量重新登录的成功账号。",
        "cpamc_badge_imported": "已导入 CPAMC",
        "cpamc_badge_failed": "导入失败",
        "extract_history_success_accounts": "提取历史成功账号",
        "extract_history_success_accounts_done": "已扫描 {updated} 个任务，其中 {non_empty} 个任务提取到了成功账号。",
        "success_accounts_page_desc": "集中查看所有任务提取出的成功账号，支持查询、分页和按账号重试导入 CPAMC。",
        "search_success_accounts": "搜索成功账号",
        "search_success_accounts_placeholder": "搜索邮箱、任务名或平台",
        "table_task": "任务",
        "table_account": "账号",
        "table_source": "来源",
        "table_status": "状态",
        "table_action": "操作",
        "pagination_prev": "上一页",
        "pagination_next": "下一页",
        "pagination_summary": "第 {page} / {pages} 页，共 {total} 条",
        "success_accounts_empty_list": "当前筛选下没有成功账号。",
        "success_accounts_last_updated": "最近刷新：{value}",
        "success_accounts_auto_refresh": "自动刷新",
        "success_accounts_filter_schedule": "定时任务筛选",
        "success_accounts_filter_all_schedules": "全部任务来源",
        "success_accounts_filter_unknown_schedule": "未知定时任务",
        "success_accounts_filter_hint": "当前仅显示来自“{value}”的成功账号。",
        "success_accounts_source_schedule": "定时任务 #{id}",
        "success_accounts_source_manual": "普通任务",
        "retry_cpamc_import": "重试导入 CPAMC",
        "retry_cpamc_import_done": "已重试导入 {email} 到 CPAMC。",
        "retry_cpamc_import_skipped": "{email} 的 JSON 已导入过，已跳过重复导入。",
        "section_schedules": "定时任务",
        "schedules_create_title": "新增定时任务",
        "schedules_create_desc": "支持每日、每周、每月三种可视化定时配置，并自动生成 cron 表达式。",
        "schedules_saved_title": "已保存定时任务",
        "schedules_saved_desc": "可以启用、停用或删除。",
        "field_schedule_kind": "重复方式",
        "schedule_kind_interval_minutes": "每隔分钟",
        "schedule_kind_interval_hours": "每隔小时",
        "schedule_kind_daily": "每天",
        "schedule_kind_weekly": "每周",
        "schedule_kind_monthly": "每月",
        "field_time_of_day": "执行时间",
        "field_interval_minutes": "分钟间隔",
        "field_interval_hours": "小时间隔",
        "field_interval_minute_offset": "每小时第几分钟",
        "field_schedule_weekdays": "执行星期",
        "field_schedule_day": "每月日期",
        "schedule_builder_hint": "使用可视化方式配置，系统会自动生成对应 cron 表达式。",
        "schedule_generated_cron": "Cron 表达式",
        "schedule_visual_summary": "执行规则：{value}",
        "schedule_human_readable": "自然语言：{value}",
        "schedule_human_interval_minutes": "每隔 {value} 分钟执行一次",
        "schedule_human_interval_hours": "每隔 {hours} 小时，在每小时第 {minute} 分钟执行",
        "schedule_human_weekly": "每周 {days} 的 {time} 执行",
        "schedule_human_monthly": "每月 {day} 日 {time} 执行",
        "schedule_human_daily": "每天 {time} 执行",
        "schedule_next_run": "下次执行时间 {value}",
        "schedule_last_run": "上次执行时间 {value}",
        "schedule_time_unknown": "未触发",
        "schedule_error_name_required": "请输入定时任务名称。",
        "schedule_error_quantity_invalid": "任务数量必须在 1 到 100000 之间。",
        "schedule_error_concurrency_invalid": "并发数必须在 1 到 64 之间。",
        "schedule_error_time_required": "请选择有效的执行时间。",
        "schedule_error_interval_minutes": "分钟间隔必须在 1 到 59 之间。",
        "schedule_error_interval_hours": "小时间隔必须在 1 到 23 之间。",
        "schedule_error_minute_offset": "每小时分钟偏移必须在 0 到 59 之间。",
        "schedule_error_weekdays_required": "每周模式至少选择一天。",
        "schedule_error_day_invalid": "每月日期必须在 1 到 31 之间。",
        "field_use_default_proxy": "使用默认代理",
        "field_schedule_auto_import_cpamc": "完成后自动导入到 CPAMC",
        "save_schedule": "保存定时任务",
        "cpamc_title": "配置 CPAMC",
        "cpamc_desc": "用于绑定 CLI Proxy API Management Center，仅处理 Codex / Grok 相关 JSON 导入。",
        "field_cpamc_enabled": "启用 CLI Proxy API Management Center",
        "field_cpamc_auto_import": "任务完成后自动导入到 CPAMC",
        "cpamc_auto_import_hint": "全局自动导入和定时任务自动导入共用同一条完成后导入链路，不会重复导入；只在达到目标成功数量后触发。",
        "field_cpamc_base_url": "域名/IP 链接",
        "field_cpamc_base_url_placeholder": "例如 http://127.0.0.1:8317 或 http://127.0.0.1:8317/v0/management",
        "field_cpamc_management_key": "管理密钥",
        "field_cpamc_management_key_placeholder": "输入 CPAMC Management Key",
        "save_cpamc": "保存 CPAMC 配置",
        "test_cpamc": "测试链接",
        "cpamc_status_linked": "已连接",
        "cpamc_status_unlinked": "未链接",
        "cpamc_status_disabled": "未启用",
        "cpamc_last_error": "最近错误：{value}",
        "cpamc_import_button": "导入到 CPAMC",
        "cpamc_force_import_button": "强制导入到 CPAMC",
        "cpamc_import_disabled": "当前任务没有可导入的 JSON 文件",
        "cpamc_import_success": "已导入 {count} 个 JSON 文件到 CPAMC。",
        "cpamc_import_partial": "已导入 {success} 个，失败 {failed} 个。",
        "cpamc_import_skipped_only": "检测到 {skipped} 个 JSON 文件已导入过，已全部跳过。",
        "cpamc_import_with_skipped": "已导入 {success} 个，跳过 {skipped} 个，失败 {failed} 个。",
        "cpamc_import_result_title": "导入完成",
        "modal_close": "关闭",
        "section_api": "API 接口",
        "api_create_title": "创建 API Key",
        "api_create_desc": "新建成功后只会显示一次，请立即保存。",
        "api_saved_title": "已有 API Key",
        "api_saved_desc": "可用于外部程序调用创建任务、查询状态和下载结果。",
        "save_api_key": "生成 API Key",
        "section_docs": "API文档",
        "docs_intro_title": "总览",
        "docs_intro_desc": "控制台支持网页操作和外部 API 调用。外部 API 默认使用站点中已配置的默认 GPTMail、默认 YesCaptcha 和默认代理。通过 API 创建的任务会在完成 24 小时后自动清理。",
        "docs_deploy_title": "部署方式",
        "docs_deploy_desc": "推荐优先使用 Docker Compose 部署，默认直接拉取 `maishanhub/mregister:main` 镜像，便于快速上线和保留运行数据；如果只是本地调试，也可以直接用 Python 启动。",
        "docs_local_deploy_title": "本地 Python 启动",
        "docs_compose_deploy_title": "Docker Compose 启动",
        "docs_api_flow_title": "API 调用流程",
        "docs_api_flow_desc": "推荐顺序：先在控制台创建 API Key，再调用创建任务接口，随后轮询查询状态，最后在任务完成后下载压缩包。",
        "docs_endpoints_title": "接口列表",
        "docs_create_params_title": "创建任务参数",
        "docs_create_example_title": "创建任务示例",
        "docs_query_example_title": "查询任务示例",
        "docs_download_example_title": "下载结果示例",
        "docs_response_title": "返回说明",
        "docs_response_desc": "`completed_count` 表示任务当前真实完成数，不按尝试次数计算。只有任务完成并且压缩包生成后，查询接口才会返回 `download_url`。API 创建的任务会在 `auto_delete_at` 指定时间后自动删除。",
        "table_method": "方法",
        "table_path": "路径",
        "table_desc": "说明",
        "table_field": "字段",
        "table_type": "类型",
        "table_required": "必填",
        "endpoint_create_desc": "创建一个新的外部任务",
        "endpoint_query_desc": "查询任务状态、真实完成数量和下载地址",
        "endpoint_download_desc": "下载任务结果压缩包",
        "required_yes": "是",
        "required_no": "否",
        "param_platform_desc": "驱动名称，目前支持 `browser-automation-local`、`chatgpt-register-v2`、`chatgpt-register-v3` 和 `grok-register`",
        "param_quantity_desc": "目标成功数量，系统按真实成功数判断完成，不按尝试次数计算",
        "param_use_proxy_desc": "是否启用默认代理，不传或传 false 表示不使用代理",
        "param_concurrency_desc": "并发数，默认 1",
        "param_name_desc": "自定义任务名，不传则由系统自动生成",
        "docs_flow_1": "1. 在“API 接口”页面创建 API Key。",
        "docs_flow_2": "2. 调用 `POST /api/external/tasks` 创建任务。",
        "docs_flow_3": "3. 轮询 `GET /api/external/tasks/{task_id}` 查询状态和完成数。",
        "docs_flow_4": "4. 任务完成后调用 `GET /api/external/tasks/{task_id}/download` 下载压缩包。",
        "dashboard_running_tasks": "运行中任务",
        "dashboard_completed_tasks": "已完成任务",
        "dashboard_credential_count": "凭据数量",
        "dashboard_proxy_count": "代理数量",
        "empty_tasks": "暂无任务",
        "empty_credentials": "暂无凭据",
        "empty_proxies": "暂无代理",
        "empty_filtered_tasks": "当前筛选下没有任务",
        "empty_schedules": "暂无定时任务",
        "empty_api_keys": "暂无 API Key",
        "default_badge": "默认",
        "created_at": "创建于 {value}",
        "task_duration_label": "耗时 {value}",
        "task_duration_value": "{value}",
        "task_duration_running": "已运行 {value}",
        "task_duration_pending": "等待中 {value}",
        "task_duration_unknown": "耗时未知",
        "task_live_timer_hint": "本次运行",
        "task_started_time": "总开始时间",
        "task_current_run_started_time": "本次开始时间",
        "task_finished_time": "结束时间",
        "task_run_duration": "耗时",
        "task_time_unknown": "--",
        "last_used_at": "最近使用时间 {value}",
        "unused": "暂未使用",
        "use_default_gptmail": "使用默认邮件凭据",
        "use_default_yescaptcha": "使用默认 YesCaptcha",
        "choose_proxy": "选择一个代理",
        "no_default_gptmail": "不设置默认邮件凭据",
        "no_default_yescaptcha": "不设置默认 YesCaptcha",
        "no_default_proxy": "不使用默认代理",
        "current_default": "当前默认",
        "set_default": "设为默认",
        "delete": "删除",
        "enable": "启用",
        "disable": "停用",
        "stop_task": "停止任务",
        "rerun_task": "重新执行",
        "rerun_task_confirm": "从 0 开始重新执行任务 #{id}？将克隆原配置并创建新任务。",
        "run_schedule_now": "立即执行",
        "download_zip": "下载压缩包",
        "delete_task": "删除任务",
        "save_now": "新建成功，请立即保存",
        "created_task_modal_title": "任务已创建",
        "created_task_modal_confirm": "前往任务详情",
        "created_task_modal_cancel": "继续创建任务",
        "status_queued": "排队中",
        "status_running": "运行中",
        "status_stopping": "停止中",
        "status_completed": "已完成",
        "status_partial": "部分完成",
        "status_failed": "失败",
        "status_stopped": "已停止",
        "status_interrupted": "已中断",
        "task_detail_empty_title": "当前筛选下没有任务",
        "task_detail_empty_desc": "调整左侧状态筛选，或先创建新的任务。",
        "console_wait": "等待选择任务后显示实时控制台输出。",
        "console_empty": "当前还没有控制台输出。",
        "task_header_meta": "{platform} | 目标数量 {quantity} | 完成数量 {completed} | 当前状态 {status}",
        "created_task_confirm": "任务 #{id} 已创建。可前往任务详情查看进度，或留在当前页面继续创建。",
        "delete_task_confirm": "删除任务 #{id}？",
        "delete_credential_confirm": "删除凭据 {name}？",
        "delete_proxy_confirm": "删除代理 {name}？",
        "delete_schedule_confirm": "删除这个定时任务？",
        "delete_api_key_confirm": "删除这个 API Key？",
        "schedule_meta": "{platform} | 每日 {time} | 数量 {quantity} | {enabled}",
        "schedule_meta_visual": "{platform} | {summary} | 数量 {quantity} | {enabled}",
        "schedule_proxy_on": "使用默认代理",
        "schedule_proxy_off": "不使用代理",
        "schedule_cpamc_auto_import_on": "完成后自动导入 CPAMC",
        "schedule_cpamc_auto_import_off": "不自动导入 CPAMC",
        "schedule_detail_title": "定时任务详情",
        "schedule_target_quantity": "目标数量 {value}",
        "schedule_completed_quantity": "完成数量 {value}",
        "schedule_today_status": "今日状态 {value}",
        "schedule_completed_runs": "已完成 {value} 次定时任务",
        "schedule_today_none": "今日未触发",
        "schedule_tag_suffix": "· 定时",
        "schedule_runs_short": "次已完成",
        "schedule_today_detail_title": "今日任务",
        "schedule_latest_task_title": "最近一次执行",
        "schedule_console_empty": "今日暂无控制台输出",
        "task_center_title": "任务中心",
        "task_center_desc": "把创建任务、运行追踪、定时任务和成功账号集中到同一个工作台里。",
        "task_center_header_create": "新建任务",
        "task_center_header_schedule": "定时任务",
        "task_center_tab_overview": "总览",
        "task_center_tab_create": "新建任务",
        "task_center_tab_detail": "任务列表与详情",
        "task_center_tab_schedules": "定时任务",
        "task_center_tab_success_accounts": "成功账号",
        "task_center_metric_running": "运行中任务",
        "task_center_metric_finished": "已完成任务",
        "task_center_metric_attention": "需关注任务",
        "task_center_metric_schedules": "启用中的定时任务",
        "task_center_quick_actions_title": "快捷操作",
        "task_center_quick_actions_desc": "常用动作直接从这里开始，不需要在多个入口间来回跳转。",
        "task_center_action_create": "创建新任务",
        "task_center_action_create_desc": "填写驱动、数量、并发和代理后立即入队。",
        "task_center_action_detail": "查看最新任务",
        "task_center_action_detail_desc": "继续查看任务 #{id} 的日志、结果和操作。",
        "task_center_action_schedule": "管理定时任务",
        "task_center_action_schedule_desc": "配置每日自动运行的批量任务。",
        "task_center_action_results": "查看成功账号",
        "task_center_action_results_desc": "统一检索、下载和处理所有成功账号。",
        "task_center_health_title": "资源状态",
        "task_center_health_desc": "开始任务前先确认默认资源和后处理链路已经准备完成。",
        "task_center_health_credentials": "邮件凭据",
        "task_center_health_proxies": "代理资源",
        "task_center_health_results": "已有结果任务数",
        "task_center_health_cpamc": "CPAMC 状态",
        "task_center_health_ready": "已就绪",
        "task_center_health_missing": "未配置",
        "task_center_health_optional": "可选",
        "task_center_recent_title": "最近任务",
        "task_center_recent_desc": "从这里快速回到最近运行、完成或失败的任务。",
        "api_key_meta": "{prefix}... | 创建于 {created_at}",
    },
    "en": {
        "site_title": "MREGISTER",
        "request_failed": "Request failed",
        "brand_console": "Register Console",
        "brand_name": "MREGISTER",
        "topbar_workspace": "Workspace",
        "auth_setup_title": "Set the admin password on first visit",
        "auth_setup_desc": "The password is stored as a local hash. Tasks, credentials, proxies, and API access stay locked until it is configured.",
        "auth_login_title": "Enter the admin password",
        "auth_login_desc": "This site is password protected. Sign in before viewing tasks, downloading archives, or managing API keys.",
        "auth_password": "Admin password",
        "auth_setup_submit": "Save and enter console",
        "auth_login_submit": "Sign in",
        "nav_dashboard": "Dashboard",
        "nav_credentials": "Credentials",
        "nav_proxies": "Proxies",
        "nav_task_center": "Task Center",
        "nav_create_task": "New Task",
        "nav_task_detail": "Task Detail",
        "nav_success_accounts": "Success Accounts",
        "nav_schedules": "Schedules",
        "nav_api_keys": "API",
        "nav_docs": "API Docs",
        "nav_logout": "Sign out",
        "toggle_sidebar": "Collapse or expand sidebar",
        "open_sidebar": "Open sidebar",
        "close_sidebar": "Close sidebar",
        "section_overview": "Overview and Defaults",
        "section_task_center": "Task Center",
        "section_success_accounts": "Success Accounts",
        "panel_defaults_title": "Default settings",
        "panel_defaults_desc": "API-created tasks will use these default credentials and proxy settings first.",
        "default_gptmail": "Default Mail Credential",
        "default_yescaptcha": "Default YesCaptcha",
        "default_proxy": "Default proxy",
        "save_defaults": "Save defaults",
        "panel_recent_tasks_title": "Recent tasks",
        "panel_recent_tasks_desc": "Click any task to jump straight into the detail view and console output.",
        "section_credentials": "Credential Management",
        "credentials_create_title": "Add credential",
        "credentials_create_desc": "Supports GPTMail, DuckMail, TempMail.lol, Temporam, 2925, MoeMail, Cloudflare Temp Email, and YesCaptcha.",
        "gptmail_optional_hint": "For GPTMail, Base URL, email prefix, and email domain all have defaults, so you can leave them blank.",
        "mail_provider_optional_hint": "Most mail providers only need the basic fields. Advanced 2925/Temporam settings can go into Extra JSON.",
        "credentials_saved_title": "Saved credentials",
        "credentials_saved_desc": "Delete, review notes, and set defaults here.",
        "credential_exhausted_badge": "exhausted",
        "credential_exhausted_reason": "This GPTMail credential is marked exhausted and will be skipped automatically.",
        "field_name": "Name",
        "field_kind": "Type",
        "field_api_key": "API Key",
        "field_api_key_optional": "API Key / Password (Optional)",
        "field_base_url": "Base URL",
        "field_prefix": "Email prefix",
        "field_domain": "Email domain",
        "field_secret": "Secret / Cookie",
        "field_extra_json": "Extra JSON",
        "field_base_url_placeholder": "Leave blank to use the default Base URL",
        "field_prefix_placeholder": "Leave blank to use the default email prefix",
        "field_domain_placeholder": "Leave blank to use the default email domain",
        "field_secret_placeholder": "For example, a site secret, cookie string, or access password",
        "field_extra_json_placeholder": "For example {\"imap_host\":\"imap.2925.com\",\"imap_user\":\"you@2925.com\"}",
        "field_notes": "Notes",
        "save_credential": "Save credential",
        "copy_api_key": "Copy API key",
        "copy_api_key_done": "Copied API key for {name}",
        "copy_api_key_failed": "Failed to copy API key. Check browser clipboard permissions.",
        "section_proxies": "Proxy Management",
        "proxies_create_title": "Add proxy",
        "proxies_create_desc": "Save multiple proxies and promote one as the site-wide default.",
        "proxies_saved_title": "Saved proxies",
        "proxies_saved_desc": "Tasks can use the default proxy, a specific proxy, or no proxy at all.",
        "field_proxy_url": "Proxy URL",
        "save_proxy": "Save proxy",
        "section_tasks": "Create Task",
        "field_task_name": "Task name",
        "field_platform": "Driver",
        "field_quantity": "Target quantity",
        "field_concurrency": "Concurrency",
        "field_email_credential": "Email credential",
        "field_captcha_credential": "Captcha credential",
        "field_proxy_mode": "Proxy mode",
        "field_proxy_select": "Specific proxy",
        "browser_automation_options_title": "Browser Automation Options",
        "browser_automation_options_desc": "Only used by Browser Automation Local. Override the adapter entry path, browser mode, browser executable, and extra environment variables.",
        "field_adapter_path": "Adapter path",
        "field_headless": "Headless mode",
        "field_browser_path": "Browser path",
        "field_extra_args": "Extra args",
        "field_env_json": "Environment JSON",
        "field_adapter_path_placeholder": "Leave empty to use the default adapter path",
        "field_browser_path_placeholder": "Leave empty to use the system browser",
        "field_extra_args_placeholder": "For example: --lang zh-CN --slow 1",
        "field_env_json_placeholder": "For example: {\"TASK_LABEL\":\"demo\"}",
        "browser_automation_headless_hint": "Enable to run the browser in headless mode.",
        "proxy_mode_none": "No proxy",
        "proxy_mode_default": "Use default proxy",
        "proxy_mode_custom": "Use selected proxy",
        "save_task": "Create and queue task",
        "created_task_opened": "Task #{id} was created and opened in Task Center.",
        "section_task_detail": "Task Detail",
        "task_detail_note": "Closing the page does not stop a task. Console output is saved in the task directory and will be shown again when you reopen it.",
        "task_failure_reason": "Failure reason",
        "task_gptmail_quota_exhausted": "The task stopped retrying because GPTMail quota or call usage was exhausted.",
        "loading": "Loading...",
        "success_accounts_page_desc": "Browse extracted successful accounts across all tasks with search, pagination, and CPAMC retry actions.",
        "search_success_accounts": "Search success accounts",
        "search_success_accounts_placeholder": "Search by email, task name, or platform",
        "table_task": "Task",
        "table_account": "Account",
        "table_source": "Source",
        "table_status": "Status",
        "table_action": "Action",
        "pagination_prev": "Previous",
        "pagination_next": "Next",
        "pagination_summary": "Page {page} / {pages}, total {total}",
        "success_accounts_empty_list": "No successful accounts match the current filter.",
        "success_accounts_last_updated": "Last refreshed: {value}",
        "success_accounts_auto_refresh": "Auto refresh",
        "success_accounts_filter_schedule": "Schedule filter",
        "success_accounts_filter_all_schedules": "All task sources",
        "success_accounts_filter_unknown_schedule": "Unknown schedule",
        "success_accounts_filter_hint": "Only successful accounts from \"{value}\" are shown right now.",
        "success_accounts_source_schedule": "Schedule #{id}",
        "success_accounts_source_manual": "Manual task",
        "retry_cpamc_import": "Retry CPAMC import",
        "retry_cpamc_import_done": "Retried CPAMC import for {email}.",
        "oauth_not_supported": "OAuth is not supported for this platform",
        "task_list_title": "Task list",
        "task_list_desc": "The left list only shows tasks that match the selected status filter.",
        "task_filter_status": "Status filter",
        "task_filter_all": "All statuses",
        "task_list_mode": "List type",
        "task_list_mode_task": "Normal tasks",
        "task_list_mode_schedule": "Scheduled tasks",
        "console_title": "Live console",
        "success_accounts_title": "Successful accounts",
        "success_accounts_empty": "No extracted successful accounts yet.",
        "preview_success_accounts": "Preview successful accounts",
        "download_success_accounts": "Download successful accounts",
        "regenerate_oauth_token": "Regenerate OAuth token",
        "batch_regenerate_oauth_token": "Batch relogin",
        "regenerate_oauth_prompt": "Run OAuth login again for {email} and write fresh tokens into the task tokens directory?",
        "regenerate_oauth_result_title": "OAuth token result",
        "regenerate_oauth_success": "Regenerated OAuth token for {email}.",
        "regenerate_oauth_cpamc_success": "Imported to CPAMC as well.",
        "regenerate_oauth_cpamc_skipped": "Skipped CPAMC import because the same content was already imported.",
        "regenerate_oauth_cpamc_failed": "CPAMC import failed: {error}",
        "batch_regenerate_oauth_prompt": "Run OAuth login again for {count} successful accounts and write fresh tokens into the task tokens directory?",
        "batch_regenerate_oauth_result_title": "Batch relogin result",
        "batch_regenerate_oauth_result_summary": "{total} accounts in total, {succeeded} succeeded, {failed} failed.",
        "batch_regenerate_oauth_result_failures": "Failures:\n{value}",
        "batch_regenerate_oauth_empty": "There are no successful accounts available for batch relogin.",
        "cpamc_badge_imported": "Imported to CPAMC",
        "cpamc_badge_failed": "Import failed",
        "extract_history_success_accounts": "Extract historical success accounts",
        "extract_history_success_accounts_done": "Scanned {updated} tasks and extracted successful accounts from {non_empty} tasks.",
        "section_schedules": "Schedules",
        "schedules_create_title": "Add schedule",
        "schedules_create_desc": "Use a visual builder for daily, weekly, or monthly schedules and generate the cron expression automatically.",
        "schedules_saved_title": "Saved schedules",
        "schedules_saved_desc": "Enable, disable, or delete scheduled tasks here.",
        "field_schedule_kind": "Repeat",
        "schedule_kind_interval_minutes": "Every N minutes",
        "schedule_kind_interval_hours": "Every N hours",
        "schedule_kind_daily": "Daily",
        "schedule_kind_weekly": "Weekly",
        "schedule_kind_monthly": "Monthly",
        "field_time_of_day": "Run time",
        "field_interval_minutes": "Minute interval",
        "field_interval_hours": "Hour interval",
        "field_interval_minute_offset": "Minute of the hour",
        "field_schedule_weekdays": "Weekdays",
        "field_schedule_day": "Day of month",
        "schedule_builder_hint": "Configure the schedule visually and let the console generate the cron expression for you.",
        "schedule_generated_cron": "Cron expression",
        "schedule_visual_summary": "Rule: {value}",
        "schedule_human_readable": "Natural language: {value}",
        "schedule_human_interval_minutes": "Run every {value} minute(s)",
        "schedule_human_interval_hours": "Run every {hours} hour(s) at minute {minute}",
        "schedule_human_weekly": "Run every {days} at {time}",
        "schedule_human_monthly": "Run on day {day} of each month at {time}",
        "schedule_human_daily": "Run every day at {time}",
        "schedule_next_run": "Next run {value}",
        "schedule_last_run": "Last run {value}",
        "schedule_time_unknown": "Not triggered yet",
        "schedule_error_name_required": "Enter a schedule name.",
        "schedule_error_quantity_invalid": "Quantity must be between 1 and 100000.",
        "schedule_error_concurrency_invalid": "Concurrency must be between 1 and 64.",
        "schedule_error_time_required": "Choose a valid run time.",
        "schedule_error_interval_minutes": "Minute interval must be between 1 and 59.",
        "schedule_error_interval_hours": "Hour interval must be between 1 and 23.",
        "schedule_error_minute_offset": "Minute offset must be between 0 and 59.",
        "schedule_error_weekdays_required": "Select at least one weekday.",
        "schedule_error_day_invalid": "Day of month must be between 1 and 31.",
        "field_use_default_proxy": "Use default proxy",
        "field_schedule_auto_import_cpamc": "Auto import to CPAMC after completion",
        "save_schedule": "Save schedule",
        "cpamc_title": "Configure CPAMC",
        "cpamc_desc": "Bind CLI Proxy API Management Center here. This is only used for Codex / Grok related JSON imports.",
        "field_cpamc_enabled": "Enable CLI Proxy API Management Center",
        "field_cpamc_auto_import": "Auto import completed tasks to CPAMC",
        "cpamc_auto_import_hint": "Global auto import and schedule auto import share the same completion path, so imports are not duplicated and only run after the target success count is reached.",
        "field_cpamc_base_url": "Domain/IP link",
        "field_cpamc_base_url_placeholder": "For example http://127.0.0.1:8317 or http://127.0.0.1:8317/v0/management",
        "field_cpamc_management_key": "Management key",
        "field_cpamc_management_key_placeholder": "Enter the CPAMC management key",
        "save_cpamc": "Save CPAMC settings",
        "test_cpamc": "Test link",
        "cpamc_status_linked": "Connected",
        "cpamc_status_unlinked": "Not linked",
        "cpamc_status_disabled": "Disabled",
        "cpamc_last_error": "Last error: {value}",
        "cpamc_import_button": "Import to CPAMC",
        "cpamc_force_import_button": "Force import to CPAMC",
        "cpamc_import_disabled": "This task has no importable JSON files",
        "cpamc_import_success": "Imported {count} JSON file(s) to CPAMC.",
        "cpamc_import_partial": "Imported {success}, failed {failed}.",
        "cpamc_import_skipped_only": "Skipped {skipped} JSON file(s) because they were already imported.",
        "cpamc_import_with_skipped": "Imported {success}, skipped {skipped}, failed {failed}.",
        "cpamc_import_result_title": "Import complete",
        "retry_cpamc_import_skipped": "Skipped duplicate CPAMC import for {email} because the same JSON was already imported.",
        "modal_close": "Close",
        "section_api": "API 接口",
        "api_create_title": "Create API key",
        "api_create_desc": "A new key is only shown once. Save it immediately.",
        "api_saved_title": "Existing API keys",
        "api_saved_desc": "Use these keys from external services to create tasks, query status, and download results.",
        "save_api_key": "Generate API key",
        "section_docs": "API Docs",
        "docs_intro_title": "总览",
        "docs_intro_desc": "控制台支持网页操作和外部 API 调用。外部 API 默认使用站点中已配置的默认 GPTMail、默认 YesCaptcha 和默认代理。通过 API 创建的任务会在完成 24 小时后自动清理。",
        "docs_deploy_title": "部署方式",
        "docs_deploy_desc": "推荐优先使用 Docker Compose 部署，默认直接拉取 `maishanhub/mregister:main` 镜像，便于快速上线和保留运行数据；如果只是本地调试，也可以直接用 Python 启动。",
        "docs_local_deploy_title": "本地 Python 启动",
        "docs_compose_deploy_title": "Docker Compose 启动",
        "docs_api_flow_title": "API 调用流程",
        "docs_api_flow_desc": "推荐顺序：先在控制台创建 API Key，再调用创建任务接口，随后轮询查询状态，最后在任务完成后下载压缩包。",
        "docs_endpoints_title": "接口列表",
        "docs_create_params_title": "创建任务参数",
        "docs_create_example_title": "创建任务示例",
        "docs_query_example_title": "查询任务示例",
        "docs_download_example_title": "下载结果示例",
        "docs_response_title": "返回说明",
        "docs_response_desc": "`completed_count` 表示任务当前真实完成数，不按尝试次数计算。只有任务完成并且压缩包生成后，查询接口才会返回 `download_url`。API 创建的任务会在 `auto_delete_at` 指定时间后自动删除。",
        "table_method": "Method",
        "table_path": "Path",
        "table_desc": "Description",
        "table_field": "Field",
        "table_type": "Type",
        "table_required": "Required",
        "endpoint_create_desc": "创建一个新的外部任务",
        "endpoint_query_desc": "查询任务状态、真实完成数量和下载地址",
        "endpoint_download_desc": "下载任务结果压缩包",
        "required_yes": "Yes",
        "required_no": "No",
        "param_platform_desc": "Driver name. Supported values: `browser-automation-local`, `chatgpt-register-v2`, `chatgpt-register-v3`, and `grok-register`",
        "param_quantity_desc": "目标成功数量，系统按真实成功数判断完成，不按尝试次数计算",
        "param_use_proxy_desc": "是否启用默认代理，不传或传 false 表示不使用代理",
        "param_concurrency_desc": "并发数，默认 1",
        "param_name_desc": "自定义任务名，不传则由系统自动生成",
        "docs_flow_1": "1. 在“API 接口”页面创建 API Key。",
        "docs_flow_2": "2. 调用 `POST /api/external/tasks` 创建任务。",
        "docs_flow_3": "3. 轮询 `GET /api/external/tasks/{task_id}` 查询状态和完成数。",
        "docs_flow_4": "4. 任务完成后调用 `GET /api/external/tasks/{task_id}/download` 下载压缩包。",
        "dashboard_running_tasks": "Running tasks",
        "dashboard_completed_tasks": "Completed tasks",
        "dashboard_credential_count": "Credentials",
        "dashboard_proxy_count": "Proxies",
        "empty_tasks": "No tasks yet",
        "empty_credentials": "No credentials yet",
        "empty_proxies": "No proxies yet",
        "empty_filtered_tasks": "No tasks match the current filter",
        "empty_schedules": "No schedules yet",
        "empty_api_keys": "No API keys yet",
        "default_badge": "default",
        "created_at": "Created at {value}",
        "task_duration_label": "Duration {value}",
        "task_duration_value": "{value}",
        "task_duration_running": "Running for {value}",
        "task_duration_pending": "Pending for {value}",
        "task_duration_unknown": "Duration unavailable",
        "task_live_timer_hint": "Current run",
        "task_started_time": "Total start",
        "task_current_run_started_time": "Current run start",
        "task_finished_time": "Finished",
        "task_run_duration": "Duration",
        "task_time_unknown": "--",
        "last_used_at": "Last used {value}",
        "unused": "Not used yet",
        "use_default_gptmail": "Use default mail credential",
        "use_default_yescaptcha": "Use default YesCaptcha",
        "choose_proxy": "Choose a proxy",
        "no_default_gptmail": "No default mail credential",
        "no_default_yescaptcha": "No default YesCaptcha",
        "no_default_proxy": "No default proxy",
        "current_default": "Current default",
        "set_default": "Set default",
        "delete": "Delete",
        "enable": "Enable",
        "disable": "Disable",
        "stop_task": "Stop task",
        "rerun_task": "Rerun",
        "rerun_task_confirm": "Rerun task #{id} from scratch? This creates a new task cloned from the original configuration.",
        "run_schedule_now": "Run now",
        "download_zip": "Download archive",
        "delete_task": "Delete task",
        "save_now": "Created successfully, save it now",
        "created_task_modal_title": "Task Created",
        "created_task_modal_confirm": "Open task detail",
        "created_task_modal_cancel": "Keep creating",
        "status_queued": "Queued",
        "status_running": "Running",
        "status_stopping": "Stopping",
        "status_completed": "Completed",
        "status_partial": "Partially completed",
        "status_failed": "Failed",
        "status_stopped": "Stopped",
        "status_interrupted": "Interrupted",
        "task_detail_empty_title": "No tasks match the current filter",
        "task_detail_empty_desc": "Adjust the status filter on the left, or create a new task first.",
        "console_wait": "Select a task to see live console output.",
        "console_empty": "No console output yet.",
        "task_header_meta": "{platform} | Target {quantity} | Completed {completed} | Status {status}",
        "created_task_confirm": "Task #{id} was created. Open task detail to check progress, or stay here and create another one.",
        "delete_task_confirm": "Delete task #{id}?",
        "delete_credential_confirm": "Delete credential {name}?",
        "delete_proxy_confirm": "Delete proxy {name}?",
        "delete_schedule_confirm": "Delete this schedule?",
        "delete_api_key_confirm": "Delete this API key?",
        "schedule_meta": "{platform} | Daily {time} | Quantity {quantity} | {enabled}",
        "schedule_meta_visual": "{platform} | {summary} | Quantity {quantity} | {enabled}",
        "schedule_proxy_on": "Use default proxy",
        "schedule_proxy_off": "No proxy",
        "schedule_cpamc_auto_import_on": "Auto import to CPAMC after completion",
        "schedule_cpamc_auto_import_off": "Do not auto import to CPAMC",
        "schedule_detail_title": "Scheduled Task Detail",
        "schedule_target_quantity": "Target {value}",
        "schedule_completed_quantity": "Completed {value}",
        "schedule_today_status": "Today {value}",
        "schedule_completed_runs": "Completed {value} scheduled runs",
        "schedule_today_none": "Not triggered today",
        "schedule_tag_suffix": "· Scheduled",
        "schedule_runs_short": "runs done",
        "schedule_today_detail_title": "Today's Run",
        "schedule_latest_task_title": "Latest Run",
        "schedule_console_empty": "No console output for today's run yet",
        "task_center_title": "Task Center",
        "task_center_desc": "Create tasks, follow live execution, manage schedules, and review successful accounts from one workspace.",
        "task_center_header_create": "New Task",
        "task_center_header_schedule": "Schedules",
        "task_center_tab_overview": "Overview",
        "task_center_tab_create": "New Task",
        "task_center_tab_detail": "Tasks & Detail",
        "task_center_tab_schedules": "Schedules",
        "task_center_tab_success_accounts": "Success Accounts",
        "task_center_metric_running": "Running tasks",
        "task_center_metric_finished": "Finished tasks",
        "task_center_metric_attention": "Needs attention",
        "task_center_metric_schedules": "Active schedules",
        "task_center_quick_actions_title": "Quick actions",
        "task_center_quick_actions_desc": "Jump into the most common actions without moving across multiple top-level pages.",
        "task_center_action_create": "Create a task",
        "task_center_action_create_desc": "Choose the driver, quantity, concurrency, and proxy mode, then queue it immediately.",
        "task_center_action_detail": "Open latest task",
        "task_center_action_detail_desc": "Continue with task #{id} and inspect logs, results, and actions.",
        "task_center_action_schedule": "Manage schedules",
        "task_center_action_schedule_desc": "Configure daily recurring jobs from the same workspace.",
        "task_center_action_results": "Review successful accounts",
        "task_center_action_results_desc": "Search, export, and post-process successful accounts in one place.",
        "task_center_health_title": "Resource readiness",
        "task_center_health_desc": "Verify the required resources and post-processing pipeline before launching a new batch.",
        "task_center_health_credentials": "Mail credentials",
        "task_center_health_proxies": "Proxy pool",
        "task_center_health_results": "Tasks with results",
        "task_center_health_cpamc": "CPAMC",
        "task_center_health_ready": "Ready",
        "task_center_health_missing": "Missing",
        "task_center_health_optional": "Optional",
        "task_center_recent_title": "Recent tasks",
        "task_center_recent_desc": "Jump back into the most recent running, completed, or failed task.",
        "api_key_meta": "{prefix}... | Created at {created_at}",
    },
}

DEFAULT_SETTING_KEYS = {
    "default_gptmail_credential_id": None,
    "default_yescaptcha_credential_id": None,
    "default_proxy_id": None,
}

EMAIL_CREDENTIAL_KINDS = {
    "gptmail",
    "duckmail",
    "tempmail_lol",
    "temporam",
    "custom2925",
    "moemail",
    "cloudflare_temp_email",
}

CPAMC_SETTING_KEYS = {
    "cpamc_enabled": "0",
    "cpamc_base_url": "",
    "cpamc_management_key": "",
    "cpamc_linked": "0",
    "cpamc_last_error": "",
}

db_lock = threading.RLock()
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))


# Windows may inherit incorrect registry mappings for JavaScript module files.
# Force the standard web MIME types before StaticFiles starts serving assets.
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("application/javascript", ".mjs")


def now() -> datetime:
    return datetime.now()


def now_iso() -> str:
    return now().strftime("%Y-%m-%d %H:%M:%S")


def date_iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def detect_ui_lang(request: Request) -> str:
    accept_language = (request.headers.get("accept-language") or "").lower()
    for raw_part in accept_language.split(","):
        token = raw_part.split(";")[0].strip()
        if token.startswith("zh"):
            return "zh-CN"
        if token.startswith("en"):
            return "en"
    return "zh-CN"


def get_ui_translations(lang: str) -> dict[str, str]:
    base = UI_TRANSLATIONS["zh-CN"]
    selected = UI_TRANSLATIONS.get(lang, {})
    return {**base, **selected}


def ensure_runtime_dirs() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    TASKS_DIR.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def init_db() -> None:
    ensure_runtime_dirs()
    with db_lock, get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS credentials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,
                api_key TEXT NOT NULL,
                is_exhausted INTEGER NOT NULL DEFAULT 0,
                exhausted_at TEXT,
                exhausted_reason TEXT,
                base_url TEXT,
                prefix TEXT,
                domain TEXT,
                secret TEXT,
                extra_json TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                proxy_url TEXT NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                platform TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                status TEXT NOT NULL,
                email_credential_id INTEGER,
                captcha_credential_id INTEGER,
                concurrency INTEGER NOT NULL DEFAULT 1,
                proxy TEXT,
                task_dir TEXT NOT NULL,
                console_path TEXT NOT NULL,
                archive_path TEXT,
                requested_config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                first_started_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                exit_code INTEGER,
                pid INTEGER,
                last_error TEXT,
                source TEXT NOT NULL DEFAULT 'ui',
                schedule_id INTEGER,
                auto_delete_at TEXT,
                FOREIGN KEY(email_credential_id) REFERENCES credentials(id),
                FOREIGN KEY(captcha_credential_id) REFERENCES credentials(id)
            );

            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                platform TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                concurrency INTEGER NOT NULL DEFAULT 1,
                time_of_day TEXT NOT NULL,
                cron_expression TEXT,
                schedule_kind TEXT NOT NULL DEFAULT 'daily',
                schedule_config_json TEXT NOT NULL DEFAULT '{}',
                use_proxy INTEGER NOT NULL DEFAULT 0,
                auto_import_cpamc INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run_date TEXT,
                last_run_slot TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                key_hash TEXT NOT NULL UNIQUE,
                key_prefix TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_used_at TEXT
            );
            """
        )
        ensure_columns(
            conn,
            "tasks",
            {
                "source": "TEXT NOT NULL DEFAULT 'ui'",
                "schedule_id": "INTEGER",
                "auto_delete_at": "TEXT",
                "first_started_at": "TEXT",
            },
        )
        ensure_columns(
            conn,
            "credentials",
            {
                "is_exhausted": "INTEGER NOT NULL DEFAULT 0",
                "exhausted_at": "TEXT",
                "exhausted_reason": "TEXT",
                "secret": "TEXT",
                "extra_json": "TEXT",
            },
        )
        ensure_columns(
            conn,
            "schedules",
            {
                "auto_import_cpamc": "INTEGER NOT NULL DEFAULT 0",
                "cron_expression": "TEXT",
                "schedule_kind": "TEXT NOT NULL DEFAULT 'daily'",
                "schedule_config_json": "TEXT NOT NULL DEFAULT '{}'",
                "last_run_slot": "TEXT",
            },
        )
        conn.execute(
            """
            UPDATE tasks
            SET first_started_at = started_at
            WHERE first_started_at IS NULL
              AND started_at IS NOT NULL
            """
        )
        conn.commit()


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    with db_lock, get_connection() as conn:
        return conn.execute(query, params).fetchall()


def fetch_one(query: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
    with db_lock, get_connection() as conn:
        return conn.execute(query, params).fetchone()


def execute(query: str, params: tuple[Any, ...] = ()) -> int:
    with db_lock, get_connection() as conn:
        cursor = conn.execute(query, params)
        conn.commit()
        return int(cursor.lastrowid or 0)


def execute_no_return(query: str, params: tuple[Any, ...] = ()) -> None:
    with db_lock, get_connection() as conn:
        conn.execute(query, params)
        conn.commit()


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    if "first_started_at" in item and not item.get("first_started_at") and item.get("started_at"):
        item["first_started_at"] = item["started_at"]
    return item


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_tail(path: Path, limit: int = 30000) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - limit))
        return fh.read().decode("utf-8", errors="replace")


def get_setting(key: str) -> str | None:
    row = fetch_one("SELECT value FROM settings WHERE key = ?", (key,))
    return None if row is None else str(row["value"])


def set_setting(key: str, value: str | None) -> None:
    if value is None:
        execute_no_return("DELETE FROM settings WHERE key = ?", (key,))
        return
    execute_no_return(
        """
        INSERT INTO settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, value, now_iso()),
    )


def get_defaults() -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    for key in DEFAULT_SETTING_KEYS:
        raw = get_setting(key)
        result[key] = int(raw) if raw and raw.isdigit() else None
    return result


def normalize_cpamc_base_url(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"http://{raw}"
    parsed = urlsplit(raw)
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="CPAMC link is invalid")
    path = (parsed.path or "").rstrip("/")
    if not path:
        path = "/v0/management"
    elif not path.endswith("/v0/management"):
        path = f"{path}/v0/management"
    return urlunsplit((parsed.scheme or "http", parsed.netloc, path, "", ""))


def get_cpamc_settings() -> dict[str, Any]:
    enabled = get_setting("cpamc_enabled") == "1"
    base_url = (get_setting("cpamc_base_url") or "").strip()
    management_key = (get_setting("cpamc_management_key") or "").strip()
    linked = get_setting("cpamc_linked") == "1"
    last_error = (get_setting("cpamc_last_error") or "").strip()
    auto_import_enabled = get_setting("cpamc_auto_import_enabled") == "1"
    return {
        "enabled": enabled,
        "base_url": base_url,
        "management_key": management_key,
        "linked": linked,
        "last_error": last_error,
        "auto_import_enabled": auto_import_enabled,
    }


def set_cpamc_settings(settings: dict[str, Any]) -> dict[str, Any]:
    set_setting("cpamc_enabled", "1" if settings.get("enabled") else "0")
    set_setting("cpamc_base_url", str(settings.get("base_url") or "").strip())
    set_setting("cpamc_management_key", str(settings.get("management_key") or "").strip())
    set_setting("cpamc_linked", "1" if settings.get("linked") else "0")
    set_setting("cpamc_last_error", str(settings.get("last_error") or "").strip())
    set_setting("cpamc_auto_import_enabled", "1" if settings.get("auto_import_enabled") else "0")
    return get_cpamc_settings()


def cpamc_headers(management_key: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {management_key}",
        "X-Management-Key": management_key,
    }
    if extra:
        headers.update(extra)
    return headers


def cpamc_request(
    method: str,
    *,
    base_url: str,
    management_key: str,
    path: str,
    **kwargs: Any,
) -> requests.Response:
    target = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    headers = kwargs.pop("headers", {})
    return requests.request(
        method=method,
        url=target,
        headers=cpamc_headers(management_key, headers),
        timeout=20,
        **kwargs,
    )


def parse_cpamc_error(response: requests.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = None
    if isinstance(payload, dict):
        detail = payload.get("message") or payload.get("error") or payload.get("detail")
        if detail:
            return str(detail)
    text = (response.text or "").strip()
    if text:
        return text[:240]
    return f"HTTP {response.status_code}"


def cpamc_import_candidates(task: sqlite3.Row | dict[str, Any], *, validate: bool) -> list[Path]:
    task_dir = Path(task["task_dir"])
    candidate_dirs = [
        task_dir / "output" / "tokens",
        task_dir / "keys",
    ]
    files: list[Path] = []
    for directory in candidate_dirs:
        if not directory.exists():
            continue
        for file_path in sorted(directory.glob("*.json")):
            if not validate:
                files.append(file_path)
                continue
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            token_type = str(payload.get("type") or "").strip().lower()
            if token_type in {"codex", "grok"} or ("access_token" in payload and "refresh_token" in payload):
                files.append(file_path)
    return files


def task_requests_cpamc_auto_import(task: sqlite3.Row | dict[str, Any]) -> bool:
    try:
        requested = json.loads(task["requested_config_json"])
    except Exception:
        requested = {}
    return bool(requested.get("cpamc_auto_import"))


def cpamc_is_ready(cpamc: dict[str, Any]) -> bool:
    return bool(cpamc.get("enabled") and cpamc.get("linked") and cpamc.get("base_url") and cpamc.get("management_key"))


def append_task_console(task: sqlite3.Row | dict[str, Any], message: str) -> None:
    console_path = Path(task["console_path"])
    console_path.parent.mkdir(parents=True, exist_ok=True)
    with console_path.open("a", encoding="utf-8", buffering=1) as log_handle:
        log_handle.write(f"[{now_iso()}] {message}\n")


def regenerate_success_account_oauth_token(task: sqlite3.Row, email: str, password: str) -> dict[str, Any]:
    platform = str(task["platform"])
    if platform not in {"chatgpt-register-v2", "chatgpt-register-v3"}:
        raise HTTPException(status_code=400, detail="OAuth token regeneration is only supported for ChatGPT Register v2/v3 tasks")

    account = find_success_account_record(task, email=email, password=password)
    if account is None:
        raise HTTPException(status_code=404, detail="The specified success account was not found in this task")

    credential_id = task["email_credential_id"]
    if not credential_id:
        raise HTTPException(status_code=400, detail="This task does not have an email credential attached")

    credential = get_credential(int(credential_id))
    proxy = str(task["proxy"] or "").strip()
    output_dir = Path(task["task_dir"]) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    config = build_email_credential_config(credential, proxy=proxy)
    account_provider = str(account.get("provider") or "").strip().lower().replace("-", "_")
    if account_provider:
        config["mail_provider"] = account_provider
    config.update(
        {
            "ak_file": str(output_dir / "ak.txt"),
            "rk_file": str(output_dir / "rk.txt"),
            "token_json_dir": str(output_dir / "tokens"),
            "upload_api_url": "",
            "upload_api_token": "",
            "enable_oauth": True,
            "oauth_required": True,
        }
    )

    append_task_console(task, f"Starting OAuth token regeneration for {email}.")

    try:
        from chatgpt_register_v2.lib.chatgpt_client import ChatGPTClient
        from chatgpt_register_v2.lib.oauth_client import OAuthClient
        from chatgpt_register_v2.lib.skymail_client import init_mail_client
        from chatgpt_register_v2.lib.token_manager import TokenManager

        provider = str(config.get("mail_provider") or "").strip().lower().replace("-", "_")
        mailbox_credential = str(account.get("mailbox_credential") or "").strip()
        if provider in SUCCESS_ACCOUNT_SEEDED_MAILBOX_PROVIDERS and not mailbox_credential:
            raise RuntimeError(
                f"Missing stored mailbox credential for provider={provider}. "
                "This account may have been created before mailbox credentials were persisted."
            )

        mail_client = init_mail_client(config)
        seed_mail_client_for_success_account(mail_client, account)
        token_manager = TokenManager(config)
        chatgpt_client = ChatGPTClient(proxy=proxy or None, verbose=False)
        oauth_client = OAuthClient(config, proxy=proxy or None, verbose=False)
        oauth_client.session = chatgpt_client.session

        tokens = oauth_client.login_and_get_tokens(
            email,
            password,
            chatgpt_client.device_id,
            chatgpt_client.ua,
            chatgpt_client.sec_ch_ua,
            chatgpt_client.impersonate,
            mail_client,
        )
        if not tokens or not tokens.get("access_token"):
            raise RuntimeError("OAuth token regeneration failed")

        token_manager.save_tokens(email, tokens)
        token_json_path = output_dir / "tokens" / f"{email}.json"
        cpamc_result: dict[str, Any] | None = None
        cpamc_settings = get_cpamc_settings()
        if cpamc_is_ready(cpamc_settings):
            try:
                cpamc_result = import_success_account_token_to_cpamc(task, email)
                if cpamc_result.get("skipped"):
                    append_task_console(task, f"Skipped CPAMC import for regenerated OAuth token of {email} because the same content was already imported.")
                else:
                    append_task_console(task, f"Imported regenerated OAuth token to CPAMC for {email}.")
            except Exception as exc:
                cpamc_result = {"imported": False, "error": str(exc), "name": token_json_path.name}
                append_task_console(task, f"Failed to import regenerated OAuth token to CPAMC for {email}: {exc}")
        statuses = load_success_account_statuses(task)
        current_status = dict(statuses.get(email) or {})
        current_status.update(
            {
                "token_json": str(token_json_path),
                "updated_at": now_iso(),
            }
        )
        if cpamc_result is not None:
            current_status["cpamc_imported"] = bool(cpamc_result.get("imported"))
            current_status["cpamc_error"] = str(cpamc_result.get("error") or "")
        statuses[email] = current_status
        save_success_account_statuses(task, statuses)
        update_success_account_status_in_results_file(
            task,
            email=email,
            new_status=SUCCESS_ACCOUNT_OAUTH_SUCCESS_STATUS,
        )
        append_task_console(task, f"OAuth token regeneration succeeded for {email}.")
        return {
            "ok": True,
            "email": email,
            "token_json": str(token_json_path),
            "access_token": tokens.get("access_token") or "",
            "refresh_token": tokens.get("refresh_token") or "",
            "cpamc": cpamc_result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        append_task_console(task, f"OAuth token regeneration failed for {email}: {exc}")
        raise HTTPException(status_code=502, detail=f"OAuth token regeneration failed: {exc}") from exc


def regenerate_success_account_oauth_batch(items: list[SuccessAccountOAuthBatchItem]) -> dict[str, Any]:
    normalized_items: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()
    for item in items:
        email = item.email.strip()
        if not email:
            continue
        dedupe_key = (int(item.task_id), email.lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized_items.append((int(item.task_id), email))

    if not normalized_items:
        raise HTTPException(status_code=400, detail="No success accounts were provided for batch OAuth regeneration")

    task_cache: dict[int, sqlite3.Row] = {}
    account_cache: dict[int, dict[str, tuple[str, str]]] = {}
    results: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0

    for task_id, requested_email in normalized_items:
        try:
            task = task_cache.get(task_id)
            if task is None:
                task = get_task(task_id)
                task_cache[task_id] = task
            task_accounts = account_cache.get(task_id)
            if task_accounts is None:
                task_accounts = {
                    str(record.get("email") or "").strip().lower(): (
                        str(record.get("email") or "").strip(),
                        str(record.get("password") or "").strip(),
                    )
                    for record in load_success_account_records(task)
                    if str(record.get("email") or "").strip() and str(record.get("password") or "").strip()
                }
                account_cache[task_id] = task_accounts
            matched = task_accounts.get(requested_email.lower())
            if not matched:
                raise HTTPException(status_code=404, detail="The specified success account was not found in this task")
            email, password = matched
            result = regenerate_success_account_oauth_token(task, email, password)
            succeeded += 1
            results.append(
                {
                    "ok": True,
                    "task_id": task_id,
                    "email": email,
                    "token_json": str(result.get("token_json") or ""),
                    "cpamc": result.get("cpamc"),
                }
            )
        except HTTPException as exc:
            failed += 1
            results.append(
                {
                    "ok": False,
                    "task_id": task_id,
                    "email": requested_email,
                    "error": str(exc.detail),
                }
            )
        except Exception as exc:
            failed += 1
            results.append(
                {
                    "ok": False,
                    "task_id": task_id,
                    "email": requested_email,
                    "error": str(exc),
                }
            )

    return {
        "ok": failed == 0,
        "total": len(normalized_items),
        "succeeded": succeeded,
        "failed": failed,
        "results": results,
    }


def resolve_success_account_batch_items(payload: SuccessAccountOAuthBatchRequest) -> list[SuccessAccountOAuthBatchItem]:
    if payload.select_all_filtered:
        records = collect_success_account_records(search=payload.search, schedule_id=payload.schedule_id)
        return [
            SuccessAccountOAuthBatchItem(task_id=int(record["task_id"]), email=str(record["email"]))
            for record in records
            if str(record.get("platform") or "") in {"chatgpt-register-v2", "chatgpt-register-v3"}
        ]
    return payload.items


def success_account_items(task: sqlite3.Row | dict[str, Any]) -> list[dict[str, Any]]:
    statuses = load_success_account_statuses(task)
    items: list[dict[str, Any]] = []
    for record in load_success_account_records(task):
        email = str(record.get("email") or "").strip()
        password = str(record.get("password") or "").strip()
        status = statuses.get(email, {})
        items.append(
            {
                "email": email,
                "password": password,
                "status": str(record.get("status") or ""),
                "provider": str(record.get("provider") or ""),
                "mailbox_credential_present": bool(str(record.get("mailbox_credential") or "").strip()),
                "cpamc_imported": bool(status.get("cpamc_imported")),
                "cpamc_error": str(status.get("cpamc_error") or ""),
                "token_json": str(status.get("token_json") or ""),
                "updated_at": str(status.get("updated_at") or ""),
            }
        )
    return items


def collect_success_account_records(*, search: str, schedule_id: int | None = None) -> list[dict[str, Any]]:
    tasks = fetch_all("SELECT * FROM tasks ORDER BY id DESC")
    keyword = search.strip().lower()
    records: list[dict[str, Any]] = []
    for row in tasks:
        task_dict = row_to_dict(row)
        row_schedule_id = task_dict.get("schedule_id")
        if schedule_id is not None and int(row_schedule_id or 0) != int(schedule_id):
            continue
        for account in success_account_items(row):
            haystack = " ".join([
                str(account.get("email") or ""),
                str(task_dict.get("name") or ""),
                str(task_dict.get("platform") or ""),
                str(task_dict.get("id") or ""),
            ]).lower()
            if keyword and keyword not in haystack:
                continue
            records.append(
                {
                    "task_id": int(task_dict["id"]),
                    "task_name": str(task_dict.get("name") or "").strip() or f"#{task_dict['id']}",
                    "platform": str(task_dict.get("platform") or ""),
                    "source": str(task_dict.get("source") or "ui"),
                    "schedule_id": int(row_schedule_id) if row_schedule_id is not None else None,
                    **account,
                }
            )
    return records


def query_success_accounts(*, page: int, page_size: int, search: str, schedule_id: int | None = None) -> dict[str, Any]:
    records = collect_success_account_records(search=search, schedule_id=schedule_id)
    total = len(records)
    page_size = max(1, min(page_size, 100))
    total_pages = max(1, math.ceil(total / page_size)) if total else 1
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": records[start:end],
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
    }


def import_task_files_to_cpamc(
    task: sqlite3.Row | dict[str, Any],
    *,
    cpamc: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    cpamc_settings = cpamc or get_cpamc_settings()
    if not cpamc_settings["enabled"]:
        raise RuntimeError("CPAMC is not enabled")
    if not cpamc_settings["linked"]:
        raise RuntimeError("CPAMC is not linked yet")
    if not cpamc_settings["base_url"] or not cpamc_settings["management_key"]:
        raise RuntimeError("CPAMC configuration is incomplete")

    candidates = cpamc_import_candidates(task, validate=True)
    if not candidates:
        raise RuntimeError("No importable JSON files were found for this task")

    import_statuses = load_task_cpamc_import_statuses(task)
    imported: list[str] = []
    skipped: list[str] = []
    failed: list[dict[str, str]] = []
    for file_path in candidates:
        try:
            payload_bytes = file_path.read_bytes()
        except Exception as exc:
            failed.append({"name": file_path.name, "error": str(exc)})
            continue
        fingerprint = build_cpamc_import_fingerprint(payload_bytes)
        if not force:
            previous = import_statuses.get(file_path.name, {})
            if previous.get("fingerprint") == fingerprint:
                skipped.append(file_path.name)
                continue
        try:
            response = cpamc_request(
                "POST",
                base_url=str(cpamc_settings["base_url"]),
                management_key=str(cpamc_settings["management_key"]),
                path=f"auth-files?name={quote(file_path.name)}",
                data=payload_bytes,
                headers={"Content-Type": "application/json"},
            )
        except requests.RequestException as exc:
            failed.append({"name": file_path.name, "error": str(exc)})
            continue
        if response.ok:
            imported.append(file_path.name)
            import_statuses[file_path.name] = {
                "fingerprint": fingerprint,
                "imported_at": now_iso(),
            }
        else:
            failed.append({"name": file_path.name, "error": parse_cpamc_error(response)})

    if imported:
        save_task_cpamc_import_statuses(task, import_statuses)

    if not imported and not skipped:
        first_error = failed[0]["error"] if failed else "Unknown import error"
        raise RuntimeError(f"CPAMC import failed: {first_error}")
    return {
        "ok": True,
        "imported_count": len(imported),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
    }


def hash_password(password: str, salt_hex: str | None = None) -> str:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str | None) -> bool:
    if not stored or "$" not in stored:
        return False
    salt_hex, expected = stored.split("$", 1)
    actual = hash_password(password, salt_hex).split("$", 1)[1]
    return hmac.compare_digest(actual, expected)


def auth_is_configured() -> bool:
    return bool(get_setting("admin_password_hash"))


def cleanup_expired_sessions() -> None:
    execute_no_return("DELETE FROM sessions WHERE expires_at <= ?", (now_iso(),))


def create_session_token() -> tuple[str, str]:
    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    expires_at = date_iso(now() + timedelta(hours=SESSION_TTL_HOURS))
    execute_no_return(
        "INSERT INTO sessions (token_hash, created_at, expires_at) VALUES (?, ?, ?)",
        (token_hash, now_iso(), expires_at),
    )
    return raw_token, expires_at


def delete_session(raw_token: str | None) -> None:
    if not raw_token:
        return
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    execute_no_return("DELETE FROM sessions WHERE token_hash = ?", (token_hash,))


def is_authenticated_request(request: Request) -> bool:
    cleanup_expired_sessions()
    raw_token = request.cookies.get(SESSION_COOKIE)
    if not raw_token:
        return False
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    row = fetch_one("SELECT id FROM sessions WHERE token_hash = ? AND expires_at > ?", (token_hash, now_iso()))
    return row is not None


def require_authenticated(request: Request) -> None:
    if not auth_is_configured():
        raise HTTPException(status_code=403, detail="Admin password is not configured yet")
    if not is_authenticated_request(request):
        raise HTTPException(status_code=401, detail="Login required")


def generate_api_key_secret() -> tuple[str, str, str]:
    raw = f"rc_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    prefix = raw[:12]
    return raw, key_hash, prefix


def verify_api_key(raw_key: str) -> sqlite3.Row | None:
    key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    row = fetch_one("SELECT * FROM api_keys WHERE key_hash = ? AND is_active = 1", (key_hash,))
    if row is not None:
        execute_no_return("UPDATE api_keys SET last_used_at = ? WHERE id = ?", (now_iso(), int(row["id"])))
    return row


def get_request_api_key(request: Request) -> str | None:
    bearer = request.headers.get("Authorization", "").strip()
    if bearer.lower().startswith("bearer "):
        return bearer[7:].strip()
    header_key = request.headers.get("X-API-Key", "").strip()
    if header_key:
        return header_key
    return request.query_params.get("api_key")


def require_api_key(request: Request) -> sqlite3.Row:
    raw_key = get_request_api_key(request)
    if not raw_key:
        raise HTTPException(status_code=401, detail="API key required")
    row = verify_api_key(raw_key)
    if row is None:
        raise HTTPException(status_code=401, detail="API key is invalid")
    return row


def get_credentials() -> list[dict[str, Any]]:
    return [row_to_dict(row) for row in fetch_all("SELECT * FROM credentials ORDER BY kind, name")]


def get_proxies() -> list[dict[str, Any]]:
    return [row_to_dict(row) for row in fetch_all("SELECT * FROM proxies ORDER BY name")]


def get_schedules() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in fetch_all("SELECT * FROM schedules ORDER BY id DESC"):
        item = row_to_dict(row)
        try:
            item["schedule_config"] = json.loads(item.get("schedule_config_json") or "{}")
        except Exception:
            item["schedule_config"] = {}
        item["cron_expression"] = str(item.get("cron_expression") or "")
        item["schedule_kind"] = str(item.get("schedule_kind") or "daily")
        item["schedule_label"] = describe_schedule(item)
        item["last_run_at"] = get_schedule_last_run_at(item)
        item["next_run_at"] = compute_next_run_at(item)
        items.append(item)
    return items


def get_api_keys() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in fetch_all("SELECT * FROM api_keys ORDER BY id DESC"):
        item = row_to_dict(row)
        item.pop("key_hash", None)
        items.append(item)
    return items


def get_credential(credential_id: int) -> sqlite3.Row:
    row = fetch_one("SELECT * FROM credentials WHERE id = ?", (credential_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Credential not found")
    return row


def is_email_credential_kind(kind: str | None) -> bool:
    return str(kind or "").strip().lower() in EMAIL_CREDENTIAL_KINDS


def credential_is_exhausted(credential: sqlite3.Row | dict[str, Any]) -> bool:
    return bool(int(credential["is_exhausted"] or 0))


def get_available_email_credential(*, exclude_ids: set[int] | None = None) -> sqlite3.Row | None:
    params: list[Any] = []
    placeholders = ", ".join("?" for _ in sorted(EMAIL_CREDENTIAL_KINDS))
    query = (
        f"SELECT * FROM credentials WHERE kind IN ({placeholders}) "
        "AND (kind != 'gptmail' OR COALESCE(is_exhausted, 0) = 0)"
    )
    params.extend(sorted(EMAIL_CREDENTIAL_KINDS))
    if exclude_ids:
        excluded = sorted(exclude_ids)
        query += f" AND id NOT IN ({', '.join('?' for _ in excluded)})"
        params.extend(excluded)
    query += " ORDER BY id ASC LIMIT 1"
    return fetch_one(query, tuple(params))


def get_available_gptmail_credential(*, exclude_ids: set[int] | None = None) -> sqlite3.Row | None:
    params: list[Any] = []
    query = "SELECT * FROM credentials WHERE kind = 'gptmail' AND COALESCE(is_exhausted, 0) = 0"
    if exclude_ids:
        placeholders = ", ".join("?" for _ in exclude_ids)
        query += f" AND id NOT IN ({placeholders})"
        params.extend(sorted(exclude_ids))
    query += " ORDER BY id ASC LIMIT 1"
    return fetch_one(query, tuple(params))


def mark_credential_exhausted(credential: sqlite3.Row | dict[str, Any], reason: str) -> None:
    timestamp = now_iso()
    execute_no_return(
        """
        UPDATE credentials
        SET is_exhausted = 1,
            exhausted_at = ?,
            exhausted_reason = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (timestamp, reason, timestamp, int(credential["id"])),
    )


def get_proxy(proxy_id: int) -> sqlite3.Row:
    row = fetch_one("SELECT * FROM proxies WHERE id = ?", (proxy_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Proxy not found")
    return row


def get_schedule(schedule_id: int) -> sqlite3.Row:
    row = fetch_one("SELECT * FROM schedules WHERE id = ?", (schedule_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return row


def normalize_daily_schedule(time_of_day: str | None) -> tuple[str, dict[str, Any], str]:
    value = str(time_of_day or "").strip()
    if not value or len(value) != 5 or value[2] != ":":
        raise HTTPException(status_code=400, detail="Daily schedules require a valid HH:MM time")
    hour, minute = value.split(":", 1)
    if not (hour.isdigit() and minute.isdigit()):
        raise HTTPException(status_code=400, detail="Daily schedules require a valid HH:MM time")
    hour_value = int(hour)
    minute_value = int(minute)
    if hour_value < 0 or hour_value > 23 or minute_value < 0 or minute_value > 59:
        raise HTTPException(status_code=400, detail="Daily schedules require a valid HH:MM time")
    normalized = f"{hour_value:02d}:{minute_value:02d}"
    cron_expression = f"{minute_value} {hour_value} * * *"
    return normalized, {"time_of_day": normalized}, cron_expression


def normalize_visual_schedule(schedule_kind: str, schedule_config: dict[str, Any] | None, time_of_day: str | None) -> tuple[str, dict[str, Any], str]:
    kind = str(schedule_kind or "daily").strip().lower()
    config = dict(schedule_config or {})

    if kind == "interval-minutes":
        interval = int(config.get("interval_minutes", 5))
        if interval < 1 or interval > 59:
            raise HTTPException(status_code=400, detail="Minute interval must be between 1 and 59")
        normalized_config = {"interval_minutes": interval}
        return "--:--", normalized_config, f"*/{interval} * * * *"

    if kind == "interval-hours":
        interval = int(config.get("interval_hours", 1))
        minute = int(config.get("minute", 0))
        if interval < 1 or interval > 23:
            raise HTTPException(status_code=400, detail="Hour interval must be between 1 and 23")
        if minute < 0 or minute > 59:
            raise HTTPException(status_code=400, detail="Schedule time is invalid")
        normalized_config = {"interval_hours": interval, "minute": minute}
        return f"--:{minute:02d}", normalized_config, f"{minute} */{interval} * * *"

    if kind == "daily":
        return normalize_daily_schedule(time_of_day or (schedule_config or {}).get("time_of_day"))

    hour = int(config.get("hour", 0))
    minute = int(config.get("minute", 0))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise HTTPException(status_code=400, detail="Schedule time is invalid")
    normalized_time = f"{hour:02d}:{minute:02d}"

    if kind == "weekly":
        weekdays = config.get("weekdays") or []
        if not isinstance(weekdays, list) or not weekdays:
            raise HTTPException(status_code=400, detail="Weekly schedules require at least one weekday")
        weekday_values = sorted({int(value) for value in weekdays if str(value).isdigit() and 0 <= int(value) <= 6})
        if not weekday_values:
            raise HTTPException(status_code=400, detail="Weekly schedules require at least one weekday")
        normalized_config = {"hour": hour, "minute": minute, "weekdays": weekday_values}
        return normalized_time, normalized_config, f"{minute} {hour} * * {','.join(str(value) for value in weekday_values)}"

    if kind == "monthly":
        day = int(config.get("day", 1))
        if day < 1 or day > 31:
            raise HTTPException(status_code=400, detail="Monthly schedules require a valid day of month")
        normalized_config = {"hour": hour, "minute": minute, "day": day}
        return normalized_time, normalized_config, f"{minute} {hour} {day} * *"

    raise HTTPException(status_code=400, detail="Unsupported schedule type")


def cron_matches_now(cron_expression: str, moment: datetime) -> bool:
    parts = str(cron_expression or "").split()
    if len(parts) != 5:
        return False
    minute, hour, day, month, weekday = parts
    cron_weekday = (moment.weekday() + 1) % 7
    values = [moment.minute, moment.hour, moment.day, moment.month, cron_weekday]
    ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
    for field, value, limits in zip(parts, values, ranges):
        if not cron_field_matches(field, value, limits):
            return False
    if day != '*' and month != '*' and moment.day > monthrange(moment.year, moment.month)[1]:
        return False
    return True


def cron_field_matches(field: str, value: int, limits: tuple[int, int]) -> bool:
    field = field.strip()
    if field == '*':
        return True
    for token in field.split(','):
        token = token.strip()
        if not token:
            continue
        if '/' in token:
            base, step_text = token.split('/', 1)
            if not step_text.isdigit() or int(step_text) <= 0:
                return False
            step = int(step_text)
            if base == '*':
                start, end = limits
            elif '-' in base:
                start_text, end_text = base.split('-', 1)
                if not (start_text.isdigit() and end_text.isdigit()):
                    return False
                start, end = int(start_text), int(end_text)
            else:
                if not base.isdigit():
                    return False
                start = int(base)
                end = limits[1]
            if start <= value <= end and (value - start) % step == 0:
                return True
            continue
        if '-' in token:
            start_text, end_text = token.split('-', 1)
            if start_text.isdigit() and end_text.isdigit() and int(start_text) <= value <= int(end_text):
                return True
            continue
        if token.isdigit() and int(token) == value:
            return True
    return False


def schedule_slot_key(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M")


def describe_schedule(schedule: dict[str, Any]) -> str:
    kind = str(schedule.get("schedule_kind") or "daily")
    config = schedule.get("schedule_config") or {}
    time_of_day = str(schedule.get("time_of_day") or config.get("time_of_day") or "")
    if kind == "interval-minutes":
        return f"Every {config.get('interval_minutes', 5)} min"
    if kind == "interval-hours":
        return f"Every {config.get('interval_hours', 1)} hour(s) at minute {int(config.get('minute', 0)):02d}"
    if kind == "weekly":
        weekday_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        values = [weekday_names[int(item)] for item in config.get("weekdays", []) if 0 <= int(item) <= 6]
        return f"Weekly · {', '.join(values)} · {time_of_day}" if values else f"Weekly · {time_of_day}"
    if kind == "monthly":
        return f"Monthly · Day {config.get('day', 1)} · {time_of_day}"
    return f"Daily · {time_of_day}"


def compute_next_run_at(schedule: dict[str, Any], *, after: datetime | None = None) -> str:
    cron_expression = str(schedule.get("cron_expression") or "").strip()
    if not cron_expression:
        return ""
    cursor = (after or now()).replace(second=0, microsecond=0)
    for _ in range(0, 60 * 24 * 370):
        cursor += timedelta(minutes=1)
        if cron_matches_now(cron_expression, cursor):
            return cursor.strftime("%Y-%m-%d %H:%M:%S")
    return ""


def get_schedule_last_run_at(schedule: dict[str, Any]) -> str:
    schedule_id = int(schedule.get("id") or 0)
    if not schedule_id:
        return ""
    row = fetch_one("SELECT created_at FROM tasks WHERE schedule_id = ? ORDER BY id DESC LIMIT 1", (schedule_id,))
    return str(row["created_at"] or "") if row else ""


def get_task(task_id: int) -> sqlite3.Row:
    row = fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return row


def resolve_required_credential(kind: str, credential_id: int | None) -> sqlite3.Row:
    defaults = get_defaults()
    selected_id = credential_id
    if selected_id is None:
        selected_id = defaults["default_gptmail_credential_id"] if kind == "gptmail" else defaults["default_yescaptcha_credential_id"]
    if selected_id is None:
        if kind == "gptmail":
            fallback = get_available_email_credential()
            if fallback is not None:
                return fallback
        raise HTTPException(status_code=400, detail=f"No default {kind} credential is configured")
    credential = get_credential(int(selected_id))
    if kind == "gptmail":
        if not is_email_credential_kind(str(credential["kind"] or "")):
            raise HTTPException(status_code=400, detail=f"Credential {selected_id} is not an email credential")
    elif credential["kind"] != kind:
        raise HTTPException(status_code=400, detail=f"Credential {selected_id} is not of type {kind}")
    if kind == "gptmail" and str(credential["kind"] or "") == "gptmail" and credential_is_exhausted(credential):
        fallback = get_available_email_credential(exclude_ids={int(credential["id"])})
        if fallback is not None:
            return fallback
        raise HTTPException(status_code=400, detail="No available email credential remains")
    return credential


def resolve_proxy_value(proxy_mode: str, proxy_id: int | None) -> str | None:
    mode = proxy_mode or "none"
    defaults = get_defaults()
    if mode == "none":
        return None
    if mode == "default":
        selected = defaults["default_proxy_id"]
        if selected is None:
            raise HTTPException(status_code=400, detail="No default proxy is configured")
        return str(get_proxy(int(selected))["proxy_url"])
    if mode == "custom":
        if proxy_id is None:
            raise HTTPException(status_code=400, detail="A proxy must be selected")
        return str(get_proxy(proxy_id)["proxy_url"])
    raise HTTPException(status_code=400, detail="Unsupported proxy mode")


def task_paths(task: sqlite3.Row | dict[str, Any]) -> dict[str, Path]:
    task_dir = Path(task["task_dir"])
    if task["platform"] == "browser-automation-local":
        results_file = task_dir / "output" / "results.txt"
    elif task["platform"] == "openai-register":
        results_file = task_dir / "output" / "tokens" / "accounts.txt"
    elif task["platform"] == "chatgpt-register-v2":
        results_file = task_dir / "output" / "registered_accounts.txt"
    elif task["platform"] == "chatgpt-register-v3":
        results_file = task_dir / "output" / "registered_accounts.txt"
    else:
        results_file = task_dir / "keys" / "accounts.txt"
    archive_path = Path(task["archive_path"]) if task["archive_path"] else task_dir / "task_result.zip"
    return {
        "task_dir": task_dir,
        "console_path": Path(task["console_path"]),
        "results_file": results_file,
        "success_accounts_file": task_dir / "output" / "success_accounts.txt",
        "success_accounts_json_file": task_dir / "output" / "success_accounts.json",
        "success_accounts_status_file": task_dir / "output" / "success_accounts_status.json",
        "cpamc_import_status_file": task_dir / "output" / "cpamc_import_status.json",
        "archive_path": archive_path,
    }


SUCCESS_ACCOUNT_REGISTERED_STATUS = "已注册"
SUCCESS_ACCOUNT_OAUTH_SUCCESS_STATUS = "已注册/OAuth成功"
SUCCESS_ACCOUNT_OAUTH_FAILED_STATUS = "已注册/OAuth失败"
SUCCESS_ACCOUNT_PIPE_PLATFORMS = {"chatgpt-register-v2", "chatgpt-register-v3"}
SUCCESS_ACCOUNT_SEEDED_MAILBOX_PROVIDERS = {"duckmail", "tempmail_lol", "cloudflare_temp_email"}


def success_account_timestamp() -> str:
    return now().strftime("%Y%m%d_%H%M%S")


def task_success_account_provider(task: sqlite3.Row | dict[str, Any]) -> str:
    credential_id = task.get("email_credential_id") if isinstance(task, dict) else task["email_credential_id"]
    if not credential_id:
        return ""
    try:
        credential = get_credential(int(credential_id))
    except Exception:
        return ""
    return str(credential["kind"] or "").strip().lower().replace("-", "_")


def _oauth_suffix_to_status(extra_parts: list[str]) -> str:
    normalized = [part.strip().lower() for part in extra_parts if str(part).strip()]
    if any(part == "oauth=ok" for part in normalized):
        return SUCCESS_ACCOUNT_OAUTH_SUCCESS_STATUS
    if any(part == "oauth=failed" for part in normalized):
        return SUCCESS_ACCOUNT_OAUTH_FAILED_STATUS
    return SUCCESS_ACCOUNT_REGISTERED_STATUS


def parse_success_account_record(task: sqlite3.Row | dict[str, Any], line: str) -> dict[str, str] | None:
    value = line.strip()
    if not value:
        return None
    platform = str(task["platform"])
    provider = task_success_account_provider(task)

    if "|" in value:
        parts = [part.strip() for part in value.split("|")]
        if len(parts) >= 2 and "@" in parts[0] and parts[1]:
            return {
                "email": parts[0],
                "password": parts[1],
                "timestamp": parts[2] if len(parts) > 2 else "",
                "status": parts[3] if len(parts) > 3 else SUCCESS_ACCOUNT_REGISTERED_STATUS,
                "mailbox_credential": parts[4] if len(parts) > 4 else "",
                "provider": (parts[5] if len(parts) > 5 else provider).strip().lower().replace("-", "_"),
            }

    if platform in {"chatgpt-register-v2", "chatgpt-register-v3", "browser-automation-local"}:
        parts = [part.strip() for part in value.split("----")]
        if len(parts) >= 2 and "@" in parts[0] and parts[1]:
            return {
                "email": parts[0],
                "password": parts[1],
                "timestamp": "",
                "status": _oauth_suffix_to_status(parts[2:]),
                "mailbox_credential": "",
                "provider": provider,
            }
        return None

    if platform == "grok-register":
        parts = [part.strip() for part in value.split(":")]
        if len(parts) >= 2 and "@" in parts[0] and parts[1]:
            return {
                "email": parts[0],
                "password": parts[1],
                "timestamp": "",
                "status": SUCCESS_ACCOUNT_REGISTERED_STATUS,
                "mailbox_credential": "",
                "provider": provider,
            }
        return None

    if platform == "openai-register":
        parts = [part.strip() for part in (value.split("----") if "----" in value else value.split(":"))]
        if len(parts) >= 2 and "@" in parts[0] and parts[1]:
            return {
                "email": parts[0],
                "password": parts[1],
                "timestamp": "",
                "status": SUCCESS_ACCOUNT_REGISTERED_STATUS,
                "mailbox_credential": "",
                "provider": provider,
            }
        return None

    return None


def format_success_account_record(task: sqlite3.Row | dict[str, Any], record: dict[str, str], *, force_pipe: bool = False) -> str:
    email = str(record.get("email") or "").strip()
    password = str(record.get("password") or "").strip()
    if not email or not password:
        return ""
    use_pipe = force_pipe or str(task["platform"]) in SUCCESS_ACCOUNT_PIPE_PLATFORMS
    if use_pipe:
        return "|".join(
            [
                email,
                password,
                str(record.get("timestamp") or "").strip(),
                str(record.get("status") or SUCCESS_ACCOUNT_REGISTERED_STATUS).strip(),
                str(record.get("mailbox_credential") or "").strip(),
                str(record.get("provider") or task_success_account_provider(task)).strip().lower().replace("-", "_"),
            ]
        )
    return f"{email}----{password}"


def _normalize_success_account_record(task: sqlite3.Row | dict[str, Any], record: dict[str, Any]) -> dict[str, str]:
    return {
        "email": str(record.get("email") or "").strip(),
        "password": str(record.get("password") or "").strip(),
        "timestamp": str(record.get("timestamp") or "").strip(),
        "status": str(record.get("status") or SUCCESS_ACCOUNT_REGISTERED_STATUS).strip(),
        "mailbox_credential": str(record.get("mailbox_credential") or "").strip(),
        "provider": str(record.get("provider") or task_success_account_provider(task)).strip().lower().replace("-", "_"),
    }


def _save_success_account_exports(task: sqlite3.Row | dict[str, Any], records: list[dict[str, str]]) -> Path:
    paths = task_paths(task)
    output_file = paths["success_accounts_file"]
    json_file = paths["success_accounts_json_file"]
    output_file.parent.mkdir(parents=True, exist_ok=True)

    normalized_records = [
        normalized
        for record in records
        if (normalized := _normalize_success_account_record(task, record)).get("email")
        and normalized.get("password")
    ]
    rendered_lines = [format_success_account_record(task, record) for record in normalized_records]

    output_file.write_text(("\n".join(rendered_lines) + "\n") if rendered_lines else "", encoding="utf-8")
    json_file.write_text(json.dumps(normalized_records, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_file


def _load_persisted_success_account_records(task: sqlite3.Row | dict[str, Any]) -> list[dict[str, str]]:
    json_file = task_paths(task)["success_accounts_json_file"]
    if not json_file.exists():
        return []
    try:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []

    records: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        record = _normalize_success_account_record(task, item)
        if not record["email"] or not record["password"]:
            continue
        key = f"{record['email'].lower()}----{record['password']}"
        if key in seen:
            continue
        seen.add(key)
        records.append(record)
    return records


def extract_success_accounts(task: sqlite3.Row | dict[str, Any]) -> Path | None:
    paths = task_paths(task)
    results_file = paths["results_file"]
    if not results_file.exists():
        persisted_records = _load_persisted_success_account_records(task)
        return _save_success_account_exports(task, persisted_records)
    extracted: list[dict[str, str]] = []
    seen: set[str] = set()
    with results_file.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            record = parse_success_account_record(task, raw_line)
            if not record:
                continue
            key = f"{record['email'].lower()}----{record['password']}"
            if key in seen:
                continue
            seen.add(key)
            extracted.append(_normalize_success_account_record(task, record))
    return _save_success_account_exports(task, extracted)


def load_success_account_records(task: sqlite3.Row | dict[str, Any]) -> list[dict[str, str]]:
    extract_success_accounts(task)
    persisted_records = _load_persisted_success_account_records(task)
    if persisted_records:
        return persisted_records
    path = task_paths(task)["success_accounts_file"]
    if not path.exists():
        return []
    records: list[dict[str, str]] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            record = parse_success_account_record(task, raw_line)
            if not record:
                continue
            key = f"{record['email'].lower()}----{record['password']}"
            if key in seen:
                continue
            seen.add(key)
            records.append(record)
    return records


def load_success_account_mailboxes(task: sqlite3.Row | dict[str, Any]) -> list[tuple[str, str, str]]:
    return [
        (
            str(record.get("email") or "").strip(),
            str(record.get("password") or "").strip(),
            str(record.get("mailbox_credential") or "").strip(),
        )
        for record in load_success_account_records(task)
        if str(record.get("email") or "").strip() and str(record.get("password") or "").strip()
    ]


def load_success_accounts(task: sqlite3.Row | dict[str, Any]) -> list[tuple[str, str]]:
    return [
        (str(record.get("email") or "").strip(), str(record.get("password") or "").strip())
        for record in load_success_account_records(task)
        if str(record.get("email") or "").strip() and str(record.get("password") or "").strip()
    ]


def find_success_account_record(
    task: sqlite3.Row | dict[str, Any],
    *,
    email: str,
    password: str | None = None,
) -> dict[str, str] | None:
    normalized_email = email.strip().lower()
    normalized_password = (password or "").strip()
    for record in load_success_account_records(task):
        if str(record.get("email") or "").strip().lower() != normalized_email:
            continue
        if normalized_password and str(record.get("password") or "").strip() != normalized_password:
            continue
        return record
    return None


def seed_mail_client_for_success_account(mail_client: Any, record: dict[str, str]) -> None:
    provider = str(record.get("provider") or "").strip().lower().replace("-", "_")
    email = str(record.get("email") or "").strip()
    mailbox_credential = str(record.get("mailbox_credential") or "").strip()
    if not email or not provider or not mailbox_credential:
        return
    if provider == "duckmail" and hasattr(mail_client, "_accounts"):
        existing = dict(getattr(mail_client, "_accounts", {}).get(email) or {})
        existing["password"] = mailbox_credential
        getattr(mail_client, "_accounts")[email] = existing
    elif provider == "tempmail_lol" and hasattr(mail_client, "_tokens"):
        getattr(mail_client, "_tokens")[email] = mailbox_credential
    elif provider == "cloudflare_temp_email" and hasattr(mail_client, "_jwt_by_email"):
        getattr(mail_client, "_jwt_by_email")[email] = mailbox_credential


def update_success_account_status_in_results_file(
    task: sqlite3.Row | dict[str, Any],
    *,
    email: str,
    new_status: str,
) -> None:
    results_file = task_paths(task)["results_file"]
    if not results_file.exists():
        return
    lines = results_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    updated_lines: list[str] = []
    found = False
    for raw_line in lines:
        record = parse_success_account_record(task, raw_line)
        if not record or str(record.get("email") or "").strip().lower() != email.strip().lower():
            updated_lines.append(raw_line)
            continue
        record["timestamp"] = success_account_timestamp()
        record["status"] = new_status
        if not str(record.get("provider") or "").strip():
            record["provider"] = task_success_account_provider(task)
        updated_lines.append(format_success_account_record(task, record, force_pipe=True))
        found = True
    if not found:
        return
    results_file.write_text("".join(f"{line}\n" for line in updated_lines), encoding="utf-8")
    extract_success_accounts(task)


def load_success_account_statuses(task: sqlite3.Row | dict[str, Any]) -> dict[str, dict[str, Any]]:
    path = task_paths(task)["success_accounts_status_file"]
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(key, str) and isinstance(value, dict):
            result[key] = value
    return result


def save_success_account_statuses(task: sqlite3.Row | dict[str, Any], statuses: dict[str, dict[str, Any]]) -> None:
    path = task_paths(task)["success_accounts_status_file"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(statuses, ensure_ascii=False, indent=2), encoding="utf-8")


def load_task_cpamc_import_statuses(task: sqlite3.Row | dict[str, Any]) -> dict[str, dict[str, Any]]:
    path = task_paths(task)["cpamc_import_status_file"]
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(key, str) and isinstance(value, dict):
            result[key] = value
    return result


def save_task_cpamc_import_statuses(task: sqlite3.Row | dict[str, Any], statuses: dict[str, dict[str, Any]]) -> None:
    path = task_paths(task)["cpamc_import_status_file"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(statuses, ensure_ascii=False, indent=2), encoding="utf-8")


def build_cpamc_import_fingerprint(payload_bytes: bytes) -> str:
    return hashlib.sha256(payload_bytes).hexdigest()


def import_success_account_token_to_cpamc(task: sqlite3.Row | dict[str, Any], email: str) -> dict[str, Any]:
    cpamc_settings = get_cpamc_settings()
    if not cpamc_is_ready(cpamc_settings):
        raise RuntimeError("CPAMC is not enabled or linked")
    token_json_path = Path(task["task_dir"]) / "output" / "tokens" / f"{email}.json"
    if not token_json_path.exists():
        raise RuntimeError("Token JSON file not found for this account")
    payload_bytes = token_json_path.read_bytes()
    fingerprint = build_cpamc_import_fingerprint(payload_bytes)
    import_statuses = load_task_cpamc_import_statuses(task)
    previous = import_statuses.get(token_json_path.name, {})
    if previous.get("fingerprint") == fingerprint:
        return {
            "imported": True,
            "skipped": True,
            "name": token_json_path.name,
            "token_json": str(token_json_path),
        }
    response = cpamc_request(
        "POST",
        base_url=str(cpamc_settings["base_url"]),
        management_key=str(cpamc_settings["management_key"]),
        path=f"auth-files?name={quote(token_json_path.name)}",
        data=payload_bytes,
        headers={"Content-Type": "application/json"},
    )
    if not response.ok:
        raise RuntimeError(parse_cpamc_error(response))
    import_statuses[token_json_path.name] = {
        "fingerprint": fingerprint,
        "imported_at": now_iso(),
    }
    save_task_cpamc_import_statuses(task, import_statuses)
    return {
        "imported": True,
        "skipped": False,
        "name": token_json_path.name,
        "token_json": str(token_json_path),
    }


def backfill_all_success_accounts() -> dict[str, int]:
    rows = fetch_all("SELECT * FROM tasks ORDER BY id ASC")
    updated = 0
    non_empty = 0
    for row in rows:
        path = extract_success_accounts(row)
        updated += 1
        if path and path.exists():
            try:
                with path.open("r", encoding="utf-8", errors="ignore") as handle:
                    if any(line.strip() for line in handle):
                        non_empty += 1
            except Exception:
                pass
    return {"total": len(rows), "updated": updated, "non_empty": non_empty}


def count_result_lines(task: sqlite3.Row | dict[str, Any]) -> int:
    results_file = task_paths(task)["results_file"]
    if not results_file.exists():
        return 0
    with results_file.open("r", encoding="utf-8", errors="ignore") as fh:
        return sum(1 for line in fh if line.strip())


def create_archive(task: sqlite3.Row | dict[str, Any]) -> Path:
    paths = task_paths(task)
    extract_success_accounts(task)
    archive_path = paths["archive_path"]
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in paths["task_dir"].rglob("*"):
            if file_path.is_dir() or file_path == archive_path:
                continue
            zf.write(file_path, file_path.relative_to(paths["task_dir"]))
    execute_no_return("UPDATE tasks SET archive_path = ? WHERE id = ?", (str(archive_path), int(task["id"])))
    return archive_path


def serialize_task(row: sqlite3.Row) -> dict[str, Any]:
    item = row_to_dict(row)
    success_accounts_file = extract_success_accounts(row)
    success_accounts = success_account_items(row)
    item["results_count"] = count_result_lines(row)
    item["console_tail"] = read_tail(Path(item["console_path"]))
    item["success_accounts_count"] = len(success_accounts)
    item["success_accounts_preview"] = ""
    item["success_accounts"] = success_accounts
    if success_accounts_file and success_accounts_file.exists():
        item["success_accounts_preview"] = read_tail(success_accounts_file, limit=12000)
    item["cpamc_importable_count"] = len(cpamc_import_candidates(row, validate=False))
    try:
        item["requested_config"] = json.loads(item["requested_config_json"])
    except Exception:
        item["requested_config"] = {}
    return item


def get_tasks() -> list[dict[str, Any]]:
    return [serialize_task(row) for row in fetch_all("SELECT * FROM tasks ORDER BY id DESC")]


def dashboard_summary() -> dict[str, Any]:
    tasks = get_tasks()
    credentials = get_credentials()
    proxies = get_proxies()
    schedules = get_schedules()
    return {
        "running_tasks": sum(1 for task in tasks if task["status"] in {"queued", "running", "stopping"}),
        "completed_tasks": sum(1 for task in tasks if task["status"] == "completed"),
        "credential_count": len(credentials),
        "proxy_count": len(proxies),
        "schedule_count": len(schedules),
        "recent_tasks": tasks[:5],
    }


class CredentialCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    kind: str
    api_key: str | None = None
    base_url: str | None = None
    prefix: str | None = None
    domain: str | None = None
    secret: str | None = None
    extra_json: str | None = None
    notes: str | None = None


class ProxyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    proxy_url: str = Field(min_length=1, max_length=300)
    notes: str | None = None


class ScheduleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    platform: str
    quantity: int = Field(ge=1, le=100000)
    concurrency: int = Field(default=1, ge=1, le=64)
    schedule_kind: str = Field(default="daily")
    time_of_day: str | None = None
    cron_expression: str | None = None
    schedule_config: dict[str, Any] | None = None
    use_proxy: bool = False
    auto_import_cpamc: bool = False
    enabled: bool = True


class TaskCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    platform: str
    quantity: int = Field(ge=1, le=100000)
    email_credential_id: int | None = None
    captcha_credential_id: int | None = None
    concurrency: int = Field(default=1, ge=1, le=64)
    proxy_mode: str = "none"
    proxy_id: int | None = None
    platform_options: dict[str, Any] | None = None


class ExternalTaskCreate(BaseModel):
    platform: str
    quantity: int = Field(ge=1, le=100000)
    use_proxy: bool = False
    concurrency: int | None = Field(default=None, ge=1, le=64)
    name: str | None = None
    platform_options: dict[str, Any] | None = None


class PasswordPayload(BaseModel):
    password: str = Field(min_length=8, max_length=256)


class DefaultSettingsPayload(BaseModel):
    default_gptmail_credential_id: int | None = None
    default_yescaptcha_credential_id: int | None = None
    default_proxy_id: int | None = None


class CpamcSettingsPayload(BaseModel):
    enabled: bool = False
    base_url: str | None = None
    management_key: str | None = None
    auto_import_enabled: bool = False


class ApiKeyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class SuccessAccountOAuthRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)


class SuccessAccountOAuthBatchItem(BaseModel):
    task_id: int = Field(ge=1)
    email: str = Field(min_length=3, max_length=320)


class SuccessAccountOAuthBatchRequest(BaseModel):
    items: list[SuccessAccountOAuthBatchItem] = Field(default_factory=list, max_length=5000)
    select_all_filtered: bool = False
    search: str = ""
    schedule_id: int | None = None


@dataclass
class TaskResolvedConfig:
    platform: str
    quantity: int
    concurrency: int
    email_credential_id: int | None
    captcha_credential_id: int | None
    proxy_value: str | None
    proxy_mode: str
    source: str
    schedule_id: int | None
    cpamc_auto_import: bool
    auto_delete_at: str | None
    requested_config: dict[str, Any]


def clone_task_config_from_requested(*, requested: dict[str, Any], source: str, name_suffix: str = "") -> tuple[str, TaskResolvedConfig]:
    platform = str(requested.get("platform") or "").strip()
    if not platform:
        raise HTTPException(status_code=400, detail="Original task configuration is missing platform")

    validate_platform(platform)
    base_name = str(requested.get("name") or platform).strip() or platform
    cloned_name = f"{base_name}{name_suffix}" if name_suffix else base_name
    quantity = max(1, int(requested.get("quantity") or 1))
    concurrency = max(1, int(requested.get("concurrency") or 1))

    email_credential_id = requested.get("email_credential_id")
    if email_credential_id is not None:
        credential = get_credential(int(email_credential_id))
        if not is_email_credential_kind(str(credential["kind"] or "")):
            raise HTTPException(status_code=400, detail="Original email credential is no longer valid")
        email_credential_id = int(email_credential_id)

    captcha_credential_id = requested.get("captcha_credential_id")
    if captcha_credential_id is not None:
        credential = get_credential(int(captcha_credential_id))
        if credential["kind"] != "yescaptcha":
            raise HTTPException(status_code=400, detail="Original YesCaptcha credential is no longer valid")
        captcha_credential_id = int(captcha_credential_id)

    proxy_mode = str(requested.get("proxy_mode") or "none")
    proxy_value = requested.get("proxy_value")
    auto_delete_at = requested.get("auto_delete_at")
    platform_options = requested.get("platform_options")

    if platform == "browser-automation-local":
        platform_options = resolve_browser_automation_options(platform_options)

    cloned_requested = {
        "name": cloned_name,
        "platform": platform,
        "quantity": quantity,
        "concurrency": concurrency,
        "source": source,
        "schedule_id": None,
        "cpamc_auto_import": False,
        "proxy_mode": proxy_mode,
        "proxy_id": None,
        "proxy_value": proxy_value,
        "email_credential_id": email_credential_id,
        "captcha_credential_id": captcha_credential_id,
        "auto_delete_at": auto_delete_at,
        "platform_options": platform_options,
    }

    return cloned_name, TaskResolvedConfig(
        platform=platform,
        quantity=quantity,
        concurrency=concurrency,
        email_credential_id=email_credential_id,
        captcha_credential_id=captcha_credential_id,
        proxy_value=str(proxy_value) if proxy_value else None,
        proxy_mode=proxy_mode,
        source=source,
        schedule_id=None,
        cpamc_auto_import=False,
        auto_delete_at=auto_delete_at,
        requested_config=cloned_requested,
    )


def split_cli_args(value: str | None) -> list[str]:
    import shlex

    raw = (value or "").strip()
    if not raw:
        return []
    try:
        return list(shlex.split(raw, posix=False))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid extra arguments: {exc}") from exc


def resolve_browser_automation_options(raw: dict[str, Any] | None) -> dict[str, Any]:
    options = dict(BROWSER_AUTOMATION_DEFAULTS)
    if raw:
        options.update(raw)

    adapter_path = str(options.get("adapter_path") or BROWSER_AUTOMATION_DEFAULTS["adapter_path"]).strip()
    browser_path = str(options.get("browser_path") or "").strip()
    extra_args = str(options.get("extra_args") or "").strip()
    env_json_raw = options.get("env_json")
    headless_raw = options.get("headless", True)

    if isinstance(env_json_raw, dict):
        env_payload = {str(key): value for key, value in env_json_raw.items()}
        env_json_text = json.dumps(env_payload, ensure_ascii=False)
    else:
        env_json_text = str(env_json_raw or "{}").strip() or "{}"
        try:
            parsed = json.loads(env_json_text)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid environment JSON: {exc.msg}") from exc
        if not isinstance(parsed, dict):
            raise HTTPException(status_code=400, detail="Environment JSON must be an object")
        env_payload = {str(key): value for key, value in parsed.items()}
        env_json_text = json.dumps(env_payload, ensure_ascii=False)

    headless = bool(headless_raw) if isinstance(headless_raw, bool) else str(headless_raw).strip().lower() in {"1", "true", "yes", "y", "on"}
    return {
        "adapter_path": adapter_path,
        "headless": headless,
        "browser_path": browser_path,
        "extra_args": extra_args,
        "extra_args_list": split_cli_args(extra_args),
        "env_json": env_json_text,
        "env": env_payload,
    }


def validate_platform(platform: str) -> dict[str, Any]:
    if platform not in PLATFORMS:
        raise HTTPException(status_code=400, detail="Unsupported platform")
    return PLATFORMS[platform]


def resolve_task_configuration(
    *,
    name: str,
    platform: str,
    quantity: int,
    concurrency: int | None,
    email_credential_id: int | None,
    captcha_credential_id: int | None,
    proxy_mode: str,
    proxy_id: int | None,
    source: str,
    schedule_id: int | None,
    cpamc_auto_import: bool,
    auto_delete_at: str | None,
    platform_options: dict[str, Any] | None = None,
) -> tuple[str, TaskResolvedConfig]:
    spec = validate_platform(platform)
    resolved_name = name.strip() or f"{platform}-{now().strftime('%Y%m%d-%H%M%S')}"
    resolved_concurrency = concurrency or int(spec["default_concurrency"])
    resolved_concurrency = max(1, resolved_concurrency)

    email_row = None
    captcha_row = None
    if spec["requires_email_credential"] or platform == "browser-automation-local":
        email_row = resolve_required_credential("gptmail", email_credential_id)
    if spec["requires_captcha_credential"]:
        captcha_row = resolve_required_credential("yescaptcha", captcha_credential_id)

    proxy_value = None
    if spec["supports_proxy"]:
        proxy_value = resolve_proxy_value(proxy_mode, proxy_id)

    resolved_platform_options: dict[str, Any] | None = None
    if platform == "browser-automation-local":
        resolved_platform_options = resolve_browser_automation_options(platform_options)

    requested_config = {
        "name": resolved_name,
        "platform": platform,
        "quantity": quantity,
        "concurrency": resolved_concurrency,
        "source": source,
        "schedule_id": schedule_id,
        "cpamc_auto_import": cpamc_auto_import,
        "proxy_mode": proxy_mode,
        "proxy_id": proxy_id,
        "proxy_value": proxy_value,
        "email_credential_id": int(email_row["id"]) if email_row else None,
        "captcha_credential_id": int(captcha_row["id"]) if captcha_row else None,
        "auto_delete_at": auto_delete_at,
        "platform_options": resolved_platform_options,
    }
    return resolved_name, TaskResolvedConfig(
        platform=platform,
        quantity=quantity,
        concurrency=resolved_concurrency,
        email_credential_id=int(email_row["id"]) if email_row else None,
        captcha_credential_id=int(captcha_row["id"]) if captcha_row else None,
        proxy_value=proxy_value,
        proxy_mode=proxy_mode,
        source=source,
        schedule_id=schedule_id,
        cpamc_auto_import=cpamc_auto_import,
        auto_delete_at=auto_delete_at,
        requested_config=requested_config,
    )


def build_email_credential_config(credential: sqlite3.Row | dict[str, Any], proxy: str | None = None) -> dict[str, Any]:
    provider = str(credential["kind"] or "").strip().lower()
    config = {
        "mail_provider": provider,
        "mail_api_key": str(credential["api_key"] or ""),
        "mail_base_url": str(credential["base_url"] or ""),
        "mail_prefix": str(credential["prefix"] or ""),
        "mail_domain": str(credential["domain"] or ""),
        "mail_secret": str(credential.get("secret") if isinstance(credential, dict) else credential["secret"] or ""),
        "mail_extra_json": str(credential.get("extra_json") if isinstance(credential, dict) else credential["extra_json"] or "{}"),
    }
    if provider == "gptmail":
        config.update(
            {
                "gptmail_api_key": str(credential["api_key"] or ""),
                "gptmail_base_url": str(credential["base_url"] or "https://mail.chatgpt.org.uk"),
                "gptmail_prefix": str(credential["prefix"] or ""),
                "gptmail_domain": str(credential["domain"] or ""),
            }
        )
    if proxy:
        config["proxy"] = proxy
    return config


def apply_email_credential_env(env: dict[str, str], credential_id: int | None) -> None:
    if not credential_id:
        return
    credential = get_credential(int(credential_id))
    config = build_email_credential_config(credential)
    env["MAIL_PROVIDER"] = str(config.get("mail_provider") or "")
    env["MAIL_API_KEY"] = str(config.get("mail_api_key") or "")
    env["MAIL_BASE_URL"] = str(config.get("mail_base_url") or "")
    env["MAIL_PREFIX"] = str(config.get("mail_prefix") or "")
    env["MAIL_DOMAIN"] = str(config.get("mail_domain") or "")
    env["MAIL_SECRET"] = str(config.get("mail_secret") or "")
    env["MAIL_EXTRA_JSON"] = str(config.get("mail_extra_json") or "{}")
    if config.get("gptmail_api_key"):
        env["GPTMAIL_API_KEY"] = str(config["gptmail_api_key"])
    if config.get("gptmail_base_url"):
        env["GPTMAIL_BASE_URL"] = str(config["gptmail_base_url"])
    if config.get("gptmail_prefix"):
        env["GPTMAIL_PREFIX"] = str(config["gptmail_prefix"])
    if config.get("gptmail_domain"):
        env["GPTMAIL_DOMAIN"] = str(config["gptmail_domain"])


def insert_task(*, name: str, config: TaskResolvedConfig) -> int:
    timestamp = now_iso()
    placeholder_dir = TASKS_DIR / f"pending_{int(time.time() * 1000)}_{secrets.token_hex(3)}"
    placeholder_dir.mkdir(parents=True, exist_ok=True)
    console_path = placeholder_dir / "console.log"
    task_id = execute(
        """
        INSERT INTO tasks (
            name, platform, quantity, status, email_credential_id, captcha_credential_id, concurrency,
            proxy, task_dir, console_path, archive_path, requested_config_json, created_at, source, schedule_id, auto_delete_at
        )
        VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
        """,
        (
            name,
            config.platform,
            config.quantity,
            config.email_credential_id,
            config.captcha_credential_id,
            config.concurrency,
            config.proxy_value,
            str(placeholder_dir),
            str(console_path),
            json.dumps(config.requested_config, ensure_ascii=False),
            timestamp,
            config.source,
            config.schedule_id,
            config.auto_delete_at,
        ),
    )
    final_dir = TASKS_DIR / f"task_{task_id}"
    final_console_path = final_dir / "console.log"
    if placeholder_dir.exists():
        if final_dir.exists():
            shutil.rmtree(final_dir, ignore_errors=True)
        placeholder_dir.rename(final_dir)
    execute_no_return(
        "UPDATE tasks SET task_dir = ?, console_path = ? WHERE id = ?",
        (str(final_dir), str(final_console_path), task_id),
    )
    write_json(final_dir / "task.json", {"id": task_id, **config.requested_config, "created_at": timestamp})
    return task_id


def create_task_from_schedule(schedule: sqlite3.Row, *, update_last_run_date: bool) -> int:
    quantity = int(schedule["quantity"])
    concurrency = int(schedule["concurrency"])
    proxy_mode = "default" if int(schedule["use_proxy"] or 0) else "none"
    schedule_name = f"{schedule['name']} {now().strftime('%Y-%m-%d %H:%M:%S')}"
    _, config = resolve_task_configuration(
        name=schedule_name,
        platform=str(schedule["platform"]),
        quantity=quantity,
        concurrency=concurrency,
        email_credential_id=None,
        captcha_credential_id=None,
        proxy_mode=proxy_mode,
        proxy_id=None,
        source="schedule",
        schedule_id=int(schedule["id"]),
        cpamc_auto_import=bool(schedule["auto_import_cpamc"]),
        auto_delete_at=None,
    )
    task_id = insert_task(name=schedule_name, config=config)
    if update_last_run_date:
        execute_no_return(
            "UPDATE schedules SET last_run_date = ?, updated_at = ? WHERE id = ?",
            (now().strftime("%Y-%m-%d"), now_iso(), int(schedule["id"])),
        )
    return task_id


@dataclass
class ManagedProcess:
    task_id: int
    process: subprocess.Popen[str]
    log_handle: Any


class TaskSupervisor:
    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._processes: dict[int, ManagedProcess] = {}
        self._lock = threading.RLock()

    def start(self) -> None:
        self.recover_stale_tasks()
        self._thread = threading.Thread(target=self._run_loop, name="register-supervisor", daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        with self._lock:
            items = list(self._processes.values())
        for item in items:
            self._terminate_process(item.process)
            try:
                item.log_handle.close()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    def recover_stale_tasks(self) -> None:
        for row in fetch_all("SELECT * FROM tasks WHERE status IN ('running', 'stopping')"):
            execute_no_return(
                """
                UPDATE tasks
                SET status = 'interrupted',
                    finished_at = ?,
                    last_error = COALESCE(last_error, 'Process ended while the service was offline.'),
                    pid = NULL
                WHERE id = ?
                """,
                (now_iso(), int(row["id"])),
            )
            try:
                create_archive(get_task(int(row["id"])))
            except Exception:
                pass

    def stop_task(self, task_id: int) -> None:
        row = get_task(task_id)
        if row["status"] == "queued":
            execute_no_return(
                "UPDATE tasks SET status = 'stopped', finished_at = ?, last_error = ? WHERE id = ?",
                (now_iso(), "Task stopped before launch.", task_id),
            )
            create_archive(get_task(task_id))
            return
        with self._lock:
            managed = self._processes.get(task_id)
        if managed is None:
            raise HTTPException(status_code=409, detail="Task is not running")
        execute_no_return("UPDATE tasks SET status = 'stopping' WHERE id = ?", (task_id,))
        self._terminate_process(managed.process)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                cleanup_expired_sessions()
                self._finalize_finished()
                self._enforce_target_counts()
                self._trigger_schedules()
                self._cleanup_expired_tasks()
                self._launch_queued()
            except Exception as exc:
                print(f"[web-console] supervisor error: {exc}")
            time.sleep(POLL_INTERVAL_SECONDS)

    def _launch_queued(self) -> None:
        slots = MAX_CONCURRENT_TASKS - self._running_count()
        if slots <= 0:
            return
        queued = fetch_all("SELECT * FROM tasks WHERE status = 'queued' ORDER BY id ASC LIMIT ?", (slots,))
        for row in queued:
            self._start_task(row)

    def _running_count(self) -> int:
        with self._lock:
            return len(self._processes)

    def _start_task(self, task: sqlite3.Row) -> None:
        task = self._prepare_task_email_credential(task)
        if task is None:
            return
        paths = task_paths(task)
        task_dir = paths["task_dir"]
        task_dir.mkdir(parents=True, exist_ok=True)
        console_path = paths["console_path"]
        requested = json.loads(task["requested_config_json"])
        remaining_quantity = max(1, int(task["quantity"]) - count_result_lines(task))

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        stdin_payload: str | None = None

        if task["platform"] == "browser-automation-local":
            platform_options = resolve_browser_automation_options(requested.get("platform_options"))
            project_dir = ROOT_DIR
            adapter_path = ROOT_DIR / str(platform_options["adapter_path"])
            output_dir = task_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            apply_email_credential_env(env, task["email_credential_id"])
            env["MREGISTER_PROJECT_DIR"] = str(project_dir)
            env["MREGISTER_TASK_DIR"] = str(task_dir)
            env["MREGISTER_OUTPUT_DIR"] = str(output_dir)
            env["MREGISTER_RESULTS_FILE"] = str(output_dir / "results.txt")
            env["MREGISTER_HEADLESS"] = "1" if platform_options["headless"] else "0"
            env["MREGISTER_BROWSER_PATH"] = str(platform_options["browser_path"])
            env["MREGISTER_EXTRA_ARGS"] = "\n".join(platform_options["extra_args_list"])
            env["MREGISTER_EXTRA_ENV_JSON"] = str(platform_options["env_json"])
            env["BROWSER_HEADLESS"] = "true" if platform_options["headless"] else "false"
            if platform_options["browser_path"]:
                env["BROWSER_PATH"] = str(platform_options["browser_path"])
            for key, value in platform_options["env"].items():
                env[str(key)] = str(value)
            if task["proxy"]:
                env["MREGISTER_PROXY"] = str(task["proxy"])
                env["BROWSER_PROXY"] = str(task["proxy"])
            command = [
                sys.executable,
                str(WEB_DIR / "browser_automation_task.py"),
                "--project-dir",
                str(project_dir),
                "--adapter",
                str(adapter_path),
                "--task-dir",
                str(task_dir),
                "--quantity",
                str(remaining_quantity),
                "--workers",
                str(int(task["concurrency"])),
            ]
            command.extend(platform_options["extra_args_list"])
            cwd = ROOT_DIR
        elif task["platform"] == "openai-register":
            credential = get_credential(int(task["email_credential_id"]))
            env["GPTMAIL_API_KEY"] = credential["api_key"]
            if credential["base_url"]:
                env["GPTMAIL_BASE_URL"] = credential["base_url"]
            if credential["prefix"]:
                env["GPTMAIL_PREFIX"] = credential["prefix"]
            if credential["domain"]:
                env["GPTMAIL_DOMAIN"] = credential["domain"]
            command = [
                sys.executable,
                str(ROOT_DIR / "openai-register" / "openai_register.py"),
                "--output-dir",
                str(task_dir / "output"),
                "--sleep-min",
                "2",
                "--sleep-max",
                "5",
            ]
            if task["proxy"]:
                command.extend(["--proxy", str(task["proxy"])])
            cwd = ROOT_DIR / "openai-register"
        elif task["platform"] == "chatgpt-register-v2":
            output_dir = task_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            apply_email_credential_env(env, task["email_credential_id"])
            env["OUTPUT_FILE"] = str(output_dir / "registered_accounts.txt")
            env["AK_FILE"] = str(output_dir / "ak.txt")
            env["RK_FILE"] = str(output_dir / "rk.txt")
            env["TOKEN_JSON_DIR"] = str(output_dir / "tokens")
            if task["proxy"]:
                env["PROXY"] = str(task["proxy"])
            command = [
                sys.executable,
                str(ROOT_DIR / "chatgpt_register_v2" / "chatgpt_register_v2.py"),
                "-n",
                str(remaining_quantity),
                "-w",
                str(int(task["concurrency"])),
            ]
            cwd = ROOT_DIR / "chatgpt_register_v2"
        elif task["platform"] == "chatgpt-register-v3":
            output_dir = task_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            apply_email_credential_env(env, task["email_credential_id"])
            env["OUTPUT_FILE"] = str(output_dir / "registered_accounts.txt")
            env["AK_FILE"] = str(output_dir / "ak.txt")
            env["RK_FILE"] = str(output_dir / "rk.txt")
            env["TOKEN_JSON_DIR"] = str(output_dir / "tokens")
            if task["proxy"]:
                env["PROXY"] = str(task["proxy"])
            command = [
                sys.executable,
                str(ROOT_DIR / "chatgpt_register_v3" / "chatgpt_register_v3.py"),
                "-n",
                str(remaining_quantity),
                "-w",
                str(int(task["concurrency"])),
            ]
            cwd = ROOT_DIR / "chatgpt_register_v3"
        elif task["platform"] == "grok-register":
            credential = get_credential(int(task["captcha_credential_id"]))
            env["YESCAPTCHA_KEY"] = credential["api_key"]
            command = [sys.executable, str(ROOT_DIR / "grok-register" / "grok.py")]
            cwd = task_dir
            stdin_payload = f"{int(task['concurrency'])}\n"
        else:
            raise RuntimeError(f"Unsupported platform: {task['platform']}")

        log_handle = console_path.open("a", encoding="utf-8", buffering=1)
        log_handle.write(f"[{now_iso()}] Starting task {task['id']} ({task['platform']})\n")
        log_handle.write(f"[{now_iso()}] Config: {json.dumps(requested, ensure_ascii=False)}\n")
        log_handle.flush()

        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=env,
            stdin=subprocess.PIPE if stdin_payload is not None else None,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
        )
        if stdin_payload is not None and process.stdin is not None:
            process.stdin.write(stdin_payload)
            process.stdin.flush()
            process.stdin.close()

        execute_no_return(
            """
            UPDATE tasks
            SET status = 'running',
                first_started_at = COALESCE(first_started_at, ?),
                started_at = ?,
                pid = ?,
                last_error = NULL
            WHERE id = ?
            """,
            (now_iso(), now_iso(), process.pid, int(task["id"])),
        )
        with self._lock:
            self._processes[int(task["id"])] = ManagedProcess(task_id=int(task["id"]), process=process, log_handle=log_handle)

    def _prepare_task_email_credential(self, task: sqlite3.Row) -> sqlite3.Row | None:
        credential_id = task["email_credential_id"]
        if not credential_id:
            return task
        credential = get_credential(int(credential_id))
        if credential["kind"] != "gptmail" or not credential_is_exhausted(credential):
            return task
        replacement = get_available_email_credential(exclude_ids={int(credential["id"])})
        if replacement is None:
            message = "No available email credential remains after the current credential was marked exhausted."
            append_task_console(task, message)
            execute_no_return(
                "UPDATE tasks SET status = 'failed', finished_at = ?, last_error = ? WHERE id = ?",
                (now_iso(), message, int(task["id"])),
            )
            return None
        append_task_console(
            task,
            f"Mail credential '{credential['name']}' is exhausted. Switched to '{replacement['name']}' before launch.",
        )
        execute_no_return("UPDATE tasks SET email_credential_id = ?, last_error = NULL WHERE id = ?", (int(replacement["id"]), int(task["id"])))
        return get_task(int(task["id"]))

    def _finalize_finished(self) -> None:
        with self._lock:
            items = list(self._processes.items())
        for task_id, item in items:
            exit_code = item.process.poll()
            if exit_code is None:
                continue
            try:
                item.log_handle.write(f"[{now_iso()}] Process exited with code {exit_code}\n")
                item.log_handle.flush()
            except Exception:
                pass
            try:
                item.log_handle.close()
            except Exception:
                pass
            with self._lock:
                self._processes.pop(task_id, None)
            self._complete_task(task_id, exit_code)

    def _enforce_target_counts(self) -> None:
        with self._lock:
            items = list(self._processes.items())
        for task_id, managed in items:
            row = fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
            if row is None or row["status"] != "running":
                continue
            if count_result_lines(row) >= int(row["quantity"]):
                execute_no_return("UPDATE tasks SET status = 'stopping' WHERE id = ?", (task_id,))
                self._terminate_process(managed.process)

    def _trigger_schedules(self) -> None:
        current = now()
        current_slot = schedule_slot_key(current)
        for schedule in fetch_all("SELECT * FROM schedules WHERE enabled = 1 ORDER BY id ASC"):
            cron_expression = str(schedule["cron_expression"] or "").strip()
            if not cron_expression:
                _, _, cron_expression = normalize_daily_schedule(str(schedule["time_of_day"] or ""))
            if not cron_matches_now(cron_expression, current):
                continue
            if str(schedule["last_run_slot"] or "") == current_slot:
                continue
            try:
                create_task_from_schedule(schedule, update_last_run_date=True)
                execute_no_return(
                    "UPDATE schedules SET last_run_slot = ?, updated_at = ? WHERE id = ?",
                    (current_slot, now_iso(), int(schedule["id"])),
                )
            except Exception as exc:
                print(f"[web-console] schedule {schedule['id']} failed: {exc}")

    def _cleanup_expired_tasks(self) -> None:
        expired = fetch_all(
            """
            SELECT * FROM tasks
            WHERE auto_delete_at IS NOT NULL
              AND auto_delete_at <= ?
              AND status NOT IN ('queued', 'running', 'stopping')
            """,
            (now_iso(),),
        )
        for row in expired:
            paths = task_paths(row)
            try:
                shutil.rmtree(paths["task_dir"], ignore_errors=True)
            except Exception:
                pass
            if paths["archive_path"].exists():
                try:
                    paths["archive_path"].unlink()
                except Exception:
                    pass
            execute_no_return("DELETE FROM tasks WHERE id = ?", (int(row["id"]),))

    def _complete_task(self, task_id: int, exit_code: int) -> None:
        row = get_task(task_id)
        results_count = count_result_lines(row)
        quantity = int(row["quantity"])
        current_status = row["status"]
        non_retry_reason = self._detect_non_retry_reason(row)
        exit_error = None if exit_code == 0 else (non_retry_reason or f"Task exited with code {exit_code}.")
        if results_count < quantity and non_retry_reason and self._rotate_exhausted_gptmail_credential(row, non_retry_reason):
            append_task_console(
                row,
                f"Current successful results: {results_count}/{quantity}. Switched GPTMail credential and re-queued task.",
            )
            execute_no_return(
                """
                UPDATE tasks
                SET status = 'queued',
                    pid = NULL,
                    exit_code = NULL,
                    last_error = NULL
                WHERE id = ?
                """,
                (task_id,),
            )
            return
        if self._should_retry_task(row, results_count):
            append_task_console(
                row,
                f"Current successful results: {results_count}/{quantity}. Re-queueing task to keep running until target is reached.",
            )
            execute_no_return(
                """
                UPDATE tasks
                SET status = 'queued',
                    pid = NULL,
                    exit_code = NULL,
                    last_error = NULL
                WHERE id = ?
                """,
                (task_id,),
            )
            return
        if results_count >= quantity:
            status = "completed"
            error = None
        elif current_status == "stopping":
            status = "stopped"
            error = row["last_error"] or "Task stopped by operator."
        elif results_count > 0:
            status = "partial"
            error = row["last_error"] or exit_error or f"Task finished with {results_count}/{quantity} successful results."
        else:
            status = "failed"
            error = row["last_error"] or exit_error or "Task finished without successful results."
        execute_no_return(
            """
            UPDATE tasks
            SET status = ?,
                finished_at = ?,
                exit_code = ?,
                pid = NULL,
                last_error = ?
            WHERE id = ?
            """,
            (status, now_iso(), exit_code, error, task_id),
        )
        completed_task = get_task(task_id)
        create_archive(completed_task)
        self._maybe_auto_import_task(completed_task)

    @staticmethod
    def _should_retry_task(task: sqlite3.Row, results_count: int) -> bool:
        if str(task["status"]) == "stopping":
            return False
        if int(task["quantity"]) <= results_count:
            return False
        if TaskSupervisor._detect_non_retry_reason(task):
            return False
        return str(task["platform"]) in ("chatgpt-register-v2", "chatgpt-register-v3")

    @staticmethod
    def _detect_non_retry_reason(task: sqlite3.Row) -> str | None:
        console_text = read_tail(Path(task["console_path"]), limit=12000).lower()
        if not console_text:
            return None

        hard_stop_markers = [
            "gptmail api quota exhausted",
            "gptmail usage limit reached",
            "gptmail call limit reached",
            "gptmail credits exhausted",
            "gptmail balance exhausted",
            "insufficient quota",
            "quota exceeded",
            "rate limit exceeded",
            "too many requests",
            "calls exhausted",
            "usage exhausted",
            "调用次数已用完",
            "调用次数不足",
            "额度已用完",
            "配额已用完",
            "余额不足",
            "请求次数已用完",
        ]
        for marker in hard_stop_markers:
            if marker in console_text:
                return "GPTMail quota or call limit exhausted. Stopped automatic retries."
        return None

    def _rotate_exhausted_gptmail_credential(self, task: sqlite3.Row, reason: str) -> bool:
        credential_id = task["email_credential_id"]
        if not credential_id:
            return False
        credential = get_credential(int(credential_id))
        if credential["kind"] != "gptmail":
            return False
        mark_credential_exhausted(credential, reason)
        replacement = get_available_email_credential(exclude_ids={int(credential["id"])})
        if replacement is None:
            append_task_console(
                task,
                f"Marked GPTMail credential '{credential['name']}' as exhausted. No replacement email credential is available.",
            )
            return False
        execute_no_return("UPDATE tasks SET email_credential_id = ? WHERE id = ?", (int(replacement["id"]), int(task["id"])))
        append_task_console(
            task,
            f"Marked GPTMail credential '{credential['name']}' as exhausted. Switched task to '{replacement['name']}'.",
        )
        return True

    def _maybe_auto_import_task(self, task: sqlite3.Row) -> None:
        if str(task["status"]) not in {"completed", "partial"}:
            return
        cpamc = get_cpamc_settings()
        if not (cpamc["auto_import_enabled"] or task_requests_cpamc_auto_import(task)):
            return
        if not cpamc_is_ready(cpamc):
            append_task_console(task, "Skipped auto import to CPAMC because CPAMC is not enabled or not linked.")
            return
        try:
            result = import_task_files_to_cpamc(task, cpamc=cpamc)
            append_task_console(
                task,
                f"Auto import to CPAMC finished: imported {result['imported_count']}, skipped {result['skipped_count']}, failed {result['failed_count']}.",
            )
        except Exception as exc:
            append_task_console(task, f"Auto import to CPAMC failed: {exc}")

    @staticmethod
    def _terminate_process(process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=10)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass


supervisor = TaskSupervisor()


def state_payload() -> dict[str, Any]:
    return {
        "platforms": PLATFORMS,
        "browser_automation_templates": BROWSER_AUTOMATION_TEMPLATES,
        "defaults": get_defaults(),
        "cpamc": get_cpamc_settings(),
        "credentials": get_credentials(),
        "proxies": get_proxies(),
        "tasks": get_tasks(),
        "schedules": get_schedules(),
        "api_keys": get_api_keys(),
        "dashboard": dashboard_summary(),
        "max_concurrent_tasks": MAX_CONCURRENT_TASKS,
        "server_now": now_iso(),
    }


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    cleanup_expired_sessions()
    supervisor.start()
    yield
    supervisor.shutdown()


app = FastAPI(title="Register Task Console", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")


def make_session_response(payload: dict[str, Any], raw_token: str | None = None, expires_at: str | None = None) -> JSONResponse:
    response = JSONResponse(payload)
    if raw_token and expires_at:
        response.set_cookie(
            SESSION_COOKIE,
            raw_token,
            httponly=True,
            samesite="lax",
            max_age=SESSION_TTL_HOURS * 3600,
            expires=expires_at,
        )
    return response


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    ui_lang = detect_ui_lang(request)
    translations = get_ui_translations(ui_lang)
    auth_view = "app"
    if not auth_is_configured():
        auth_view = "setup"
    elif not is_authenticated_request(request):
        auth_view = "login"
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "auth_view": auth_view,
            "platforms": PLATFORMS,
            "browser_automation_templates": BROWSER_AUTOMATION_TEMPLATES,
            "max_concurrent_tasks": MAX_CONCURRENT_TASKS,
            "api_base_url": str(request.base_url).rstrip("/"),
            "ui_lang": ui_lang,
            "t": translations,
            "translations_json": json.dumps(translations, ensure_ascii=False),
        },
    )


@app.get("/api/auth/state")
async def auth_state(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "configured": auth_is_configured(),
            "authenticated": is_authenticated_request(request) if auth_is_configured() else False,
        }
    )


@app.post("/api/auth/setup")
async def auth_setup(payload: PasswordPayload) -> JSONResponse:
    if auth_is_configured():
        raise HTTPException(status_code=409, detail="Admin password is already configured")
    set_setting("admin_password_hash", hash_password(payload.password))
    raw_token, expires_at = create_session_token()
    return make_session_response({"ok": True}, raw_token, expires_at)


@app.post("/api/auth/login")
async def auth_login(payload: PasswordPayload) -> JSONResponse:
    if not verify_password(payload.password, get_setting("admin_password_hash")):
        raise HTTPException(status_code=401, detail="Password is incorrect")
    raw_token, expires_at = create_session_token()
    return make_session_response({"ok": True}, raw_token, expires_at)


@app.post("/api/auth/logout")
async def auth_logout(request: Request) -> JSONResponse:
    delete_session(request.cookies.get(SESSION_COOKIE))
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE)
    return response


@app.get("/api/state")
async def api_state(request: Request) -> JSONResponse:
    require_authenticated(request)
    return JSONResponse(state_payload())


@app.post("/api/defaults")
async def update_defaults(payload: DefaultSettingsPayload, request: Request) -> JSONResponse:
    require_authenticated(request)
    if payload.default_gptmail_credential_id is not None and not is_email_credential_kind(str(get_credential(payload.default_gptmail_credential_id)["kind"] or "")):
        raise HTTPException(status_code=400, detail="Default email credential is invalid")
    if payload.default_yescaptcha_credential_id is not None and get_credential(payload.default_yescaptcha_credential_id)["kind"] != "yescaptcha":
        raise HTTPException(status_code=400, detail="Default YesCaptcha credential is invalid")
    if payload.default_proxy_id is not None:
        get_proxy(payload.default_proxy_id)
    set_setting("default_gptmail_credential_id", str(payload.default_gptmail_credential_id) if payload.default_gptmail_credential_id else None)
    set_setting("default_yescaptcha_credential_id", str(payload.default_yescaptcha_credential_id) if payload.default_yescaptcha_credential_id else None)
    set_setting("default_proxy_id", str(payload.default_proxy_id) if payload.default_proxy_id else None)
    return JSONResponse({"ok": True, "defaults": get_defaults()})


@app.post("/api/cpamc")
async def update_cpamc_settings(payload: CpamcSettingsPayload, request: Request) -> JSONResponse:
    require_authenticated(request)
    previous = get_cpamc_settings()
    base_url = normalize_cpamc_base_url(payload.base_url)
    management_key = (payload.management_key or "").strip()
    if payload.enabled and not base_url:
        raise HTTPException(status_code=400, detail="CPAMC link is required when enabled")
    if payload.enabled and not management_key:
        raise HTTPException(status_code=400, detail="CPAMC management key is required when enabled")
    linked = previous["linked"] and previous["base_url"] == base_url and previous["management_key"] == management_key
    saved = set_cpamc_settings(
        {
            "enabled": payload.enabled,
            "base_url": base_url,
            "management_key": management_key,
            "linked": linked,
            "last_error": previous["last_error"] if linked else "",
            "auto_import_enabled": payload.auto_import_enabled,
        }
    )
    return JSONResponse({"ok": True, "cpamc": saved})


@app.post("/api/cpamc/test")
async def test_cpamc_settings(payload: CpamcSettingsPayload, request: Request) -> JSONResponse:
    require_authenticated(request)
    base_url = normalize_cpamc_base_url(payload.base_url)
    management_key = (payload.management_key or "").strip()
    if not base_url:
        raise HTTPException(status_code=400, detail="CPAMC link is required")
    if not management_key:
        raise HTTPException(status_code=400, detail="CPAMC management key is required")
    try:
        response = cpamc_request(
            "GET",
            base_url=base_url,
            management_key=management_key,
            path="config",
        )
    except requests.RequestException as exc:
        set_cpamc_settings(
            {
                "enabled": payload.enabled,
                "base_url": base_url,
                "management_key": management_key,
                "linked": False,
                "last_error": str(exc),
                "auto_import_enabled": payload.auto_import_enabled,
            }
        )
        raise HTTPException(status_code=502, detail=f"CPAMC connection failed: {exc}") from exc
    if not response.ok:
        message = parse_cpamc_error(response)
        set_cpamc_settings(
            {
                "enabled": payload.enabled,
                "base_url": base_url,
                "management_key": management_key,
                "linked": False,
                "last_error": message,
                "auto_import_enabled": payload.auto_import_enabled,
            }
        )
        raise HTTPException(status_code=502, detail=f"CPAMC test failed: {message}")
    saved = set_cpamc_settings(
        {
            "enabled": payload.enabled,
            "base_url": base_url,
            "management_key": management_key,
            "linked": True,
            "last_error": "",
            "auto_import_enabled": payload.auto_import_enabled,
        }
    )
    return JSONResponse({"ok": True, "linked": True, "cpamc": saved})


@app.post("/api/credentials")
async def create_credential(payload: CredentialCreate, request: Request) -> JSONResponse:
    require_authenticated(request)
    if payload.kind not in EMAIL_CREDENTIAL_KINDS | {"yescaptcha"}:
        raise HTTPException(status_code=400, detail="Unsupported credential kind")
    extra_json = (payload.extra_json or "").strip() or None
    if extra_json is not None:
        try:
            parsed_extra = json.loads(extra_json)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid extra JSON: {exc.msg}") from exc
        if not isinstance(parsed_extra, dict):
            raise HTTPException(status_code=400, detail="Extra JSON must be an object")
        extra_json = json.dumps(parsed_extra, ensure_ascii=False)
    if payload.kind == "yescaptcha" and not str(payload.api_key or "").strip():
        raise HTTPException(status_code=400, detail="YesCaptcha API Key is required")
    if payload.kind in {"gptmail", "moemail", "cloudflare_temp_email"} and not str(payload.api_key or "").strip():
        raise HTTPException(status_code=400, detail="This mail provider requires an API key")
    timestamp = now_iso()
    credential_id = execute(
        """
        INSERT INTO credentials (name, kind, api_key, base_url, prefix, domain, secret, extra_json, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.name.strip(),
            payload.kind,
            str(payload.api_key or "").strip(),
            (payload.base_url or "").strip() or None,
            (payload.prefix or "").strip() or None,
            (payload.domain or "").strip() or None,
            (payload.secret or "").strip() or None,
            extra_json,
            (payload.notes or "").strip() or None,
            timestamp,
            timestamp,
        ),
    )
    return JSONResponse({"ok": True, "id": credential_id})


@app.delete("/api/credentials/{credential_id}")
async def delete_credential(credential_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    active = fetch_one(
        """
        SELECT id FROM tasks
        WHERE (email_credential_id = ? OR captcha_credential_id = ?)
          AND status IN ('queued', 'running', 'stopping')
        """,
        (credential_id, credential_id),
    )
    if active is not None:
        raise HTTPException(status_code=409, detail="Credential is used by an active task")
    defaults = get_defaults()
    for key, value in defaults.items():
        if value == credential_id:
            set_setting(key, None)
    execute_no_return("DELETE FROM credentials WHERE id = ?", (credential_id,))
    return JSONResponse({"ok": True})


@app.post("/api/proxies")
async def create_proxy(payload: ProxyCreate, request: Request) -> JSONResponse:
    require_authenticated(request)
    timestamp = now_iso()
    proxy_id = execute(
        """
        INSERT INTO proxies (name, proxy_url, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            payload.name.strip(),
            payload.proxy_url.strip(),
            (payload.notes or "").strip() or None,
            timestamp,
            timestamp,
        ),
    )
    return JSONResponse({"ok": True, "id": proxy_id})


@app.delete("/api/proxies/{proxy_id}")
async def delete_proxy(proxy_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    proxy = get_proxy(proxy_id)
    active = fetch_one("SELECT id FROM tasks WHERE proxy = ? AND status IN ('queued', 'running', 'stopping')", (str(proxy["proxy_url"]),))
    if active is not None:
        raise HTTPException(status_code=409, detail="Proxy is used by an active task")
    if get_defaults()["default_proxy_id"] == proxy_id:
        set_setting("default_proxy_id", None)
    execute_no_return("DELETE FROM proxies WHERE id = ?", (proxy_id,))
    return JSONResponse({"ok": True})


@app.post("/api/tasks")
async def create_task(payload: TaskCreate, request: Request) -> JSONResponse:
    require_authenticated(request)
    name, config = resolve_task_configuration(
        name=payload.name,
        platform=payload.platform,
        quantity=payload.quantity,
        concurrency=payload.concurrency,
        email_credential_id=payload.email_credential_id,
        captcha_credential_id=payload.captcha_credential_id,
        proxy_mode=payload.proxy_mode,
        proxy_id=payload.proxy_id,
        source="ui",
        schedule_id=None,
        cpamc_auto_import=False,
        auto_delete_at=None,
        platform_options=payload.platform_options,
    )
    task_id = insert_task(name=name, config=config)
    return JSONResponse({"ok": True, "id": task_id})


@app.get("/api/tasks/{task_id}")
async def task_detail(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    return JSONResponse({"task": serialize_task(get_task(task_id))})


@app.get("/api/tasks/{task_id}/console")
async def task_console(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_task(task_id)
    return JSONResponse({"task_id": task_id, "console": read_tail(Path(row["console_path"]))})


@app.get("/api/tasks/{task_id}/success-accounts")
async def task_success_accounts(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_task(task_id)
    path = extract_success_accounts(row)
    preview = read_tail(path, limit=12000) if path else ""
    accounts = success_account_items(row)
    return JSONResponse({"task_id": task_id, "count": len(accounts), "content": preview, "accounts": accounts})


@app.post("/api/tasks/{task_id}/success-accounts/oauth")
async def regenerate_task_success_account_oauth(task_id: int, payload: SuccessAccountOAuthRequest, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_task(task_id)
    email = payload.email.strip()
    account = find_success_account_record(row, email=email)
    if account is None:
        raise HTTPException(status_code=404, detail="The specified success account was not found in this task")
    password = str(account.get("password") or "").strip()
    result = regenerate_success_account_oauth_token(row, email, password)
    return JSONResponse(result)


@app.post("/api/success-accounts/oauth/batch")
async def regenerate_success_accounts_oauth_batch_route(payload: SuccessAccountOAuthBatchRequest, request: Request) -> JSONResponse:
    require_authenticated(request)
    items = resolve_success_account_batch_items(payload)
    result = regenerate_success_account_oauth_batch(items)
    return JSONResponse(result)


@app.post("/api/tasks/{task_id}/success-accounts/{email}/cpamc-retry")
async def retry_task_success_account_cpamc(task_id: int, email: str, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_task(task_id)
    normalized_email = email.strip()
    accounts = {item["email"]: item for item in success_account_items(row)}
    if normalized_email not in accounts:
        raise HTTPException(status_code=404, detail="The specified success account was not found in this task")
    append_task_console(row, f"Retrying CPAMC import for {normalized_email}.")
    statuses = load_success_account_statuses(row)
    try:
        result = import_success_account_token_to_cpamc(row, normalized_email)
        statuses[normalized_email] = {
            "cpamc_imported": True,
            "cpamc_error": "",
            "token_json": str(result.get("token_json") or ""),
            "updated_at": now_iso(),
        }
        save_success_account_statuses(row, statuses)
        if result.get("skipped"):
            append_task_console(row, f"CPAMC import retry skipped for {normalized_email} because the same content was already imported.")
        else:
            append_task_console(row, f"CPAMC import retry succeeded for {normalized_email}.")
        return JSONResponse({"ok": True, "email": normalized_email, "cpamc": result})
    except Exception as exc:
        statuses[normalized_email] = {
            "cpamc_imported": False,
            "cpamc_error": str(exc),
            "token_json": str(accounts[normalized_email].get("token_json") or ""),
            "updated_at": now_iso(),
        }
        save_success_account_statuses(row, statuses)
        append_task_console(row, f"CPAMC import retry failed for {normalized_email}: {exc}")
        raise HTTPException(status_code=502, detail=f"CPAMC import retry failed: {exc}") from exc


@app.post("/api/tasks/backfill-success-accounts")
async def backfill_success_accounts(request: Request) -> JSONResponse:
    require_authenticated(request)
    result = backfill_all_success_accounts()
    return JSONResponse({"ok": True, **result})


@app.get("/api/success-accounts")
async def success_accounts_listing(request: Request, page: int = 1, page_size: int = 20, search: str = "", schedule_id: int | None = None) -> JSONResponse:
    require_authenticated(request)
    return JSONResponse(query_success_accounts(page=page, page_size=page_size, search=search, schedule_id=schedule_id))


@app.post("/api/tasks/{task_id}/stop")
async def stop_task(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    supervisor.stop_task(task_id)
    return JSONResponse({"ok": True})


@app.post("/api/tasks/{task_id}/rerun")
async def rerun_task(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_task(task_id)
    if row["status"] in {"queued", "running", "stopping"}:
        raise HTTPException(status_code=409, detail="Stop the task before rerunning it")
    requested_config = json.loads(row["requested_config_json"])
    task_name, config = clone_task_config_from_requested(
        requested=requested_config,
        source="ui-rerun",
        name_suffix=" (rerun)",
    )
    new_task_id = insert_task(name=task_name, config=config)
    return JSONResponse({"ok": True, "task_id": new_task_id})


@app.post("/api/tasks/{task_id}/cpamc-import")
async def import_task_to_cpamc(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    task = get_task(task_id)
    force = request.query_params.get("force") == "1"
    try:
        result = import_task_files_to_cpamc(task, force=force)
    except RuntimeError as exc:
        message = str(exc)
        status_code = 400
        if message.startswith("CPAMC import failed:"):
            status_code = 502
        raise HTTPException(status_code=status_code, detail=message) from exc
    return JSONResponse(result)


@app.get("/api/tasks/{task_id}/download")
async def download_task(task_id: int, request: Request) -> FileResponse:
    require_authenticated(request)
    row = get_task(task_id)
    archive_path = create_archive(row)
    return FileResponse(path=archive_path, media_type="application/zip", filename=f"task_{task_id}_{row['platform']}.zip")


@app.get("/api/tasks/{task_id}/success-accounts/download")
async def download_task_success_accounts(task_id: int, request: Request) -> FileResponse:
    require_authenticated(request)
    row = get_task(task_id)
    path = extract_success_accounts(row)
    if path is None:
        raise HTTPException(status_code=404, detail="Success accounts file not found")
    return FileResponse(path=path, media_type="text/plain; charset=utf-8", filename=f"task_{task_id}_success_accounts.txt")


@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_task(task_id)
    if row["status"] in {"queued", "running", "stopping"}:
        raise HTTPException(status_code=409, detail="Stop the task before deleting it")
    paths = task_paths(row)
    try:
        shutil.rmtree(paths["task_dir"], ignore_errors=True)
    except Exception:
        pass
    if paths["archive_path"].exists():
        try:
            paths["archive_path"].unlink()
        except Exception:
            pass
    execute_no_return("DELETE FROM tasks WHERE id = ?", (task_id,))
    return JSONResponse({"ok": True})


@app.post("/api/schedules")
async def create_schedule(payload: ScheduleCreate, request: Request) -> JSONResponse:
    require_authenticated(request)
    validate_platform(payload.platform)
    normalized_time, schedule_config, cron_expression = normalize_visual_schedule(payload.schedule_kind, payload.schedule_config, payload.time_of_day)
    timestamp = now_iso()
    schedule_id = execute(
        """
        INSERT INTO schedules (name, platform, quantity, concurrency, time_of_day, cron_expression, schedule_kind, schedule_config_json, use_proxy, auto_import_cpamc, enabled, last_run_date, last_run_slot, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
        """,
        (
            payload.name.strip(),
            payload.platform,
            payload.quantity,
            payload.concurrency,
            normalized_time,
            cron_expression,
            payload.schedule_kind,
            json.dumps(schedule_config, ensure_ascii=False),
            1 if payload.use_proxy else 0,
            1 if payload.auto_import_cpamc else 0,
            1 if payload.enabled else 0,
            timestamp,
            timestamp,
        ),
    )
    return JSONResponse({"ok": True, "id": schedule_id})


@app.post("/api/schedules/{schedule_id}/toggle")
async def toggle_schedule(schedule_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_schedule(schedule_id)
    next_value = 0 if int(row["enabled"]) else 1
    execute_no_return("UPDATE schedules SET enabled = ?, updated_at = ? WHERE id = ?", (next_value, now_iso(), schedule_id))
    return JSONResponse({"ok": True})


@app.post("/api/schedules/{schedule_id}/run")
async def run_schedule_now(schedule_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    row = get_schedule(schedule_id)
    task_id = create_task_from_schedule(row, update_last_run_date=False)
    return JSONResponse({"ok": True, "id": task_id})


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    execute_no_return("DELETE FROM schedules WHERE id = ?", (schedule_id,))
    return JSONResponse({"ok": True})


@app.post("/api/api-keys")
async def create_api_key(payload: ApiKeyCreate, request: Request) -> JSONResponse:
    require_authenticated(request)
    raw_key, key_hash, prefix = generate_api_key_secret()
    key_id = execute(
        """
        INSERT INTO api_keys (name, key_hash, key_prefix, is_active, created_at, last_used_at)
        VALUES (?, ?, ?, 1, ?, NULL)
        """,
        (payload.name.strip(), key_hash, prefix, now_iso()),
    )
    return JSONResponse({"ok": True, "id": key_id, "api_key": raw_key})


@app.delete("/api/api-keys/{key_id}")
async def delete_api_key(key_id: int, request: Request) -> JSONResponse:
    require_authenticated(request)
    execute_no_return("DELETE FROM api_keys WHERE id = ?", (key_id,))
    return JSONResponse({"ok": True})


@app.post("/api/external/tasks")
async def external_create_task(payload: ExternalTaskCreate, request: Request) -> JSONResponse:
    require_api_key(request)
    auto_delete_at = date_iso(now() + timedelta(hours=24))
    task_name = payload.name or f"api-{payload.platform}-{now().strftime('%Y%m%d-%H%M%S')}"
    proxy_mode = "default" if payload.use_proxy else "none"
    _, config = resolve_task_configuration(
        name=task_name,
        platform=payload.platform,
        quantity=payload.quantity,
        concurrency=payload.concurrency,
        email_credential_id=None,
        captcha_credential_id=None,
        proxy_mode=proxy_mode,
        proxy_id=None,
        source="api",
        schedule_id=None,
        cpamc_auto_import=False,
        auto_delete_at=auto_delete_at,
        platform_options=payload.platform_options,
    )
    task_id = insert_task(name=task_name, config=config)
    return JSONResponse({"ok": True, "task_id": task_id, "auto_delete_at": auto_delete_at})


@app.get("/api/external/tasks/{task_id}")
async def external_task_status(task_id: int, request: Request) -> JSONResponse:
    require_api_key(request)
    row = get_task(task_id)
    if row["source"] != "api":
        raise HTTPException(status_code=404, detail="API task not found")
    item = serialize_task(row)
    payload = {
        "task_id": task_id,
        "status": item["status"],
        "completed_count": item["results_count"],
        "target_quantity": item["quantity"],
        "auto_delete_at": item["auto_delete_at"],
        "download_url": None,
    }
    if item["status"] not in {"queued", "running", "stopping"}:
        payload["download_url"] = f"/api/external/tasks/{task_id}/download"
    return JSONResponse(payload)


@app.get("/api/external/tasks/{task_id}/download")
async def external_download_task(task_id: int, request: Request) -> FileResponse:
    require_api_key(request)
    row = get_task(task_id)
    if row["source"] != "api":
        raise HTTPException(status_code=404, detail="API task not found")
    archive_path = create_archive(row)
    return FileResponse(path=archive_path, media_type="application/zip", filename=f"api_task_{task_id}_{row['platform']}.zip")
