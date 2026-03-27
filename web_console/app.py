from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import secrets
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile
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
        "default_gptmail": "默认 GPTMail",
        "default_yescaptcha": "默认 YesCaptcha",
        "default_proxy": "默认代理",
        "save_defaults": "保存默认设置",
        "panel_recent_tasks_title": "最近任务",
        "panel_recent_tasks_desc": "点任意任务可直接跳到详情页查看控制台输出。",
        "section_credentials": "凭据管理",
        "credentials_create_title": "新增凭据",
        "credentials_create_desc": "支持 GPTMail 与 YesCaptcha，保存后可直接设为默认。",
        "gptmail_optional_hint": "GPTMail 的 Base URL、邮箱前缀、邮箱域名都有默认值，可直接留空不填写。",
        "credentials_saved_title": "已保存凭据",
        "credentials_saved_desc": "支持删除、查看备注、设为默认。",
        "field_name": "名称",
        "field_kind": "类型",
        "field_api_key": "API Key",
        "field_base_url": "Base URL",
        "field_prefix": "邮箱前缀",
        "field_domain": "邮箱域名",
        "field_base_url_placeholder": "留空使用默认 Base URL",
        "field_prefix_placeholder": "留空使用默认邮箱前缀",
        "field_domain_placeholder": "留空使用默认邮箱域名",
        "field_notes": "备注",
        "save_credential": "保存凭据",
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
        "regenerate_oauth_prompt": "将为 {email} 重新执行 OAuth 登录并写入 tokens 目录。继续吗？",
        "regenerate_oauth_result_title": "OAuth Token 获取结果",
        "regenerate_oauth_success": "已为 {email} 重新获取 OAuth Token。",
        "regenerate_oauth_cpamc_success": "并已导入到 CPAMC。",
        "regenerate_oauth_cpamc_skipped": "CPAMC 已存在相同内容，已跳过重复导入。",
        "regenerate_oauth_cpamc_failed": "CPAMC 导入失败：{error}",
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
        "schedules_create_desc": "每天在固定时间自动创建一个独立任务。",
        "schedules_saved_title": "已保存定时任务",
        "schedules_saved_desc": "可以启用、停用或删除。",
        "field_time_of_day": "执行时间",
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
        "task_live_timer_hint": "实时计时",
        "task_created_time": "创建时间",
        "task_started_time": "开始时间",
        "task_finished_time": "结束时间",
        "task_total_duration": "总耗时",
        "task_time_unknown": "--",
        "last_used_at": "最近使用时间 {value}",
        "unused": "暂未使用",
        "use_default_gptmail": "使用默认 GPTMail",
        "use_default_yescaptcha": "使用默认 YesCaptcha",
        "choose_proxy": "选择一个代理",
        "no_default_gptmail": "不设置默认 GPTMail",
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
        "default_gptmail": "Default GPTMail",
        "default_yescaptcha": "Default YesCaptcha",
        "default_proxy": "Default proxy",
        "save_defaults": "Save defaults",
        "panel_recent_tasks_title": "Recent tasks",
        "panel_recent_tasks_desc": "Click any task to jump straight into the detail view and console output.",
        "section_credentials": "Credential Management",
        "credentials_create_title": "Add credential",
        "credentials_create_desc": "Supports GPTMail and YesCaptcha. You can set the saved item as default immediately.",
        "gptmail_optional_hint": "For GPTMail, Base URL, email prefix, and email domain all have defaults, so you can leave them blank.",
        "credentials_saved_title": "Saved credentials",
        "credentials_saved_desc": "Delete, review notes, and set defaults here.",
        "field_name": "Name",
        "field_kind": "Type",
        "field_api_key": "API Key",
        "field_base_url": "Base URL",
        "field_prefix": "Email prefix",
        "field_domain": "Email domain",
        "field_base_url_placeholder": "Leave blank to use the default Base URL",
        "field_prefix_placeholder": "Leave blank to use the default email prefix",
        "field_domain_placeholder": "Leave blank to use the default email domain",
        "field_notes": "Notes",
        "save_credential": "Save credential",
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
        "regenerate_oauth_prompt": "Run OAuth login again for {email} and write fresh tokens into the task tokens directory?",
        "regenerate_oauth_result_title": "OAuth token result",
        "regenerate_oauth_success": "Regenerated OAuth token for {email}.",
        "regenerate_oauth_cpamc_success": "Imported to CPAMC as well.",
        "regenerate_oauth_cpamc_skipped": "Skipped CPAMC import because the same content was already imported.",
        "regenerate_oauth_cpamc_failed": "CPAMC import failed: {error}",
        "cpamc_badge_imported": "Imported to CPAMC",
        "cpamc_badge_failed": "Import failed",
        "extract_history_success_accounts": "Extract historical success accounts",
        "extract_history_success_accounts_done": "Scanned {updated} tasks and extracted successful accounts from {non_empty} tasks.",
        "section_schedules": "Schedules",
        "schedules_create_title": "Add schedule",
        "schedules_create_desc": "Create an independent task automatically at the same time every day.",
        "schedules_saved_title": "Saved schedules",
        "schedules_saved_desc": "Enable, disable, or delete scheduled tasks here.",
        "field_time_of_day": "Run time",
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
        "task_live_timer_hint": "Live timer",
        "task_created_time": "Created",
        "task_started_time": "Started",
        "task_finished_time": "Finished",
        "task_total_duration": "Total duration",
        "task_time_unknown": "--",
        "last_used_at": "Last used {value}",
        "unused": "Not used yet",
        "use_default_gptmail": "Use default GPTMail",
        "use_default_yescaptcha": "Use default YesCaptcha",
        "choose_proxy": "Choose a proxy",
        "no_default_gptmail": "No default GPTMail",
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

CPAMC_SETTING_KEYS = {
    "cpamc_enabled": "0",
    "cpamc_base_url": "",
    "cpamc_management_key": "",
    "cpamc_linked": "0",
    "cpamc_last_error": "",
}

db_lock = threading.RLock()
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))


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
                base_url TEXT,
                prefix TEXT,
                domain TEXT,
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
                use_proxy INTEGER NOT NULL DEFAULT 0,
                auto_import_cpamc INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run_date TEXT,
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
            },
        )
        ensure_columns(
            conn,
            "schedules",
            {
                "auto_import_cpamc": "INTEGER NOT NULL DEFAULT 0",
            },
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
    return {key: row[key] for key in row.keys()}


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

    accounts = load_success_accounts(task)
    if (email, password) not in accounts:
        raise HTTPException(status_code=404, detail="The specified success account was not found in this task")

    credential_id = task["email_credential_id"]
    if not credential_id:
        raise HTTPException(status_code=400, detail="This task does not have an email credential attached")

    credential = get_credential(int(credential_id))
    proxy = str(task["proxy"] or "").strip()
    output_dir = Path(task["task_dir"]) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "mail_provider": "gptmail",
        "gptmail_api_key": credential["api_key"],
        "gptmail_base_url": credential["base_url"] or "https://mail.chatgpt.org.uk",
        "gptmail_prefix": credential["prefix"] or "",
        "gptmail_domain": credential["domain"] or "",
        "proxy": proxy,
        "ak_file": str(output_dir / "ak.txt"),
        "rk_file": str(output_dir / "rk.txt"),
        "token_json_dir": str(output_dir / "tokens"),
        "upload_api_url": "",
        "upload_api_token": "",
        "enable_oauth": True,
        "oauth_required": True,
    }

    append_task_console(task, f"Starting OAuth token regeneration for {email}.")

    try:
        from chatgpt_register_v2.lib.chatgpt_client import ChatGPTClient
        from chatgpt_register_v2.lib.oauth_client import OAuthClient
        from chatgpt_register_v2.lib.skymail_client import init_mail_client
        from chatgpt_register_v2.lib.token_manager import TokenManager

        mail_client = init_mail_client(config)
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
        if cpamc_result:
            statuses[email] = {
                "cpamc_imported": bool(cpamc_result.get("imported")),
                "cpamc_error": str(cpamc_result.get("error") or ""),
                "token_json": str(token_json_path),
                "updated_at": now_iso(),
            }
            save_success_account_statuses(task, statuses)
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


def success_account_items(task: sqlite3.Row | dict[str, Any]) -> list[dict[str, Any]]:
    statuses = load_success_account_statuses(task)
    items: list[dict[str, Any]] = []
    for email, password in load_success_accounts(task):
        status = statuses.get(email, {})
        items.append(
            {
                "email": email,
                "password": password,
                "cpamc_imported": bool(status.get("cpamc_imported")),
                "cpamc_error": str(status.get("cpamc_error") or ""),
                "token_json": str(status.get("token_json") or ""),
                "updated_at": str(status.get("updated_at") or ""),
            }
        )
    return items


def query_success_accounts(*, page: int, page_size: int, search: str, schedule_id: int | None = None) -> dict[str, Any]:
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
    return [row_to_dict(row) for row in fetch_all("SELECT * FROM schedules ORDER BY id DESC")]


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
        raise HTTPException(status_code=400, detail=f"No default {kind} credential is configured")
    credential = get_credential(int(selected_id))
    if credential["kind"] != kind:
        raise HTTPException(status_code=400, detail=f"Credential {selected_id} is not of type {kind}")
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
        "success_accounts_status_file": task_dir / "output" / "success_accounts_status.json",
        "cpamc_import_status_file": task_dir / "output" / "cpamc_import_status.json",
        "archive_path": archive_path,
    }


def _parse_success_account_line(platform: str, line: str) -> str | None:
    value = line.strip()
    if not value:
        return None
    if platform in {"chatgpt-register-v2", "chatgpt-register-v3", "browser-automation-local"}:
        parts = [part.strip() for part in value.split("----")]
        if len(parts) < 2:
            return None
        email, password = parts[0], parts[1]
        if "@" not in email or not password:
            return None
        return f"{email}----{password}"
    if platform == "grok-register":
        parts = [part.strip() for part in value.split(":")]
        if len(parts) < 2:
            return None
        email, password = parts[0], parts[1]
        if "@" not in email or not password:
            return None
        return f"{email}----{password}"
    if platform == "openai-register":
        if "----" in value:
            parts = [part.strip() for part in value.split("----")]
        else:
            parts = [part.strip() for part in value.split(":")]
        if len(parts) < 2:
            return None
        email, password = parts[0], parts[1]
        if "@" not in email or not password:
            return None
        return f"{email}----{password}"
    return None


def extract_success_accounts(task: sqlite3.Row | dict[str, Any]) -> Path | None:
    paths = task_paths(task)
    results_file = paths["results_file"]
    output_file = paths["success_accounts_file"]
    if not results_file.exists():
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text("", encoding="utf-8")
        return output_file
    extracted: list[str] = []
    seen: set[str] = set()
    platform = str(task["platform"])
    with results_file.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            value = _parse_success_account_line(platform, raw_line)
            if not value:
                continue
            if value in seen:
                continue
            seen.add(value)
            extracted.append(value)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(("\n".join(extracted) + "\n") if extracted else "", encoding="utf-8")
    return output_file


def load_success_accounts(task: sqlite3.Row | dict[str, Any]) -> list[tuple[str, str]]:
    path = extract_success_accounts(task)
    if path is None or not path.exists():
        return []
    accounts: list[tuple[str, str]] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or "----" not in line:
                continue
            email, password = [part.strip() for part in line.split("----", 1)]
            if not email or not password:
                continue
            key = f"{email}----{password}"
            if key in seen:
                continue
            seen.add(key)
            accounts.append((email, password))
    return accounts


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
    api_key: str = Field(min_length=1)
    base_url: str | None = None
    prefix: str | None = None
    domain: str | None = None
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
    time_of_day: str = Field(pattern=r"^\d{2}:\d{2}$")
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
        if credential["kind"] != "gptmail":
            raise HTTPException(status_code=400, detail="Original GPTMail credential is no longer valid")
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


def apply_email_credential_env(env: dict[str, str], credential_id: int | None) -> None:
    if not credential_id:
        return
    credential = get_credential(int(credential_id))
    env["MAIL_PROVIDER"] = "gptmail"
    env["GPTMAIL_API_KEY"] = credential["api_key"]
    if credential["base_url"]:
        env["GPTMAIL_BASE_URL"] = credential["base_url"]
    if credential["prefix"]:
        env["GPTMAIL_PREFIX"] = credential["prefix"]
    if credential["domain"]:
        env["GPTMAIL_DOMAIN"] = credential["domain"]


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
        paths = task_paths(task)
        task_dir = paths["task_dir"]
        task_dir.mkdir(parents=True, exist_ok=True)
        console_path = paths["console_path"]
        requested = json.loads(task["requested_config_json"])
        remaining_quantity = max(1, int(task["quantity"]) - count_result_lines(task))

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
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
            credential = get_credential(int(task["email_credential_id"]))
            output_dir = task_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            env["MAIL_PROVIDER"] = "gptmail"
            env["GPTMAIL_API_KEY"] = credential["api_key"]
            env["OUTPUT_FILE"] = str(output_dir / "registered_accounts.txt")
            env["AK_FILE"] = str(output_dir / "ak.txt")
            env["RK_FILE"] = str(output_dir / "rk.txt")
            env["TOKEN_JSON_DIR"] = str(output_dir / "tokens")
            if task["proxy"]:
                env["PROXY"] = str(task["proxy"])
            if credential["base_url"]:
                env["GPTMAIL_BASE_URL"] = credential["base_url"]
            if credential["prefix"]:
                env["GPTMAIL_PREFIX"] = credential["prefix"]
            if credential["domain"]:
                env["GPTMAIL_DOMAIN"] = credential["domain"]
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
            credential = get_credential(int(task["email_credential_id"]))
            output_dir = task_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            env["MAIL_PROVIDER"] = "gptmail"
            env["GPTMAIL_API_KEY"] = credential["api_key"]
            env["OUTPUT_FILE"] = str(output_dir / "registered_accounts.txt")
            env["AK_FILE"] = str(output_dir / "ak.txt")
            env["RK_FILE"] = str(output_dir / "rk.txt")
            env["TOKEN_JSON_DIR"] = str(output_dir / "tokens")
            if task["proxy"]:
                env["PROXY"] = str(task["proxy"])
            if credential["base_url"]:
                env["MAIL_BASE_URL"] = credential["base_url"]
            if credential["prefix"]:
                env["MAIL_PREFIX"] = credential["prefix"]
            if credential["domain"]:
                env["MAIL_DOMAIN"] = credential["domain"]
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
                started_at = ?,
                pid = ?,
                last_error = NULL
            WHERE id = ?
            """,
            (now_iso(), process.pid, int(task["id"])),
        )
        with self._lock:
            self._processes[int(task["id"])] = ManagedProcess(task_id=int(task["id"]), process=process, log_handle=log_handle)

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
        current_hm = now().strftime("%H:%M")
        today = now().strftime("%Y-%m-%d")
        for schedule in fetch_all("SELECT * FROM schedules WHERE enabled = 1 ORDER BY id ASC"):
            if str(schedule["time_of_day"]) != current_hm:
                continue
            if schedule["last_run_date"] == today:
                continue
            try:
                create_task_from_schedule(schedule, update_last_run_date=True)
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
        exit_error = None if exit_code == 0 else f"Task exited with code {exit_code}."
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
        return str(task["platform"]) in ("chatgpt-register-v2", "chatgpt-register-v3")

    def _maybe_auto_import_task(self, task: sqlite3.Row) -> None:
        if str(task["status"]) != "completed":
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
    if payload.default_gptmail_credential_id is not None and get_credential(payload.default_gptmail_credential_id)["kind"] != "gptmail":
        raise HTTPException(status_code=400, detail="Default GPTMail credential is invalid")
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
    if payload.kind not in {"gptmail", "yescaptcha"}:
        raise HTTPException(status_code=400, detail="Unsupported credential kind")
    timestamp = now_iso()
    credential_id = execute(
        """
        INSERT INTO credentials (name, kind, api_key, base_url, prefix, domain, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.name.strip(),
            payload.kind,
            payload.api_key.strip(),
            (payload.base_url or "").strip() or None,
            (payload.prefix or "").strip() or None,
            (payload.domain or "").strip() or None,
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
    accounts = dict(load_success_accounts(row))
    email = payload.email.strip()
    password = accounts.get(email)
    if not password:
        raise HTTPException(status_code=404, detail="The specified success account was not found in this task")
    result = regenerate_success_account_oauth_token(row, email, password)
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
    try:
        result = import_task_files_to_cpamc(task)
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
    timestamp = now_iso()
    schedule_id = execute(
        """
        INSERT INTO schedules (name, platform, quantity, concurrency, time_of_day, use_proxy, auto_import_cpamc, enabled, last_run_date, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
        """,
        (
            payload.name.strip(),
            payload.platform,
            payload.quantity,
            payload.concurrency,
            payload.time_of_day,
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
