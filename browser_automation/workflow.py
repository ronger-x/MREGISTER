from __future__ import annotations

import time

from chatgpt_register_v3.lib.utils import generate_random_password

from .browser import BrowserManager
from .config import load_workflow_config
from .context import RunContext
from .mail import EmailClient, mask_email
from .page import click_element, input_text, wait_for_url_contains
from .result import RunResult
from .screenshots import save_screenshot


def _capture(tab, context: RunContext, config, stage: str) -> str | None:
    if not config.capture_screenshots:
        return None
    try:
        return str(save_screenshot(tab, context.run_dir, stage))
    except Exception:
        return None


def run_generic_form_flow(context: RunContext, config, email_client: EmailClient, generated_email, metadata: dict[str, object]) -> tuple[str, str, dict[str, object]]:
    browser_manager = BrowserManager(context)
    browser = None
    screenshots: dict[str, str] = {}
    try:
        browser = browser_manager.init_browser()
        tab = browser.latest_tab
        if config.start_url:
            tab.get(config.start_url)
            time.sleep(config.page_wait_seconds)
            shot = _capture(tab, context, config, "landing")
            if shot:
                screenshots["landing"] = shot

        if config.cookie_accept_selectors:
            clicked = click_element(tab, config.cookie_accept_selectors, timeout=2)
            metadata["cookie_accept_status"] = "clicked" if clicked else "skipped"
            time.sleep(1)

        if config.step1_button_selectors:
            clicked = click_element(tab, config.step1_button_selectors, timeout=5)
            metadata["step1_click_status"] = "clicked" if clicked else "not-found"
            time.sleep(config.page_wait_seconds)
            shot = _capture(tab, context, config, "after-step1")
            if shot:
                screenshots["after_step1"] = shot

        if config.step2_button_selectors:
            clicked = click_element(tab, config.step2_button_selectors, timeout=5)
            metadata["step2_click_status"] = "clicked" if clicked else "not-found"
            time.sleep(config.page_wait_seconds)
            shot = _capture(tab, context, config, "after-step2")
            if shot:
                screenshots["after_step2"] = shot

        if generated_email and config.email_input_selectors:
            if not input_text(tab, config.email_input_selectors, generated_email.address, timeout=5):
                metadata["email_input_status"] = "not-found"
                return "partial", "Generic flow started, but the email input was not found.", screenshots
            metadata["email_input_status"] = "filled"
            shot = _capture(tab, context, config, "email-entered")
            if shot:
                screenshots["email_entered"] = shot

        if config.continue_button_selectors:
            clicked = click_element(tab, config.continue_button_selectors, timeout=5)
            metadata["continue_click_status"] = "clicked" if clicked else "not-found"
            time.sleep(config.page_wait_seconds)
            shot = _capture(tab, context, config, "after-continue")
            if shot:
                screenshots["after_continue"] = shot

        generated_password = metadata.get("generated_password")
        if generated_password and config.password_input_selectors:
            filled = input_text(tab, config.password_input_selectors, str(generated_password), timeout=5)
            metadata["password_input_status"] = "filled" if filled else "not-found"
            if filled:
                shot = _capture(tab, context, config, "password-entered")
                if shot:
                    screenshots["password_entered"] = shot

        if generated_password and config.password_button_selectors:
            clicked = click_element(tab, config.password_button_selectors, timeout=5)
            metadata["password_submit_status"] = "clicked" if clicked else "not-found"
            time.sleep(config.page_wait_seconds)
            shot = _capture(tab, context, config, "after-password-submit")
            if shot:
                screenshots["after_password_submit"] = shot

        if generated_email and config.code_input_selectors and email_client.is_configured():
            code = email_client.get_verification_code(generated_email, timeout=config.code_timeout)
            metadata["verification_code_received"] = bool(code)
            if code:
                if input_text(tab, config.code_input_selectors, code, timeout=5):
                    metadata["code_input_status"] = "filled"
                    shot = _capture(tab, context, config, "code-entered")
                    if shot:
                        screenshots["code_entered"] = shot
                    if config.verify_button_selectors:
                        clicked = click_element(tab, config.verify_button_selectors, timeout=5)
                        metadata["verify_click_status"] = "clicked" if clicked else "not-found"
                        time.sleep(config.page_wait_seconds)
                        shot = _capture(tab, context, config, "after-verify")
                        if shot:
                            screenshots["after_verify"] = shot
                else:
                    metadata["code_input_status"] = "not-found"

        if generated_email and config.display_name_input_selectors:
            display_name = generated_email.address.split("@", 1)[0]
            display_name = "".join(ch for ch in display_name if not ch.isdigit()) or display_name
            filled = input_text(tab, config.display_name_input_selectors, display_name, timeout=5)
            metadata["display_name_status"] = "filled" if filled else "not-found"
            if filled:
                shot = _capture(tab, context, config, "display-name-entered")
                if shot:
                    screenshots["display_name_entered"] = shot

        if config.display_name_button_selectors:
            clicked = click_element(tab, config.display_name_button_selectors, timeout=5)
            metadata["display_name_submit_status"] = "clicked" if clicked else "not-found"
            time.sleep(config.page_wait_seconds)
            shot = _capture(tab, context, config, "after-display-name-submit")
            if shot:
                screenshots["after_display_name_submit"] = shot

        current_url = str(tab.url or "")
        if config.success_url_contains:
            current_url = wait_for_url_contains(tab, config.success_url_contains, timeout=10, interval=1)
            metadata["success_url_matched"] = any(fragment in current_url for fragment in config.success_url_contains)
        metadata["final_url"] = current_url
        if config.success_url_contains and any(fragment in current_url for fragment in config.success_url_contains):
            return "completed", "Generic browser workflow completed and matched the success URL rule.", screenshots
        return "partial", "Generic browser workflow executed. Review metadata and screenshots to continue refining selectors.", screenshots
    finally:
        browser_manager.quit()


def run_task(context: RunContext) -> RunResult:
    context.run_dir.mkdir(parents=True, exist_ok=True)
    context.output_dir.mkdir(parents=True, exist_ok=True)
    (context.run_dir / "screenshots").mkdir(parents=True, exist_ok=True)

    workflow_config = load_workflow_config(context)
    email_client = EmailClient(context)
    generated_email = None
    if email_client.is_configured():
        generated_email = email_client.generate_email()
    generated_password = generate_random_password() if generated_email else None

    user_agent = None
    browser_manager = BrowserManager(context)
    try:
        user_agent = browser_manager.get_user_agent()
    except Exception:
        user_agent = None

    message = "Template workflow executed. Replace browser_automation.workflow.run_task with your local browser automation implementation."
    metadata = {
        "run_index": context.run_index,
        "project_dir": str(context.project_dir),
        "task_dir": str(context.task_dir),
        "output_dir": str(context.output_dir),
        "run_dir": str(context.run_dir),
        "results_file": str(context.results_file),
        "proxy": context.proxy,
        "headless": context.headless,
        "browser_path": context.browser_path,
        "extra_args": context.extra_args,
        "extra_env": context.extra_env,
        "mail_provider": context.mail_provider,
        "mail_base_url": context.mail_base_url,
        "mail_prefix": context.mail_prefix,
        "mail_domain": context.mail_domain,
        "generated_email": mask_email(generated_email.address) if generated_email else None,
        "email_configured": email_client.is_configured(),
        "user_agent": user_agent,
        "workflow": {
            "start_url": workflow_config.start_url,
            "cookie_accept_selectors": workflow_config.cookie_accept_selectors,
            "step1_button_selectors": workflow_config.step1_button_selectors,
            "step2_button_selectors": workflow_config.step2_button_selectors,
            "email_input_selectors": workflow_config.email_input_selectors,
            "continue_button_selectors": workflow_config.continue_button_selectors,
            "password_input_selectors": workflow_config.password_input_selectors,
            "password_button_selectors": workflow_config.password_button_selectors,
            "code_input_selectors": workflow_config.code_input_selectors,
            "verify_button_selectors": workflow_config.verify_button_selectors,
            "display_name_input_selectors": workflow_config.display_name_input_selectors,
            "display_name_button_selectors": workflow_config.display_name_button_selectors,
            "success_url_contains": workflow_config.success_url_contains,
            "code_timeout": workflow_config.code_timeout,
            "capture_screenshots": workflow_config.capture_screenshots,
        },
        "timestamp": int(time.time()),
    }
    if workflow_config.start_url:
        metadata["generated_password"] = generated_password if generated_password else None
        status, message, screenshots = run_generic_form_flow(context, workflow_config, email_client, generated_email, metadata)
        metadata["screenshots"] = screenshots
        if status == "completed" and generated_email and generated_password:
            metadata["successful_account"] = {
                "email": generated_email.address,
                "password": generated_password,
            }
        return RunResult(success=status in {"completed", "partial"}, status=status, message=message, metadata=metadata)
    return RunResult(success=True, status="template-ready", message=message, metadata=metadata)
