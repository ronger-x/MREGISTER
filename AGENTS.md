# MREGISTER Agent Notes

## Scope

- This repository contains `web_console`, `chatgpt_register_v2`, `chatgpt_register_v3`, `browser_automation`, and `grok-register`.
- Prefer minimal, targeted changes. Do not revert unrelated user changes in a dirty worktree.

## Encoding Rules

- Use `UTF-8` for source files, config files, JSON, TXT outputs, and console/log processing.
- Do not rely on `GBK`, ANSI, or locale-default encodings.
- When reading or writing text files in Python, explicitly pass `encoding="utf-8"` when practical.
- On Windows, prefer running with:
  - PowerShell: `$env:PYTHONUTF8="1"; $env:PYTHONIOENCODING="utf-8"`
  - CMD: `set PYTHONUTF8=1` and `set PYTHONIOENCODING=utf-8`

## Mail Provider Rules

- `mail.tm` is not GPTMail.
- If provider is `mailtm` and no Base URL is configured, default to `https://api.mail.tm`.
- Do not fall back from generic `mail_*` config to `gptmail_*` defaults unless the active provider is actually `gptmail`.
- For providers that require mailbox credentials for later inbox access or token refresh, preserve those credentials in persisted success-account data.

## Success Account Persistence

- Successful account records may include:
  - OpenAI account email
  - OpenAI account password
  - mailbox credential
  - provider
  - status
  - timestamp
- Keep backward compatibility with older flat-text account formats when possible.

## Web Console Runtime

- Task subprocesses should run with UTF-8-friendly environment settings.
- Be careful when changing credential propagation from `web_console/app.py` into task environments.
- Changes affecting task output formats should preserve existing task listing and success-account regeneration flows.

## Validation

- After Python changes, run at least `python -m py_compile` on touched modules when feasible.
- If behavior depends on provider selection, verify the effective provider and resolved API base URL with a minimal smoke test.
