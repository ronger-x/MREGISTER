from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def append_result(result_path: Path, value: str) -> None:
    result_path.parent.mkdir(parents=True, exist_ok=True)
    with result_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{value}\n")


def load_success_record(run_dir: Path) -> str | None:
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        return None
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    account = payload.get("metadata", {}).get("successful_account")
    if not isinstance(account, dict):
        return None
    email = str(account.get("email") or "").strip()
    password = str(account.get("password") or "").strip()
    if not email or not password:
        return None
    return f"{email}----{password}"


def run_adapter(*, index: int, project_dir: Path, adapter_path: Path, task_dir: Path, results_file: Path, proxy: str | None) -> tuple[bool, str]:
    run_dir = task_dir / "runs" / f"run_{index:04d}"
    run_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["MREGISTER_RUN_INDEX"] = str(index)
    env["MREGISTER_RUN_DIR"] = str(run_dir)
    env["MREGISTER_TASK_DIR"] = str(task_dir)
    env["MREGISTER_OUTPUT_DIR"] = str(task_dir / "output")
    env["MREGISTER_RESULTS_FILE"] = str(results_file)
    env["MREGISTER_PROJECT_DIR"] = str(project_dir)
    if proxy:
        env["MREGISTER_PROXY"] = proxy
        env["BROWSER_PROXY"] = proxy

    print(f"[browser-automation] starting run {index} using {adapter_path}")
    completed = subprocess.run(
        [sys.executable, str(adapter_path)],
        cwd=str(project_dir),
        env=env,
        text=True,
        capture_output=True,
    )

    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n")
    if completed.stderr:
        print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n")

    if completed.returncode == 0:
        append_result(results_file, load_success_record(run_dir) or f"run-{index:04d}")
        return True, f"run {index} completed"
    return False, f"run {index} failed with exit code {completed.returncode}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generic browser automation task wrapper for MREGISTER")
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--adapter", required=True)
    parser.add_argument("--task-dir", required=True)
    parser.add_argument("--quantity", type=int, required=True)
    parser.add_argument("--workers", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_dir = Path(args.project_dir).resolve()
    adapter_path = Path(args.adapter).resolve()
    task_dir = Path(args.task_dir).resolve()
    results_file = task_dir / "output" / "results.txt"
    proxy = os.environ.get("MREGISTER_PROXY", "").strip() or None

    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "output").mkdir(parents=True, exist_ok=True)
    (task_dir / "runs").mkdir(parents=True, exist_ok=True)

    if not project_dir.exists():
        print(f"[browser-automation] project directory not found: {project_dir}")
        return 1
    if not adapter_path.exists():
        print(f"[browser-automation] adapter not found: {adapter_path}")
        print("[browser-automation] create the adapter file and implement your local browser workflow there.")
        return 1

    quantity = max(1, int(args.quantity))
    workers = max(1, int(args.workers))
    print(f"[browser-automation] project={project_dir}")
    print(f"[browser-automation] adapter={adapter_path}")
    print(f"[browser-automation] quantity={quantity} workers={workers}")

    success_count = 0
    failure_count = 0
    lock = threading.Lock()

    def run_one(index: int) -> tuple[bool, str]:
        return run_adapter(
            index=index,
            project_dir=project_dir,
            adapter_path=adapter_path,
            task_dir=task_dir,
            results_file=results_file,
            proxy=proxy,
        )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(run_one, index): index for index in range(1, quantity + 1)}
        for future in as_completed(futures):
            ok, message = future.result()
            with lock:
                if ok:
                    success_count += 1
                else:
                    failure_count += 1
            print(f"[browser-automation] {message}")

    print(f"[browser-automation] finished: success={success_count} failed={failure_count}")
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
