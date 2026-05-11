import asyncio
import json
import os
import sys
import time

from dotenv import load_dotenv

from structured_logging import configure_logger, log_event


load_dotenv(".env.local")
LOGGER = configure_logger("runner")

MONITORS = [
    ("ashby", "ashby.py"),
    ("greenhouse", "greenhouse.py"),
    ("lever", "lever.py"),
]

RESTART_BASE_SECONDS = float(os.getenv("MONITOR_RESTART_BASE_SECONDS", "5"))
RESTART_MAX_SECONDS = float(os.getenv("MONITOR_RESTART_MAX_SECONDS", "120"))


async def pump_output(name: str, stream: asyncio.StreamReader) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        text = line.decode(errors="replace").rstrip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            log_event(LOGGER, "subprocess_output", child_service=name, message=text)
            continue

        if isinstance(parsed, dict):
            sys.stdout.write(text + "\n")
            sys.stdout.flush()
        else:
            log_event(LOGGER, "subprocess_output", child_service=name, message=text)


async def run_monitor(name: str, script: str) -> int:
    log_event(LOGGER, "monitor_process_starting", child_service=name, script=script)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=os.environ.copy(),
    )
    assert process.stdout is not None
    try:
        await pump_output(name, process.stdout)
        code = await process.wait()
        log_event(
            LOGGER,
            "monitor_process_exited",
            level="ERROR" if code else "INFO",
            child_service=name,
            exit_code=code,
        )
        return code
    finally:
        if process.returncode is None:
            log_event(LOGGER, "monitor_process_terminating", level="WARNING", child_service=name)
            process.terminate()
            await process.wait()


async def supervise_monitor(name: str, script: str) -> None:
    restart_count = 0
    while True:
        started = time.monotonic()
        try:
            code = await run_monitor(name, script)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            code = 1
            log_event(LOGGER, "monitor_supervisor_error", level="ERROR", child_service=name, error=str(e))

        ran_for = time.monotonic() - started
        if ran_for > 300:
            restart_count = 0
        else:
            restart_count += 1

        delay = min(RESTART_MAX_SECONDS, RESTART_BASE_SECONDS * (2 ** min(restart_count, 6)))
        log_event(
            LOGGER,
            "monitor_process_restarting",
            level="WARNING" if code else "INFO",
            child_service=name,
            exit_code=code,
            restart_count=restart_count,
            ran_for_seconds=round(ran_for, 2),
            restart_delay_seconds=delay,
        )
        await asyncio.sleep(delay)


async def main() -> None:
    tasks = [asyncio.create_task(supervise_monitor(name, script)) for name, script in MONITORS]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_event(LOGGER, "monitor_group_stopped")
        raise SystemExit(130)
