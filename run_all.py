import asyncio
import os
import sys


MONITORS = [
    ("ashby", "ashby.py"),
    ("greenhouse", "greenhouse.py"),
    ("lever", "lever.py"),
]


async def pump_output(name: str, stream: asyncio.StreamReader) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        print(f"[{name}] {line.decode(errors='replace').rstrip()}", flush=True)


async def run_monitor(name: str, script: str) -> int:
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
        return await process.wait()
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()


async def main() -> None:
    tasks = [asyncio.create_task(run_monitor(name, script)) for name, script in MONITORS]
    results = await asyncio.gather(*tasks)
    if any(code != 0 for code in results):
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
