#!/usr/bin/env python3
"""Wait for a backend to become ready."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 30
    interval_sec: float = 2.0


@dataclass(frozen=True)
class AttemptResult:
    success: bool
    attempt: int
    message: str


def try_connect(backend: str, config: dict[str, str]) -> AttemptResult:
    """Single connection attempt."""
    import polars_db as pdb

    try:
        conn = pdb.connect(**config)
        conn.execute_raw("SELECT 1")
        conn.close()
    except Exception as e:
        return AttemptResult(success=False, attempt=0, message=str(e))
    else:
        return AttemptResult(success=True, attempt=0, message="Connection OK")


def wait_for_ready(
    backend: str,
    config: dict[str, str],
    retry: RetryConfig | None = None,
) -> AttemptResult:
    """Retry loop until the backend is ready."""
    if retry is None:
        retry = RetryConfig()
    for attempt in range(1, retry.max_attempts + 1):
        result = try_connect(backend, config)
        if result.success:
            print(f"Backend {backend} ready after {attempt} attempts")
            return AttemptResult(success=True, attempt=attempt, message=result.message)
        print(f"Attempt {attempt}: {result.message}")
        time.sleep(retry.interval_sec)

    return AttemptResult(
        success=False,
        attempt=retry.max_attempts,
        message=f"Backend {backend} failed to become ready",
    )


def main() -> None:
    backend = sys.argv[1]

    sys.path.insert(0, ".")
    from tests.conftest import BACKEND_CONFIG

    config = dict(BACKEND_CONFIG[backend])
    result = wait_for_ready(backend, config)
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
