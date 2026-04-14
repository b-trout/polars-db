#!/usr/bin/env python3
"""Start a docker compose service."""

from __future__ import annotations

import subprocess
import sys


def _build_command(service: str, profile: str | None) -> tuple[str, ...]:
    base = ("docker", "compose")
    profile_args = ("--profile", profile) if profile else ()
    return (*base, *profile_args, "up", "-d", "--wait", service)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    cmd = _build_command(args.service, args.profile)
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
