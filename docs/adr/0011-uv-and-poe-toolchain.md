# ADR-0011: uv + poethepoet Toolchain

## Status

Accepted (diverges from initial design)

## Context

The initial design specified `pip` for package management and direct CLI commands for task execution. Modern Python tooling offers faster, more reproducible alternatives.

## Decision

Adopt `uv` as the package manager and `poethepoet` (poe) as the task runner:

- **uv** — 10-100x faster than pip for dependency resolution and installation. Provides `uv sync`, `uv add`, `uv remove` with lockfile support.
- **poethepoet** — Task definitions in `pyproject.toml` for consistent command execution across environments.

### Pre-commit Hooks

The following checks run on every commit via pre-commit:

1. `ruff format` — Code formatting
2. `ruff check --fix` — Linting with auto-fix
3. `ty check` — Type checking (replaces mypy from original design)
4. `yamlfix` — YAML formatting
5. `yamllint` — YAML linting
6. `hadolint` — Dockerfile linting

### Type Checker Change

The original design specified `mypy --strict`. The implementation uses `ty` (from the ruff/astral ecosystem) instead:

- Faster execution
- Better integration with the ruff toolchain
- Configured in `pyproject.toml` under `[tool.ty.rules]`

## Consequences

- `uv sync --all-extras` installs all dependencies in ~2 seconds (vs ~30s with pip).
- `poe ci` runs the full CI pipeline locally with a single command.
- All developers use identical tool versions via `uv.lock`.
- Trade-off: `ty` is newer and less mature than mypy. Some sqlglot dynamic patterns require `unresolved-attribute = "warn"`.
