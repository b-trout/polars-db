# ADR-0010: Devcontainer with Docker-in-Docker

## Status

Accepted

## Context

Integration tests require running database containers (PostgreSQL, MySQL, SQL Server, BigQuery emulator). The development environment runs inside a devcontainer, which itself is a Docker container. Running `docker compose up` from inside a container requires Docker-in-Docker (DinD) capability.

## Decision

Add the Docker-in-Docker devcontainer feature with official Docker CE:

```json
{
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {
      "moby": false
    }
  }
}
```

- `moby: false` uses official Docker CE instead of the Moby open-source variant, which has better compatibility with `docker compose` v2.
- Build context is set to `..` so the Dockerfile can reference project root files.

## Consequences

- Developers can run `docker compose up -d --wait postgres` directly inside the devcontainer.
- The same `docker-compose.yml` works for both local development and CI.
- Docker socket is available inside the devcontainer without host Docker socket mounting.
- Trade-off: DinD adds ~200MB to the devcontainer image and has slightly slower container startup compared to host Docker socket mounting.
