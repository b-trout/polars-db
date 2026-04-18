# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2026-04-18

### ⚠ Breaking Changes

- `pdb.connect(...)` no longer auto-creates SQL Server databases. Pass `create_if_missing=True` explicitly to restore the pre-0.1.1 behavior. Prevents accidental database creation from typos in production connection strings — see [ADR-0013](docs/adr/0013-sqlserver-auto-database-creation.md) for the rationale.
- SQLite now requires the `sqlite` extra (`pip install polars-db[sqlite]`) since the backend has been migrated to the `adbc-driver-sqlite` driver.

### Bug Fixes

- Alias JoinValidator subquery so MySQL/T-SQL accept the generated SQL([e042492](https://github.com/b-trout/polars-db/commit/e042492cd63ee89bf86ffe95410722b50a979d42))
- Emit explicit ON condition for T-SQL JOIN (USING is unsupported)([a87bece](https://github.com/b-trout/polars-db/commit/a87becedb9762478faa06636bc4d679be7738653))
- Filter schema_query by current database/schema to prevent cross-schema column bleed([9f18a52](https://github.com/b-trout/polars-db/commit/9f18a52c90776e27697a76f369a1c653ca5bf259))
- Suffix duplicate columns on JOIN to prevent ambiguous references([99c7059](https://github.com/b-trout/polars-db/commit/99c705941da4ff08c9cd4084f5610e3d94cadfdf))
- Use AST builder for BigQuery schema_query to prevent injection([63758cf](https://github.com/b-trout/polars-db/commit/63758cf95c1d584b765dd2f3423959d5f027d88d))
- Validate SQL Server database identifier to prevent injection([3dc3073](https://github.com/b-trout/polars-db/commit/3dc3073b0deb4af85260536b37bd6456aec04bb7))
- Unset core.hooksPath before pre-commit install in devcontainer([9644f13](https://github.com/b-trout/polars-db/commit/9644f13f72c5c3553d2496ad1969f2ac6fa65cd0))

### Miscellaneous

- Remove unused _is_select_star helper and document Postgres thread-safety([7634d7e](https://github.com/b-trout/polars-db/commit/7634d7e45459235437554914b296508182f2c717))
- Remove unused _compile_join_on_same method([9ada191](https://github.com/b-trout/polars-db/commit/9ada191ee8a9339b4f322f2c1e316797e2427269))

### Performance

- Migrate PostgreSQL and SQLite backends to ADBC for native Arrow transport([7ce68f8](https://github.com/b-trout/polars-db/commit/7ce68f820b27b22e132d336da987566d988926ec))

### Refactor

- Make SQL Server database auto-creation opt-in via create_if_missing([98668ab](https://github.com/b-trout/polars-db/commit/98668aba8248c8c1cae76bdb34cf2b17948f5b16))
- Apply optimizer passes to fixpoint and remove dead mutation([d9e5321](https://github.com/b-trout/polars-db/commit/d9e5321997b2258834591d87670983fb6a893d50))

### Testing

- Expose fake dbapi as attribute on parent adbc mock([0e5c55b](https://github.com/b-trout/polars-db/commit/0e5c55be2ea6f42378ade387faabcfa61e4d43b4))
- Patch parent adbc_driver_postgresql in sys.modules for unit test([2d05e41](https://github.com/b-trout/polars-db/commit/2d05e41586c02ed5e2c4b61c4a48567f3203932e))
- Add integration tests for JOIN across 6 backends([4c609dd](https://github.com/b-trout/polars-db/commit/4c609dd24c96d2a1e12f4d7e806b4492f9c13166))
- Accept Decimal return from Postgres EXTRACT and xfail SQLite dt.*([656a62a](https://github.com/b-trout/polars-db/commit/656a62afbc49cd00aa3defa0ccdbbb269165e303))
- Add integration tests for case-when, namespaces, join validator, and misc expressions([8668d95](https://github.com/b-trout/polars-db/commit/8668d95530577f527aaa455ef66aa84128a942d6))

### Merge

- Sync with main to pick up JoinValidator fix (#48)([0b5567d](https://github.com/b-trout/polars-db/commit/0b5567d9aa1e0f2986337516e40aa52e851d0450))
- Sync with main to pick up bug fixes (#45, #46)([9440855](https://github.com/b-trout/polars-db/commit/94408559bc7ae76759c69b62ca42e5295dc97b8c))
- Sync with main to pick up bug fixes (#45, #46)([0104674](https://github.com/b-trout/polars-db/commit/0104674b89d4557e79a2bef9262536c3d237b0cb))

## [0.1.0] - 2026-04-18

### Bug Fixes

- Resolve MySQL schema case sensitivity and SQL Server ORDER BY requirement([d232d32](https://github.com/b-trout/polars-db/commit/d232d32a03d83be2ad36ce1e4ef5225299d865a1))
- Add aliases to derived tables in SQL compiler([d459c88](https://github.com/b-trout/polars-db/commit/d459c882d96af5fa1148992ee98703ed48c69c65))
- Rewrite PostgreSQL backend to use native psycopg2 driver([2dd4856](https://github.com/b-trout/polars-db/commit/2dd48562305fbed84f217e387c6b2b6be43aba1a))
- Rewrite DuckDB and SQLite backends to use native drivers([6e4d47a](https://github.com/b-trout/polars-db/commit/6e4d47a2049e391058802b9280a2441063b84d18))

### CI/CD

- Add release pipeline with tag-based versioning and CHANGELOG([1b3e82d](https://github.com/b-trout/polars-db/commit/1b3e82dbcfd8432630c481d9d1d0151f13f056e7))
- Integrate Codecov for coverage reporting([311eeb0](https://github.com/b-trout/polars-db/commit/311eeb022d6900df7f90fedeb26032d0d90ff0f3))
- Add 3-stage pipeline with per-backend integration tests (#23)([7037d8b](https://github.com/b-trout/polars-db/commit/7037d8b7b25c8216ca17b410ad8dd8d88ccc4383))
- Add GitHub Actions PR check workflow using poe ci([b2d2b28](https://github.com/b-trout/polars-db/commit/b2d2b28110bdd4dd0022dade0c988c3be7ab084f))

### Documentation

- Add CI, Python version, and license badges to README([6800b82](https://github.com/b-trout/polars-db/commit/6800b82cc56a1055bbf21d182777939da7704fdf))
- Add cumulative window function example to README([496ebe3](https://github.com/b-trout/polars-db/commit/496ebe3060d404b14af984d09aebdcd84bb7e5b6))
- Add motivation section to README([308c96f](https://github.com/b-trout/polars-db/commit/308c96f81c973962fadc11e129384393271a199d))
- Add comprehensive README with install, usage, and dev guide([bc7c526](https://github.com/b-trout/polars-db/commit/bc7c526fa2f46919cf0a2b6bdba4dc0b4cbcb391))
- Add 14 Architecture Decision Records (ADRs)([ba65ce7](https://github.com/b-trout/polars-db/commit/ba65ce716c6e52ca7a35755d6e59fe8e71417b7b))
- Add NumPy-style docstrings to all test classes and methods([5ed103c](https://github.com/b-trout/polars-db/commit/5ed103c1db2dbc4d6985d5d41d769edec4a4b0b4))
- Add pull request template([c95ea8b](https://github.com/b-trout/polars-db/commit/c95ea8b243aa637b9eb832e06791983b9b5ad28f))

### Features

- Add explicit frame specification to over() API([b924567](https://github.com/b-trout/polars-db/commit/b9245673073adf4be1afb8cb6ba1703093902ef2))
- Add order_by to over(), dense_rank, and cumulative window functions([b299797](https://github.com/b-trout/polars-db/commit/b299797b4a34cf2b326d3ba8d9df4e90ce720a99))
- Add BigQuery emulator integration tests([c61d66c](https://github.com/b-trout/polars-db/commit/c61d66cbfc4edffedca4b0db66c55d27214484b3))
- Remove connectorx and rewrite all backends to use native drivers([0c070b6](https://github.com/b-trout/polars-db/commit/0c070b6cc76e523bb4d8ad04aeb6eb03860316dc))
- Configure Docker-in-Docker build context and use official Docker engine([3a2cb28](https://github.com/b-trout/polars-db/commit/3a2cb2895938a3b7837ebf93d75dc711860e097a))
- Add Docker-in-Docker feature to devcontainer([7fedf86](https://github.com/b-trout/polars-db/commit/7fedf864aba879881b56d36209e7ac0279720efb))
- Add type mapping and namespace function compilation([ad21c9c](https://github.com/b-trout/polars-db/commit/ad21c9c8b6902b01e0b9afffe0346cf3256a75c6))
- Add integration test infrastructure([368a4cb](https://github.com/b-trout/polars-db/commit/368a4cbe91c4edc8937058c41d5cf106a685b0ad))
- Add DuckDB, MySQL, SQLite, SQL Server, and BigQuery backends([cd06292](https://github.com/b-trout/polars-db/commit/cd062925ba44292d17a8900a1a7ee2dec6916d76))
- Add window function tests and fix shift/lead compilation([8acc431](https://github.com/b-trout/polars-db/commit/8acc431dda013e36dfc4123064660d0663c27707))
- Implement Optimizer and JoinValidator with tests([06f4eeb](https://github.com/b-trout/polars-db/commit/06f4eeb06ce0923b3792d3d09387a176244c14c4))
- Add Connection, LazyFrame, public API, and unit tests([2600258](https://github.com/b-trout/polars-db/commit/2600258a0c37f629d0297a515a632359c0581e9b))
- Add Connection, LazyFrame, types stub, and public API([94a5880](https://github.com/b-trout/polars-db/commit/94a5880d6cc2286ae4b3e871ecaf96a470487aec))
- Add Backend ABC, ExprCompiler, QueryCompiler, and Optimizer([e1ec7b3](https://github.com/b-trout/polars-db/commit/e1ec7b3f64a457d5bb08a00bb1d1752d912b4a28))
- Add Expr AST, Op tree nodes, and exception hierarchy([fce4f4a](https://github.com/b-trout/polars-db/commit/fce4f4a011eb39220d370b379674f81baac55713))
- Add connectorx, pytest plugins, and test markers to project config([43b1f34](https://github.com/b-trout/polars-db/commit/43b1f34de00a14b17d620319db34666bf048483c))
- Add SSH keys bind mount to devcontainer([ce65087](https://github.com/b-trout/polars-db/commit/ce6508701afe6af217f2cbe19412b704e8f763f9))
- Add .claude.json bind mount to devcontainer([5543218](https://github.com/b-trout/polars-db/commit/5543218011e86f73ba18828d23a9e42707144dc7))
- Add devcontainer configuration([fd2db15](https://github.com/b-trout/polars-db/commit/fd2db1598b5d859ebffb02af9134ebbfa0d718a2))
- Add Dockerfile and package structure([eb5bc85](https://github.com/b-trout/polars-db/commit/eb5bc85ecee8a6ac819f55536b6fc5c0f7107448))

### Miscellaneous

- Add PyPI metadata, classifiers, and py.typed marker([fc779d5](https://github.com/b-trout/polars-db/commit/fc779d55c9ba4c58ac37593bf81c36f2d962bc06))
- Add dev tooling configuration([22a4e27](https://github.com/b-trout/polars-db/commit/22a4e27d3fc76a77cb42b648e04cbdfe6780e3e8))
- Update .gitignore([5f53340](https://github.com/b-trout/polars-db/commit/5f53340ec71cb0ee8a08a5ed3a398d939b9424ae))

### Testing

- Add edge case integration tests for window functions([992dc61](https://github.com/b-trout/polars-db/commit/992dc61d197d1f3bd9d10f78f43e47e0f12bbb95))

### Merge

- Resolve test_window.py conflict with main([bbed200](https://github.com/b-trout/polars-db/commit/bbed200fa5229cd1ffe6b285ec676098a2d07204))

