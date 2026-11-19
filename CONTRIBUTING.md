# Contributing

Thanks for helping. This page covers the development setup, the test suites, and how releases are made.

## Setup

You need Python 3.11+ and [uv](https://docs.astral.sh/uv/). For the dashboard you also need
Node 22, and for the Postgres tests a Postgres server.

```bash
git clone https://github.com/sunilsk17/Ninelives && cd Ninelives
uv sync --all-extras
```

## Checks

Every change should pass these, and CI runs all of them:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy ninelives
uv run pytest                                  # SQLite backends
```

### Postgres

The store, engine and chaos tests also run against Postgres when `NINELIVES_TEST_POSTGRES_DSN` is
set. The tests truncate Ninelives' tables in that database, so use a throwaway one:

```bash
docker run -d --name nl-pg -e POSTGRES_PASSWORD=pg -e POSTGRES_DB=ninelives_test -p 5432:5432 postgres:16
NINELIVES_TEST_POSTGRES_DSN=postgresql://postgres:pg@localhost:5432/ninelives_test uv run pytest
```

### Chaos tests

`tests/test_chaos.py` starts real worker processes and kills them at random, which takes about 30
seconds. Skip it while iterating with `uv run pytest -m "not chaos"`.

### Dashboard

```bash
cd dashboard
npm ci
npm run dev       # hot-reloading UI; proxies /api to a server on :8000 (run `ninelives serve ... --worker`)
npm run build     # type-checks and writes the bundle into ninelives/server/static
npm run e2e       # Playwright tests against two live servers (needs `npx playwright install chromium`)
```

The built dashboard is committed under `ninelives/server/static`, so `pip install` users get it
without Node. Rebuild and commit it whenever you change `dashboard/src`; CI fails if the committed
build is out of date.

## Writing changes

- **Behaviour changes need tests.** Store behaviour belongs in the contract tests
  (`tests/test_store_contract.py`, `tests/test_store_v2.py`), which run on every backend.
- **Stores.** Store operations are transaction scripts in `ninelives/stores/sql.py`. Write SQL with
  `?` placeholders and keep it portable; the drivers handle dialect differences.
- **Schema changes** go in a new entry at the end of `MIGRATIONS` in `ninelives/stores/sql.py`.
  Never edit a released migration.
- **Changelog.** Add a line to `CHANGELOG.md` under an "Unreleased" heading.

## Releasing

1. Update `version` in `pyproject.toml` and `__version__` in `ninelives/__init__.py`, and move the
   changelog entries under the new version.
2. Commit, then tag: `git tag v0.2.0 && git push origin v0.2.0`.
3. The **Release** workflow builds the wheel and sdist, checks them, and attaches them to a GitHub
   release.
4. To publish to PyPI as well, add PyPI as a
   [trusted publisher](https://docs.pypi.org/trusted-publishers/) for this repository (workflow
   `release.yml`, environment `pypi`) and set the repository variable `PUBLISH_TO_PYPI` to `true`.
   From then on, every release tag also uploads to PyPI.
