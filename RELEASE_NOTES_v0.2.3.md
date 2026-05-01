# dbt-meta v0.2.3

**Release date:** 2026-05-01

A maintenance release focused on a long-standing UX gap in `meta validate` / `meta scan`, plus comprehensive doc and test improvements.

## Highlights

### `validate` and `scan` now degrade gracefully when compiled SQL is missing

Previously, `meta validate model --dev` would fail with `❌ No compiled SQL found in manifest` whenever the dev manifest came from `meta refresh --dev` (which runs `dbt parse`, not `dbt compile`). You had to manually run `dbt compile` first.

Now both commands fall back through three levels:

1. **Manifest** — `model['compiled_code']` (works after `dbt compile` / `dbt run`)
2. **Disk** — `target/compiled/{package}/{path}.sql` (works when `dbt compile` ran in a separate step)
3. **Auto-compile** — runs `dbt compile --select <model> --target dev` and re-reads from disk (only with `--dev`)

```bash
$ meta validate my_model --dev
ℹ️  No compiled SQL for 'my_model'. Running `dbt compile --select my_model --target dev`...
✅ Valid
```

If `dbt` isn't on PATH, compilation fails, or no `dbt_project.yml` is found, you get a clear actionable error suggesting the manual command — no silent failures.

## What's New

### Added
- 3-level compiled SQL fallback for `validate` and `scan` ([details](#validate-and-scan-now-degrade-gracefully-when-compiled-sql-is-missing))

### Fixed
- **JSON error output for all commands** — errors now always emit valid JSON when `-j` is used (previously could mix Rich output and JSON)
- **`bq` CLI not found when meta runs as installed package** — added 3-level discovery for `bq` binary location (current `PATH` → SDK locations → direct file check)

### Documentation
- Comprehensive doc audit — README, CLAUDE.md, and `meta --help` now reflect every command and flag
- New **Command Inventory** in CLAUDE.md (24 commands + subcommands, all flags, defaults)
- README **Commands Reference** rewritten with grouped tables and a "Key flags" column
- README + CLAUDE.md document the new compiled SQL fallback strategy

### Tests
- **Coverage: 91.30% → 95.86%** (756 → 784 tests, all passing)
- 10 modules now at 100% coverage including `validate.py`, `scan.py`, `hotspots.py`, `powerbi.py`, `monitoring.py`, `compiled_sql.py`

## Install / Upgrade

```bash
pip install --upgrade dbt-meta
```

## Compatibility

- Python 3.9, 3.10, 3.11, 3.12
- Backward-compatible — no API changes, no flag changes

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md#023---2026-05-01).
