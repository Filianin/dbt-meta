# CLAUDE.md

**dbt-meta** is an agent-first CLI (`meta`) that answers dbt metadata
questions from `manifest.json`: production manifest by default, fallback to the
dev manifest and then BigQuery, cached so a command costs ~10 ms.

## Where the truth lives

This file holds only what no single source file shows: rules that span
modules, and the change workflow. Everything else has one home:

- **Commands, flags, env vars, config keys** → `meta <cmd> -h` and the README
  (Commands Reference, Configuration).
- **How a command or algorithm works** (advisor weights, refresh propagation,
  hotspots scoring, lineage build, contour, run-set, Power BI compliance) →
  the module docstring of the file that implements it. Read it before editing
  the module, and update it in the same change.
- **History and breaking changes** → `CHANGELOG.md`.
- **Data-source decision tree** (prod / dev / BigQuery, 5 scenarios) →
  `.qa/decision_tree_visual.txt`, `.qa/data_source_logic.md`.

## Cross-module invariants

Each rule below is enforced across several files; breaking one silently
returns wrong metadata or slows every command.

1. **Parser cache.** Load a manifest only via `get_cached_parser(path)`
   (`utils/__init__.py`). A direct `ManifestParser(path)` skips the LRU cache:
   5–10 ms per command becomes 30–60 ms.
2. **Manifest discovery** (`manifest/finder.py`, `cli.get_manifest_path`):
   `--manifest PATH` → `DBT_DEV_MANIFEST_PATH` when `--dev` → otherwise
   `DBT_PROD_MANIFEST_PATH` → `./target/manifest.json` →
   `~/dbt-state/manifest.json`. `--manifest` wins over `--dev` (warning).
   `meta run-set` is dev-only and resolves `--manifest` → `Config.dev_manifest_path`.
3. **Table naming.** The prod manifest names a table by `config.alias`, the
   dev manifest by the SQL file name. `meta schema` and the BigQuery fallback
   depend on this.
4. **Fallback order** (`fallback.py`): prod manifest → dev manifest →
   BigQuery, toggled by `DBT_FALLBACK_TARGET` / `DBT_FALLBACK_BIGQUERY`.
   BigQuery fallback serves `schema`, `columns`, `info`, `config` only;
   `deps`, `sql`, `parents`, `children` are dbt-only.
5. **Schema follows the manifest that found the model.** Found in dev → dev
   schema (`personal_<user>`); found in prod → prod schema. Once a model is
   found in dev, prod is never searched again (`utils/state_detector.py`,
   `command_impl/column_source.py`).
6. **Catalog staleness is judged by file mtime.** `catalog.json` file older
   than 24 h → BigQuery fallback (CI is broken). Internal `generated_at`
   older than 7 days → info message only: the catalog is rewritten on every
   merge to master, so an old `generated_at` just means an unchanged schema
   (`command_impl/column_source.py`).
7. **Compiled SQL lookup** (`utils/compiled_sql.py:get_compiled_sql`):
   manifest `compiled_code` → `<project_root>/target/compiled/<package>/<path>`
   → `dbt compile --target dev` (only with `--dev`). The project root is found
   by walking up from the manifest to `dbt_project.yml`; `validate --dev` and
   `scan --dev` pass the **dev** manifest path so the lookup and compile run
   in the dev project.
8. **Dev schema** = `DBT_DEV_SCHEMA`, else `personal_<username>` from
   `DBT_USER` / `$USER`, sanitized with `re.sub(r'[^a-zA-Z0-9_]', '_', ...)`
   (BigQuery dataset names allow only `[a-zA-Z0-9_]`).
9. **Output shape contract.** `meta context` always emits the full key set;
   an empty value is the signal (`partition_by: null` = not partitioned,
   `description: ""` = undocumented). `meta list` is sparse: `meta` appears
   only when non-empty. Agents branch on values, never on key presence.
10. **One dependency graph, one "modified".** Transitive walks (ancestors,
    descendants, paths, build order) go through `manifest/graph.py::ManifestGraph`
    (`parser.graph(prefixes)` builds it once per cached parser); never re-walk
    `depends_on` / `child_map` by hand. What changed on a branch is
    `manifest/changes.py::detect_modified`, shared by `run-set` and
    `optimize refresh -m`.

## Adding or changing a command

1. Logic in `command_impl/<name>.py`, manifest via `get_cached_parser`,
   module docstring describing the algorithm and its invariants.
2. Typer command in `cli.py` with `@meta_command` and `-j/--json`; add rows to
   `_build_commands_panel()` and `_build_examples_panel()`.
3. Tests in `tests/test_<name>.py`; CLI paths via `typer.testing.CliRunner`.
4. Keep four surfaces in sync: `meta <cmd> -h`, README Commands Reference,
   `CHANGELOG.md` top entry, and this file when a cross-module rule changes.

## Done gate

```bash
pytest --cov=dbt_meta          # coverage bar 90% (pyproject.toml)
mypy src/dbt_meta              # strict
ruff check src/dbt_meta
```

- Run `ruff format` on new files only. Most of the repo predates the
  formatter, and formatting an existing file rewrites every line.
- `cli.py` and `manifest/finder.py` are excluded from coverage. Test them
  through `CliRunner` anyway.
- If pytest-cov fails with "Can't combine statement coverage data with branch
  data", delete the stale `.coverage*` files and rerun.
- Release: bump the version in both `pyproject.toml` and
  `src/dbt_meta/__init__.py`, add the CHANGELOG entry, and tag `v0.x.y`.
