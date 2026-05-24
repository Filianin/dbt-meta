# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-05-24

Major release: column-level lineage, on-demand optimization advisors, and a
mandatory compiled-SQL pre-flight that auto-runs `dbt compile` when the
loaded manifest is from `dbt parse` (no Jinja-rendered SQL) and would
otherwise produce empty / misleading recommendations.

### Added

#### Column-level lineage (`meta lineage`)
- New subcommand group backed by SQLGlot 30.7+ (`sqlglot.lineage` all-columns mode) and rustworkx for native-code graph traversal.
- `meta lineage build [-d/--dev] [-v/--verbose] [--timeout N] [-o PATH] [--no-compile]` — parse compiled SQL of every model and write a `lineage.json` artifact (~8.7 MB for 27k columns / 23k edges on a 934-model project).
  - Per-model SIGALRM-based timeout (default 30s) prevents one pathological model from hanging the whole build; offending models are skipped with a `models_skipped_timeout` warning.
  - `--verbose` prints per-model progress with elapsed time and `slow` flag.
  - Slow-models report (top 5) is shown at the end of every build.
- `meta lineage column <model>.<col>` — direct + transitive upstream lineage for one column.
- `meta lineage downstream <model>.<col>` — direct + transitive downstream impact.
- `meta lineage stats` — artifact summary (nodes, edges, generated_at, warnings).
- All commands support `-j/--json` for machine consumption.
- Embedded design: single JSON artifact distributed via the same `~/dbt-state/` path as `manifest.json`/`catalog.json` — no server, no infra. Sub-10ms reads via LRU-cached rustworkx graph.
- New env vars: `DBT_PROD_LINEAGE_PATH`, `DBT_DEV_LINEAGE_PATH`.
- Optional install: `pip install dbt-meta[lineage]` (pulls `sqlglot>=30.0`, `rustworkx>=0.15`). `sqlglot[c]` is supported and recommended for 2-4× speedup on large projects.
- Location: `src/dbt_meta/lineage/{builder,graph,artifact,finder}.py`, `src/dbt_meta/command_impl/lineage.py`.

#### Optimization advisors (`meta optimize`)
On-demand, column-usage-aware advisors. They read each downstream model's compiled SQL via SQLGlot, build a column-usage report (WHERE / JOIN / GROUP BY / ORDER / QUALIFY / window PARTITION BY) and apply explainable heuristics. No `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, no LLM, no extra build step — pure AST analysis on existing artifacts.

- `meta optimize cluster <model> [--top N] [--no-compile]` — BigQuery clustering keys (capped at 4). Per-column score: `WHERE eq×3.0`, `WHERE in×2.5`, `WHERE between/range×2.0`, `JOIN×2.0`, `GROUP BY×1.0`, `WHERE fn-wrapped×0.5`, multiplied by `log2(downstream_count_using_column + 1)`. Excludes the model's own partition column and disallowed types (STRUCT/ARRAY/GEOGRAPHY/JSON).
- `meta optimize partition <model> [--no-compile]` — single partition column. Type-aware (TIMESTAMP/DATE × 1.5, DATETIME × 1.3, INT64 × 1.0); range/equality filter weights; granularity heuristic (DAY for DATE/TIMESTAMP, RANGE_BUCKET for INT64). Returns one primary recommendation + up to 4 alternatives + `pruning_impact_pct`.
- `meta optimize refresh [<models>...]` — column-aware `--full-refresh` planner with **chain-aware propagation** (see Changed below).
- All three support `-d/--dev`, `-j/--json`. New flag for `refresh`: `--cols MODEL:c1,c2` for column-precision (whole-model semantics by default).
- Output enrichments for `cluster`/`partition` (see Changed): direct-vs-transitive downstream count, materialization breakdown, "matches current" detection, human-readable reasoning, manifest-path footer.
- New module: `src/dbt_meta/usage/{extractor,advisor_cluster,advisor_partition,advisor_refresh,_common}.py`.

#### `meta optimize refresh` — git integration & ergonomics
- `-m/--modified` auto-detects changed models from git: `git diff <base>...HEAD --name-only` (committed) + `git diff HEAD` (unstaged) + `git diff --cached` (staged) + untracked from `git status --porcelain --untracked-files=all`.
- `--base BRANCH` lets you override the auto-detected base (auto: `origin/main` → `origin/master` → `main` → `master`).
- Output shows the resolved git base and per-model source tag (`committed` / `uncommitted` / `untracked` / `explicit`).
- Suggested ready-to-paste shell command appears in both text and JSON output: `dbt run -fs <full_refresh models>` and `dbt run -s <incremental models>`. Printed via plain `print()` (not Rich) so terminal copy-paste returns a single uninterrupited line.
- `SKIP` bucket is capped at 5 models in text output (`(+N more — use -j for full list)`); `FULL REFRESH` and `INCREMENTAL` keep the 30-row limit.

#### Mandatory compiled-SQL pre-flight (`_ensure_manifest_compiled`)
- Applies to commands that read compiled SQL: `lineage build`, `optimize cluster`, `optimize partition`, `sql`, `validate`, `scan`, `branch`. (`optimize refresh` keeps its own bulk-compile path.)
- When a loaded manifest has compiled SQL for fewer than half its models — the unmistakable shape of a `dbt parse`-only manifest — the pre-flight either:
  1. Auto-runs `dbt compile` for the **whole project** (no `--select`, since selective compiles leave gaps that re-trigger compile on the next run), then reloads the manifest, OR
  2. Hard-fails with a path-tagged error explaining why (covers prod manifest, explicit `--manifest`, `--no-compile`, or no dbt project found).
- New `--no-compile` flag on every affected command opts out of step 1.
- Error/info messages include the offending manifest path so users immediately see *which* manifest was loaded.

#### Auto-compile fallback for `meta optimize refresh`
- When the chosen manifest lacks `compiled_code` (e.g. local manifest from `dbt parse`), the advisor walks a 3-level fallback per downstream: (1) `model['compiled_code']`; (2) `<project>/target/compiled/<package>/<path>.sql` on disk; (3) one bulk `dbt compile --select <downstream models>` invocation per run.
- Trigger guarded: only fires when project root resolved (`dbt_project.yml` walked up from manifest path), `dbt` is in PATH, and >50% of a sampled downstream slice is missing SQL.
- `--no-compile` disables level (3).
- Friendly error path: when `dbt compile` fails (deprecation errors, project misconfig), the advisor surfaces the first error line + a workaround hint (`--manifest ~/dbt-state/manifest.json`).

### Changed

#### `meta optimize cluster` / `partition` — output overhaul
- **Direct downstream, not transitive.** Cluster/partition pruning only applies to models that read the target table *directly*. Grandchildren read the intermediate model's storage. The advisor now BFS-walks one level only (`direct_downstream` in `usage/_common.py`); the "X of Y models reference this table" line shifts from transitive descendants to direct readers — a smaller, more honest number.
- **Materialization breakdown.** The summary now shows `Direct downstream: N models (X incremental, Y table/view); Z with analyzable references`. Per-recommendation buckets:
  - ✅ `incremental_with_pruning` — incremental downstream that filter on the recommended column (good).
  - ❗ `incremental_without_pruning` — incremental downstream that read the table without a pruning filter. Each one triggers a full upstream scan on every incremental run — the advisor calls these out **in red** with a per-model list and a fix hint.
  - `non_incremental_with_pruning` / `non_incremental_full_scan` — table/view downstream where a full scan is expected.
- **"Matches current" detection.** When the top recommendation equals the model's existing `partition_by` / `cluster_by`, the advisor prints `✓ Current partitioning by '<col>' is already optimal — no changes needed.` instead of an apparent diff.
- **Human-readable reasoning.** Replaced cryptic `WHERE range ×1 · WHERE fn ×4 · in 4 models` with sentences: *"1 model uses BETWEEN range filter"*, *"4 models wrap the column in a function (e.g. DATE(col)) — those queries CANNOT prune partitions"*, etc.
- **Manifest path footer.** Both commands now print `Manifest: <resolved-path>` so users see *which* manifest was actually loaded (matters when the discovery fallback picks `./target/manifest.json` over `~/dbt-state/...`).
- **JSON schema change:** old fields `downstream_count` / `parsed_downstream_count` renamed to `direct_downstream_count` / `analysed_downstream_count`; new fields `incremental_count`, `non_incremental_count`, `current_cluster_by`, `matches_current`. Per-recommendation: `incremental_with_pruning`, `incremental_without_pruning`, `non_incremental_with_pruning`, `non_incremental_full_scan`. Legacy `models_using_pruning` / `models_without_pruning` aggregates retained for back-compat.

#### `ColumnUsageExtractor` — `wrapping_fn` field
- `UsageEvent` gains a `wrapping_fn: str` field — lowercased name of the closest function ancestor between the column and the comparison operator (`"date"`, `"timestamp_trunc"`, `"upper"`, …). The legacy `operator == "fn"` sentinel still fires when the column is wrapped *and* no comparison was found, for back-compat; modern consumers check `wrapping_fn` directly so they can preserve the real comparison (`eq`/`ge`/…) alongside the wrapper.
- **Partition advisor uses this** with a whitelist of BigQuery-prune-friendly wrappers (`date`, `timestamp`, `datetime`, `timestamptrunc`, `datetimetrunc`, `datetrunc`, `timetrunc`). Filters like `WHERE DATE(event_time) >= '2026-01-01'` against a TIMESTAMP partition now correctly count as pruning — they had been misclassified as "no pruning" before, because every function wrap was treated as opaque.

#### `meta optimize refresh` — chain-aware propagation
The original V1 of the refresh planner only checked **direct** references between each downstream and the changed model. That missed transitive consumers — e.g. if A → B → C and C never mentions A directly (only B), C was incorrectly classified as `skip`.

The new algorithm walks transitive downstream in **topological order** (Kahn's, on `depends_on.nodes`) and propagates affectedness through the chain:
1. Start: `affected_cols[changed_model] = <user-provided cols>` (or `None` for whole-model).
2. For each downstream `M` in topo order, look at its already-affected upstreams. Three cases:
   - `M` does `SELECT * FROM <affected_upstream>` → `M.affected = None` (whole-row), propagated downward.
   - `M` references specific upstream cols that intersect `affected_cols[upstream]` → those upstream-col names become `M.affected`.
   - Whole-parent (`affected = None`) + any reference → `M.affected = None`.
3. Per-model classification: `affected = None` → full; `affected ∩ unique_key` non-empty → full; `affected ∩ partition_by` non-empty → full; otherwise incremental (if model is incremental) else full.

`SELECT *` detection is now AST-level (`exp.Select` with `exp.Star` projection AND a matching `from_` clause), not regex — no false positives from comments/CTE/window expressions.

Added `--cols MODEL:c1,c2` for column-precision input. Without it, the planner is conservative (whole-model); with it, only models that actually use those specific columns end up in `full_refresh` / `incremental`.

#### `ColumnUsageExtractor` — `qualify_columns` pre-pass
SQLGlot's scope-walk previously missed bare-name column references (`WHERE event_type = 1` without alias prefix) because `col.table` was empty and our alias filter rejected them. The extractor now runs `sqlglot.optimizer.qualify_tables` + `qualify_columns` (with `infer_schema=True`) before walking clauses, so every `Column` node has `.table` populated. Real-world impact: dbt-compiled SQL routinely uses bare references — without this fix the extractor silently returned 0 events on most production models.

### Fixed
- **`meta optimize refresh -m` missed untracked models in new directories.** `git status --porcelain` collapses untracked directories to a single entry (`?? models/new_dir/`), which never matched any node's `original_file_path`. Now uses `--untracked-files=all` so each individual `.sql` file is listed.
- **Silent score inflation in cluster/partition advisors.** Unknown WHERE operators (`neq`, `is_null`, `like`, `none`) used to fall through `dict.get(..., 0.5)` and silently add 0.5 to the score without any matching counter in the visible reasoning — the displayed score didn't equal the sum of explained bullets. Unknown operators are now explicitly skipped; only listed ones contribute, and the `downstream_set` driving the log2 multiplier and "in N models" line counts only scoring events.
- **Misleading "not found" error for invalid lineage column input.** `meta lineage column foo` (no `.` or `:` separator) and `meta lineage column foo.` (empty column) used to surface as "Column 'foo' not found in lineage graph". Now returns a format-specific error: `Invalid column reference 'foo': expected 'model.column' or 'model:column'`.
- **`meta optimize refresh ghost_model` exited 0** when all specified models were absent from the manifest — it ran the planner anyway, printed an empty summary with a buried warning, and returned success. Now exits 1 with `None of the specified models exist in the active manifest: …` and a hint to run `dbt parse` / `dbt compile`.
- **`0/N models reference this table` was misleading** for cluster/partition when downstream `compiled_code` was empty. The total (`N`) came from manifest structure, the analysed count (`0`) from SQL parsing — different sources, same line. Added `diagnose_no_extraction()` warning that surfaces "all N downstream models have empty compiled_code — manifest probably produced by `dbt parse`" before showing the empty summary.
- **`select_star_from()` reads SQLGlot's modern `Select.args["from_"]` key** (was hard-coded to `"from"` and never matched).
- **Replaced the lone `except Exception:` in `lineage/builder.py`** with the project-mandated specific exception list (`SqlglotError`, `RecursionError`, `AttributeError`) — silent-failures lint now passes.
- **README test output capture:** `optimize refresh -m` previously printed via Rich's `console.print`, which soft-wrapped long `dbt run -fs ...` lines; pasting the visual output into a shell ran only the first line. Suggested commands are now emitted via plain `print()` so the entire command lands in the clipboard as one line.

## [0.2.3] - 2026-05-01

### Added
- **3-level compiled SQL fallback for `validate` and `scan`** — both commands now degrade gracefully when `compiled_code` is missing from the manifest (e.g. dev manifest produced by `dbt parse`).
  - **Level 1:** `model['compiled_code']` from manifest (default fast path)
  - **Level 2:** read `{project_root}/target/compiled/{package}/{original_file_path}` from disk (works when user ran `dbt compile` separately from `meta refresh --dev`)
  - **Level 3 (`--dev` only):** auto-runs `dbt compile --select <model> --target dev` (180s timeout) and re-reads from disk
  - Project root is found by walking up from manifest path to a `dbt_project.yml`. Package name extracted from `model['package_name']` or `unique_id`.
  - Failure modes return clear actionable errors: `dbt` not on PATH, compile timeout, compile error, no `dbt_project.yml`. Without `--dev`, suggests adding `--dev` for local changes.
  - Location: `utils/compiled_sql.py`, integrated in `command_impl/validate.py` and `command_impl/scan.py`

### Fixed
- **JSON error output for all commands** — errors now always return valid JSON when `-j` is used
  - Before: errors wrote Rich-formatted text to stderr; `2>&1 | jq` received mixed input → exit code 5
  - After: all errors emit `{"error": "..."}` to stdout when `-j` is passed
  - Affects: `handle_error()`, `_not_found_error()` helpers + all 20 command error paths
  - `ColumnsCommand._print_not_found_message()` suppressed in JSON mode (CLI layer handles it)
  - Location: `cli.py:53-88`, `command_impl/columns.py:410`

- **`bq` CLI not found when meta is run as installed package**
  - `shutil.which('bq')` was returning `None` because shell `PATH` (from `.zshrc`) is not
    inherited when running as a non-interactive subprocess
  - Added `_find_bq_cmd()` with 3-level discovery: current `PATH` → extended PATH with common
    SDK locations → direct file existence check
  - Common paths searched: `/opt/homebrew/bin`, `/usr/local/bin`, `~/google-cloud-sdk/bin`
  - subprocess `PATH` also extended so `bq` can find its own dependencies
  - Location: `utils/bigquery.py:15-44`

### Documentation
- **Comprehensive doc audit** — README, CLAUDE.md, and `meta --help` now reflect every command and flag (including previously undocumented `hotspots -n/--limit/--min-gb`, `powerbi --measures/--columns/--full/--by-table`, `settings init -f/--force`).
- New **Command Inventory** section in CLAUDE.md (24 commands + subcommands, all flags, defaults).
- README **Commands Reference** rewritten as grouped tables with a "Key flags" column per command.
- README + CLAUDE.md document the new 3-level compiled SQL fallback strategy.

### Tests
- **Coverage raised from 91.30% → 95.86%** (756 → 784 tests).
- New test files closing gaps: `test_powerbi_command_gaps.py`, `test_hotspots_gaps.py`, `test_monitoring_gaps.py`, `test_catalog_gaps.py`, `test_columns_command_gaps.py`, `test_path_command_gaps.py`, `test_compiled_sql.py`.
- 10 modules now at 100% coverage: `command_impl/columns.py`, `command_impl/hotspots.py`, `command_impl/powerbi.py`, `command_impl/scan.py`, `command_impl/sql.py`, `command_impl/validate.py`, `utils/compiled_sql.py`, `utils/monitoring.py`, `utils/powerbi.py`, `errors.py`.

## [0.2.2] - 2026-03-12

### Added
- **`meta hotspots` command** - Find models with highest optimization potential
  - Analyzes all tables using `dbt_bigquery_monitoring` data in parallel (ThreadPoolExecutor)
  - Two output blocks: **Top by Score** (optimization priority) + **Top Storage Savings**
  - Scoring algorithm v4 — calibrated in "cents equivalent" (€0.01 = 1pt):
    - `query_cost`: direct spend, €0.01/week = 1pt (primary signal)
    - `high_scan`: bytes/query × log2(frequency), up to 100pts
    - `high_slot`: slot_sec/query × log2(frequency), up to 75pts
    - `no_partition`: table_size × log2(frequency), up to 75pts
    - `no_clustering`: table_size × log2(frequency), up to 50pts
    - `low_cache`: wasted cost × 100 if cache_hit < 30%, up to 200pts
    - `unused`: monthly storage cost × 100 if unused >30 days, up to 200pts
  - Returns `scoring_details` with per-criterion recommendations
  - Options: `--limit N` (default: 10), `--min-gb X` (default: 0.1), `-j` for JSON
  - Location: `command_impl/hotspots.py`, `utils/monitoring.py`

- **`meta analyze <model>` command** - Deep analysis of single model
  - Combines manifest config with BigQuery monitoring data
  - Storage metrics, partition stats, query frequency, clustering effectiveness
  - Recommendations based on actual usage patterns
  - Location: `command_impl/analyze.py`

- **`meta branch <model>` command** - Optimization analysis across model lineage
  - Examines upstream/downstream models for partition/cluster alignment
  - Identifies filter patterns in downstream that should inform upstream config
  - Location: `command_impl/branch.py`

- **BigQuery monitoring queries** (`utils/monitoring.py`):
  - `fetch_all_tables_storage()` — storage metrics from `storage_with_cost`
  - `fetch_model_query_costs()` — costs from `most_expensive_models`
  - `fetch_partition_info_all()` — partition config from `partitions_monitoring`
  - `fetch_read_heavy_tables()` — read frequency from `read_heavy_tables`
  - `fetch_unused_tables()` — unused tables from `unused_tables`
  - `fetch_model_metrics()` — execution metrics from `models_costs_incremental`
  - `fetch_table_query_frequency()` — query frequency per table
  - `fetch_dataset_billing_recommendations()` — physical vs logical billing
  - `fetch_total_bigquery_costs()` — total BQ spend for context

- **`meta powerbi [workspace_id]` command** - Power BI integration
  - Extracts BigQuery tables used by Power BI dashboards and maps to dbt models
  - Default view: Workspace → Dataset → Reports → Tables hierarchy
  - `--by-table` flag: aggregated view grouped by BigQuery table with usage counts
  - `--measures` flag: DAX expressions with `parse_dax_references()` analysis
  - `--columns` flag: column schemas with data types, formats, visibility
  - `--full` flag: all metadata combined
  - OAuth via Service Principal (client_credentials flow using `curl`)
  - TOML config or `POWERBI_*` env vars
  - Location: `command_impl/powerbi.py`, `utils/powerbi.py`

### Changed
- **Renamed `cost` command to `scan`** — better reflects the command's purpose
  - `meta cost` → `meta scan`
  - File: `command_impl/scan.py` (renamed from `cost.py`)

- **Minimalist text output for `schema`, `path`, `sql`** — shell-friendly
  - Removed decorative headers and blank lines
  - Returns only the value: table name, file path, or SQL
  - Enables: `TABLE=$(meta schema model_name)`
  - JSON mode unchanged
  - Location: `cli.py:665-674`, `cli.py:849-850`, `cli.py:958-960`

- **Help message** — new command categories in `meta --help`
  - **Optimization** section: `hotspots`, `analyze`, `branch`
  - **Integration** section: `powerbi`
  - Location: `cli.py:136-149`

- **Configurable monitoring dataset** — no more hardcoded `"prod"` schema
  - Added `monitoring_dataset` to Config (default: `"prod"`)
  - TOML: `[bigquery] monitoring_dataset = "prod"`
  - Env: `DBT_MONITORING_DATASET`
  - Location: `config.py`, `utils/monitoring.py`

### Fixed
- **`meta hotspots` BigQuery CLI integration** — fixed on macOS
  - Removed PYTHONPATH clearing that broke `bq` CLI
  - Removed hardcoded `LIKE 'admirals%'` project filter
  - Location: `utils/monitoring.py:14-53`

## [0.2.1] - 2026-01-02

### Added
- **`meta validate` command** - Validate SQL syntax using BigQuery dry run
  - Validates compiled SQL against BigQuery without executing
  - Checks: syntax errors, table/column existence, type mismatches
  - Returns: `{model, valid, error}` - valid=true if SQL is correct
  - Supports `--dev` flag for dev manifest
  - Example: `meta validate customers` → `✅ Valid` or `❌ Error: Unrecognized name: col at [1:8]`
  - Location: `command_impl/validate.py`, `cli.py`

- **`meta cost` command** - Estimate query scan size using BigQuery dry run
  - Estimates bytes scanned without executing query
  - Returns: `{model, bytes, formatted, error}` - formatted like "3.2 GB"
  - Uses `bq query --dry_run` for cost estimation
  - Supports `--dev` flag for dev manifest
  - Example: `meta cost customers` → `Scan size: 3.2 GB`
  - CI/CD usage: `meta cost -j model | jq '.bytes'` for programmatic checks
  - Location: `command_impl/cost.py`, `cli.py`

- **BigQuery dry run utility** - Shared function for validate/cost commands
  - `run_dry_run_query(sql)` - validates SQL and returns bytes estimate
  - `format_bytes(bytes)` - formats bytes to "X.X MB" or "X.X GB"
  - Parses bq output: "Query successfully validated. ... N bytes of data."
  - Handles errors: syntax, table not found, permission denied
  - Location: `utils/bigquery.py:311-397`

- **Tests for validate/cost** - 14 new tests with 100% coverage
  - ValidateCommand: valid SQL, invalid SQL, no compiled SQL, model not found
  - CostCommand: valid SQL, small query, invalid SQL, no compiled SQL, model not found, zero bytes
  - format_bytes: MB, GB, zero, large values
  - Location: `tests/test_validate_cost.py`

### Fixed
- **Test environment isolation** - Fixed 19 failing tests caused by environment variables
  - Issue: Tests affected by shell env vars (`DBT_VALIDATE_BIGQUERY`, `DBT_DEV_SCHEMA`)
  - Fix: Added `monkeypatch.delenv()` to clear env vars in affected tests
  - Fix: Added `patch('dbt_meta.config.Config.find_config_file')` to force env var usage
  - Fix: Corrected mock patch locations (patch where imported, not where defined)
  - All 502 tests now pass

## [0.2.0] - 2025-12-07

### Changed
- **Flag naming to match dbt CLI** - Renamed `--refresh` to `--full-refresh` with `-f` alias
  - `--refresh` → `--full-refresh` (matches `dbt build --full-refresh`)
  - Added `-f` as short alias for `--full-refresh` (like dbt CLI)
  - `-m` changed from `--manifest` to `--modified` (list command only)
  - `--manifest` now only has long form (no short `-m` alias)
  - Rationale: Consistent terminology with dbt CLI, frequently used flags get short aliases
  - **Breaking Change**:
    - Commands using `-m` for manifest must use `--manifest` instead
    - Commands using `--refresh` must use `--full-refresh` instead
  - Updated help text and examples to reflect new aliases
  - Location: `cli.py:970, 973, 1018-1019, 1026, 164, 991, 1004`

### Added
- **Text output headers** - Bold green headers with blank lines before output
  - `meta list` - Shows "Models:" before model list (including empty results)
  - `meta schema` - Shows "Table:" before table name
  - `meta path` - Shows "File path:" before path
  - `meta sql` - Shows "Compiled SQL:" or "Raw SQL:" before SQL output
  - Text mode: Always shows header with empty line before it
  - JSON mode: No headers (clean JSON output)
  - Empty results: Shows header even when no models found (text mode only)
  - Location: `cli.py:660-662, 840-843, 875-877, 1018-1033`

- **Help improvements** - Better organization and focused examples
  - Added "Model filtering (list)" section with 3 key examples
  - Reduced "Combined flags" from 3 to 1 example for clarity
  - Restructured Flags section: Global → Output → Specific (was "Command-specific")
  - Added `meta list` flags: `--and`, `--group`, `-m/--modified`, `-f/--full-refresh`
  - Clarified `-a, --all` flag: "Recursive mode (parents/children commands)"
  - Location: `cli.py:149-188`

- **`meta list` command** - Powerful model filtering (replaces `dbt ls`)
  - Selectors: `tag:`, `config.`, `path:`, `package:`
  - Tag logic: OR by default, AND with `--and` flag
  - Git-aware: `-m, --modified` flag shows changed/new models (optimized batch git calls)
  - Graph traversal: `-f, --full-refresh` flag finds models needing `--full-refresh`
  - Output modes:
    - Text: space-separated model names
    - `--full-refresh` text: model names with `+` suffix (for dbt select syntax)
    - `--group`: grouped by tag
    - `-j`: JSON format (detailed for normal mode, compact `{models: [], tables: []}` for `--full-refresh`/`--modified`)
    - `--all` with `--full-refresh`: tree view showing dependency graph from modified to downstream
  - Dev/prod: `--dev` flag for dev manifest
  - Renamed old `list` command to `models` (simple substring search)
  - Test coverage: 41 new tests, all passing (89.52% total coverage)
  - Location: `commands.py:478-838`, `cli.py:951-1027`

- **Empty result warnings** - Informative messages when no models found
  - `meta list -m` shows "No modified models found" when branch is clean
  - `meta list -f` shows "No models need refresh" when no changes
  - Prevents confusion from silent empty results
  - Location: `commands.py:560-578`

- **Pipe-friendly output** - Headers hidden in command substitution
  - Headers ("Models:", "Table:", etc.) shown only in TTY (interactive terminal)
  - When piped or in `$()` substitution - only data output
  - Enables: `defer test --select $(meta list -m)`
  - Warnings still go to stderr (visible but not captured)
  - Location: `cli.py:1032-1044`

- **Tree view for `-f --all`** - Visual lineage from modified to downstream
  - Shows full dependency tree from each modified model
  - Icons: 🔴 uncommitted (red), ✅ committed (green)
  - Recursive display with proper indentation (├──, └──, │)
  - Example: `meta list -f --all`
  - Helps understand impact of changes before running --full-refresh
  - Location: `commands.py:698-775`, `cli.py:973`

### Fixed
- **Git comparison logic** - Now compares current branch vs main/master (not just uncommitted)
  - **Breaking Change**: `-m/--modified` and `-f/--full-refresh` now detect both:
    - Committed changes (in branch but not in main/master)
    - Uncommitted changes (local modifications)
  - Before: Only detected uncommitted local changes
  - After: Detects ALL changes relative to production (committed + uncommitted)
  - Added `is_committed_but_not_in_main()` function to check committed changes
  - Fallback logic: tries origin/main → origin/master → main → master
  - Test coverage: 5 new tests for committed change detection
  - Location: `utils/git.py:90-140, 300-302`, `tests/test_git.py:479-545`

- **Git warning accuracy** - Improved messages for committed models
  - Before: "Model NOT modified in git, but using --dev flag" (confusing for committed models)
  - After: "Model is committed but not merged to main" (clear distinction)
  - Warning types:
    - `dev_without_changes` - Model clean, using --dev (suggests removing --dev)
    - `dev_committed_not_merged` - Model committed, using --dev (info message)
    - `git_committed` - Model committed, not using --dev (suggests --dev if needed)
  - Warnings no longer duplicate model names (shown once in output)
  - Location: `utils/git.py:361-399`, `commands.py:650-678`

- **Unified git status messages** - Single INFO block for `-m/--modified` and `-f/--full-refresh`
  - Combines uncommitted and committed counts in one message
  - Example: "Found 1 uncommitted and 4 committed model(s) in current branch"
  - Reduced noise from multiple warnings (dev-oriented commands)
  - Location: `commands.py:662-683`

- **Uncommitted changes detection** - Now includes locally modified files
  - Before: Only detected files changed in branch vs main/master
  - After: Detects both branch changes AND locally modified files
  - Example: File in main with local edits now appears in `-m/--modified`
  - Location: `commands.py:788-801`

- **`meta list -f` KeyError** - Fixed descendant lookup in full-refresh filter
  - Problem: `children()` returns 'path' field (file path), but models dict uses unique_id
  - Solution: Added path→unique_id reverse lookup mapping before descendants loop
  - Tests: Added 6 edge case tests for error handling and selector validation
  - Location: `commands.py:787-800`, `tests/test_commands.py:2153-2229`

- **`meta list -m` performance** - Optimized git operations from O(N) to O(1)
  - Problem: Was calling git subprocess for every model (600+ calls for 300 models)
  - Solution: Batch git operations (4 calls total regardless of model count)
  - Performance: ~100x faster for large manifests
  - Before: `git diff` + `git status` once per model
  - After: Single batch call of each, cache results in memory as sets
  - Location: `commands.py:715-799`

## [0.1.6] - 2025-11-28

### Changed
- **UI terminology improvements** - Simplified lineage command labels for clarity
  - Tree view: "All ancestors" → "All parents", "All descendants" → "All children"
  - Kept "Direct parents" and "Direct children" for non-recursive views
  - Location: `cli.py:981, 986, 1031, 1036`

- **Emoji consistency** - Improved visual clarity in CLI output
  - Success messages: ✓ → ✅ (more visible in terminals)
  - Parents tree: 📊 → 👴 (clearer semantic meaning)
  - Children tree: 📊 → 👶 (clearer semantic meaning)
  - Location: `cli.py:369, 481, 981, 1031, 1075`, `commands.py:387, 399`

## [0.1.5] - 2025-11-28

### Fixed
- **SQL command behavior for modified models** - Now returns production SQL with clear git warnings
  - Behavior: `meta sql model_name` returns production compiled SQL (may be old version if modified)
  - Git warnings automatically shown via `BaseCommand.get_model_with_fallback()`
  - Warnings explain model is modified and suggest using `--dev` flag for dev SQL
  - Removed CLI error handling for empty result (empty string is valid)
  - All 889 models in production manifest have compiled_code (verified)
  - Location: `command_impl/sql.py:59-75`, `cli.py:829-831`

- **Path references updated** - Replaced all `.dbt-state/` with `~/dbt-state/` in docstrings
  - Fixed incorrect path references in command_impl modules
  - Updated all documentation to use correct default path
  - Location: All `command_impl/*.py` docstrings

### Added
- **SQL command test** - `test_sql_returns_empty_string_not_none_for_missing_compiled`
  - Verifies empty string (not None) for missing compiled_code
  - Total: 440 tests (+3), coverage 91.76%

### Changed
- **README AI features section** - Enhanced feature list for better AI agent clarity
  - Added "Machine-readable JSON" - every command has `-j` flag for structured output
  - Added "3-level fallback" - Production manifest → Dev manifest → Database
  - Added "Git-aware" - auto-detects model state with helpful warnings
  - Simplified "Why CLI over MCP?" section for clarity

## [0.1.4] - 2025-11-27

### Fixed
- **Git status detection from non-project directory** - Fixed false "DELETED locally" warnings
  - Bug: Running `meta columns` from outside dbt project showed models as deleted
  - Symptom: `⚠️ Model 'stg_google_ads__campaign_basic_stats' is DELETED locally`
  - Root cause: `get_model_git_status()` searched for file relative to CWD, not using manifest path
  - Fix: Added `file_path` parameter to use path from manifest (`original_file_path`)
  - Now: Git status detection works regardless of current working directory
  - Location: `utils/git.py:334`, `command_impl/columns.py:69-76`

- **Git modification detection for full model names** - Fixed `is_modified()` missing files
  - Bug: Models with full filename (e.g., `core_google_events__user_devices.sql`) not detected as modified
  - Symptom: `--dev` flag warned "NOT modified" despite uncommitted changes
  - Root cause: `is_modified()` only searched by table name (`user_devices.sql`), not full model name
  - Fix: Search by both table name AND full model name in git diff/status
  - Now: Detects modifications for all filename patterns
  - Location: `utils/git.py:129-134, 149-154`

- **BigQuery fallback message clarity** - Improved prod/dev table messaging
  - Changed "IS modified" to "is modified" for consistent lowercase formatting
  - Added prod/dev table distinction: "BigQuery (prod table: X)" vs "BigQuery (dev table: X)"
  - Removed misleading "Using dev version" warning when querying production tables
  - Location: `utils/git.py:322`, `command_impl/columns.py:379-413`

### Added
- **Git status tests** - 3 new tests for file_path parameter
  - `test_git_status_with_manifest_file_path`
  - `test_git_status_file_deleted_from_disk`
  - `test_git_status_without_file_path_uses_find`

- **is_modified tests** - 4 new tests for full model name detection
  - `test_is_modified_detects_full_model_name`
  - `test_is_modified_detects_short_table_name`
  - `test_is_modified_new_file_full_name`
  - `test_is_modified_no_match_returns_false`

- **Message formatting tests** - 7 new tests for message clarity improvements
  - `TestGitWarningFormatting` - 2 tests for lowercase "is modified"
  - `TestBigQueryMessageFormatting` - 5 tests for prod/dev table distinction
  - Location: `tests/test_infrastructure.py:1217-1405`

### Changed
- **Test organization** - Consolidated test files for better maintainability
  - Reduced test files: 24 → 18 (-25%)
  - **test_bigquery.py** - Merged 3 files (bigquery_final_coverage, bigquery_retry, path_bigquery_coverage)
  - **test_git.py** - Merged 2 files (git_edge_coverage, git_safety)
  - **test_errors.py** - Merged exception_handling tests
  - Logical grouping by feature/concern (BigQuery, Git, Errors)
  - Total: 437 tests, 91.76% coverage

## [0.1.3] - 2025-11-27

### Fixed
- **BigQuery fallback schema resolution** - Fixed incorrect dev schema usage for MODIFIED models
  - Bug: `meta columns model_name` used dev schema (`personal_xxx`) instead of production
  - Symptom: `Failed to fetch from: personal_pavel_filianin.stg_google_play__installs_app_version`
  - Expected: `staging_google_play.installs_app_version` (production schema)
  - Root cause: `_fetch_from_bigquery_with_model()` used model's schema from dev manifest fallback
  - Fix: Added `prod_model` parameter to BigQuery fallback methods for correct schema resolution
  - Now: Production schema is always used for `MODIFIED_UNCOMMITTED` without `--dev` flag

- **Catalog staleness detection** - Now uses file mtime instead of internal generated_at
  - Fallback to BigQuery only if file not updated >24h (indicates CI/CD issue)
  - Info message if internal age >7 days (not regenerated, but file still synced)
  - Prevents false "catalog too old" warnings when file is fresh but schema unchanged

### Added
- **BigQuery schema resolution tests** - 3 new tests for prod/dev schema fallback scenarios
  - `test_modified_uncommitted_uses_prod_schema_not_dev`
  - `test_modified_uncommitted_without_model_uses_prod_schema`
  - `test_new_uncommitted_still_uses_dev_schema`

- **Catalog file age tests** - 4 new tests for file mtime vs internal timestamp scenarios
- **`get_file_age_hours()` method** - CatalogParser now distinguishes file age from internal age

## [0.1.2] - 2025-11-26

### Fixed
- **Model state detection** - Fixed false `MODIFIED_IN_DEV` warning for stable models
  - `is_committed` incorrectly triggered MODIFIED states for all tracked files
  - Now only `is_modified=True` triggers MODIFIED states
  - Removed unused `MODIFIED_COMMITTED` state

## [0.1.1] - 2025-11-25

### Fixed
- **README tables rendering on PyPI** - Removed `<br>` tags that broke table layout
  - PyPI markdown doesn't support HTML `<br>` tags in tables
  - Simplified examples to one per command

## [0.1.0] - 2025-11-25

### Added

#### TOML Configuration Support
- **Modern configuration files** with XDG Base Directory compliance
  - Config locations: `./.dbt-meta.toml`, `~/.config/dbt-meta/config.toml`, `~/.dbt-meta.toml`
  - Priority: CLI flags > TOML config > Environment variables > Defaults
  - Template-based initialization with inline documentation
  - TOML parsing: `tomllib` (Python 3.11+) or `tomli` (Python <3.11)

#### Settings Management Commands
- **`meta settings init`** - Create config file from template
- **`meta settings show`** - Display current merged configuration (text or JSON)
- **`meta settings validate`** - Validate config file syntax and values
- **`meta settings path`** - Show path to active config file

#### Configuration System
- **`Config.from_toml()`** - Load configuration from TOML file
- **`Config.from_config_or_env()`** - Load from TOML with env var fallback
- **`Config.from_env()`** - Load from environment variables only
- **`Config.find_config_file()`** - Auto-discover config file
- Full configuration dataclass with type hints and validation
- Automatic path expansion (~/ to home directory)
- Boolean parsing with sensible defaults

#### CLI Improvements
- **`-h` short flag support** - Both `-h` and `--help` work for all commands
  - Enabled via `context_settings={"help_option_names": ["-h", "--help"]}`
  - Works for main app and all subcommands (settings, etc.)
- **Simplified `schema` command output**
  - Text mode: Just the full table name (e.g., `admirals-bi-dwh.core_client.client_info`)
  - JSON mode: `{"model_name": "...", "full_name": "..."}`
  - Optimized for shell scripting and AI consumption

#### Username Sanitization
- **Improved BigQuery dataset compatibility**
  - Replaces ALL non-alphanumeric characters (not just dots/hyphens)
  - Uses regex: `re.sub(r'[^a-zA-Z0-9_]', '_', username)`
  - Examples: `pavel.filianin` → `pavel_filianin`, `user@example.com` → `user_example_com`
  - Ensures valid BigQuery dataset names (letters, numbers, underscores only)

#### Core Metadata Commands
- **`info`** - Model summary (name, schema, table, materialization, tags)
- **`schema`** - Full table name (database.schema.table)
- **`path`** - Relative file path to .sql file
- **`columns`** - Column names and types with catalog/BigQuery fallback
- **`sql`** - Compiled SQL (default) or raw SQL with `--jinja` flag
- **`docs`** - Column names, types, and descriptions
- **`config`** - Full dbt config (partition_by, cluster_by, etc.)

#### Dependency Navigation
- **`deps`** - Dependencies by type (refs, sources, macros)
- **`parents`** - Upstream dependencies (direct or all ancestors with `-a`)
- **`children`** - Downstream dependencies (direct or all descendants with `-a`)

#### Search and Discovery
- **`list [pattern]`** - List all models (optionally filter by pattern)
- **`search <query>`** - Search models by name or description

#### Fallback System
- **3-level fallback** - Production manifest → Dev manifest → Catalog.json
- **Catalog.json support** - Fallback to catalog when manifest columns empty
- **Environment variables** - `DBT_FALLBACK_TARGET`, `DBT_FALLBACK_CATALOG` (default: `true`)
- **Intelligent warnings** - Automatic git change detection with helpful suggestions

#### Output Modes
- **`--json, -j`** - JSON output for all commands (AI-friendly)
- **Rich formatted output** - Colored tables and text (default)
- **Combined flags** - Use `-dj`, `-ajd`, `-jm` for faster typing

#### Flexible Naming Configuration
- **Production table naming** - `DBT_PROD_TABLE_NAME` (alias_or_name, name, alias)
- **Production schema/database** - `DBT_PROD_SCHEMA_SOURCE` (config_or_model, model, config)
- **Dev schema naming** - `DBT_DEV_SCHEMA` (direct override, default: `personal_{username}`)
- **Username override** - `DBT_USER` (default: `$USER`)

#### Manifest Discovery
- **Auto-discovery** - Searches for manifest.json automatically
- **Simple Mode** - Works out-of-box with `./target/manifest.json` after `dbt compile`
- **Production Mode** - `DBT_PROD_MANIFEST_PATH` for central manifest location
- **Dev Mode** - `DBT_DEV_MANIFEST_PATH` (default: `./target/manifest.json`)
- **`-m, --manifest` flag** - Explicit manifest path override

#### Testing
- **416 comprehensive tests** - All passing
- **91.67% code coverage** - Exceeds 90% requirement
- Test categories: unit, integration, performance benchmarks
- Edge case testing: empty strings, null values, special characters

#### Performance
- **LRU caching** - Manifest parser cached for sub-10ms responses
- **orjson** - Fast JSON parsing (6-20x faster than stdlib)
- **Lazy loading** - Manifest loaded only when needed

#### Documentation
- Comprehensive README with examples and use cases
- Environment variables reference
- CLAUDE.md for AI agent integration
- Apache 2.0 license

[0.2.2]: https://github.com/Filianin/dbt-meta/releases/tag/v0.2.2
[0.2.1]: https://github.com/Filianin/dbt-meta/releases/tag/v0.2.1
[0.2.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.2.0
[0.1.6]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.6
[0.1.5]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.5
[0.1.4]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.4
[0.1.3]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.3
[0.1.2]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.2
[0.1.1]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.1
[0.1.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.0
