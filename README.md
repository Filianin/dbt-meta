# dbt-meta

> ⚡ AI-first CLI for dbt metadata extraction

**dbt-meta** is a lightning-fast command-line tool that extracts metadata from dbt's artifacts for DEs and AI agents, eliminating the need to parse `.sql` files or query your data warehouse for schema information. This is especially useful for fast and accurate agent operation, for example, Claude Code.

## ✨ Features

- **🎯 Works out-of-box** — Simple Mode: just run `dbt compile` and start using
- **⚙️ TOML configuration** — Modern config files with XDG compliance (optional)
- **⚡ Lightning fast** — Optimized Python with LRU caching and orjson parser
- **🔄 Production Mode** — Full `defer` workflow support for multi-project setups and development environment
- **📊 AI-friendly JSON** — Machine-readable structured output (`-j` flag) on every command
- **🔍 Rich metadata** — Schema, columns, dependencies, config, compiled SQL, column docs
- **🌳 Dependency navigation** — Trace upstream/downstream models (flat or tree view)
- **🔎 Advanced filtering** — Filter models by tags, config, path, package with OR/AND logic
- **🔀 Git-aware** — Find modified models and dependencies needing `--full-refresh`
- **📋 Smart search** — Find models by name or description
- **✅ SQL validation** — Validate SQL syntax using BigQuery dry run
- **📏 Scan estimation** — Estimate query scan size before running (MB / GB)
- **📊 Optimization analysis** — Find hotspots, analyze single models, branch-level alignment
- **🧬 Column-level lineage** — `meta lineage build/column/downstream/stats` with rustworkx graph (sub-10ms queries)
- **🎯 Column-usage-aware advisors** — `meta optimize cluster/partition/refresh` recommends BigQuery clustering/partition keys and minimal `--full-refresh` plan based on real downstream WHERE/JOIN usage
- **🔗 Power BI integration** — Extract BigQuery tables (+ DAX measures & column schemas) from dashboards
- **🔁 Artifact sync** — `meta refresh` pulls prod artifacts or parses local project
- **🎨 Beautiful UI** — Rich terminal formatting with categorized help panels
- **⚡ Combined flags** — Use `-dj`, `-adj`, `-mf` for faster typing (order-independent)


## 🤖 Built for AI Workflows

**dbt-meta** was specifically designed to eliminate AI agent hallucinations when working with dbt projects.

### The Problem

AI agents (like Claude Code, GitHub Copilot, ChatGPT) often hallucinate when working with dbt:
- ❌ **Wrong table names** - Confusing alias vs filename (`customers` vs `dim_customers`)
- ❌ **Wrong schema names** - Confusing prod and dev schemas
- ❌ **Unknown dependencies** - Missing refs/sources in lineage
- ❌ **Incorrect column types** - Using wrong data types in WHERE clauses
- ❌ **Non-existent fields** - Querying columns that don't exist

### The Solution

Following Anthropic's recommendation to use CLI tools over MCP for AI agents, **dbt-meta** provides:

- ✅ **Fast** - Optimized Python with caching, no repeated manifest parsing
- ✅ **Machine-readable JSON** - Every command has `-j` flag for structured output, no text parsing needed
- ✅ **Schema validation** - Prevents hallucinations by providing accurate metadata
- ✅ **Type-safe** - Mypy strict mode, comprehensive test coverage (91%+)
- ✅ **3-level fallback** - Production manifest → Dev manifest → your database (always finds metadata)
- ✅ **Git-aware** - Auto-detects model state (modified, new, deleted) with helpful warnings

### Integration

**dbt-meta** integrates seamlessly with:
- Claude Code (Anthropic) - Add to allowed commands in `.claude/settings.local.json`
- GitHub Copilot - Use in terminal and inline suggestions
- ChatGPT / Custom GPTs - Execute commands and parse JSON output
- Other AI agents - Standard CLI interface with JSON output

### Why CLI over MCP?

- Have deterministic, structured output
- Are faster and more reliable
- Work in any environment
- Don't require additional infrastructure

### Performance

**dbt-meta** uses several optimization techniques:

- **LRU Caching**: ManifestParser cached with `@lru_cache(maxsize=1)`
- **orjson**: Fast JSON parsing (2-3x faster than standard json)
- **Lazy loading**: Manifest parsed only when needed
- **Catalog fallback**: Use `catalog.json` instead of BigQuery queries

Measured performance (~900 models manifest):

| Command | Time | Notes |
|---------|------|-------|
| `meta schema` | ~250ms | Manifest only |
| `meta parents --all` | ~300ms | Traversed 295 ancestors |
| `meta columns` (catalog) | ~50ms | With fresh `catalog.json` |
| `meta columns` (BigQuery via bq CLI) | ~2-3s | Fallback when catalog stale |

**Tip**: Keep `catalog.json` fresh (prod state) for fastest `columns` performance.
But this only works for unmodified columns. For models built using defer in dev schema, 
column metadata is only in DWH.

## 📦 Installation

### PyPI Installation (Recommended)

```bash
# Install from PyPI (when published)
pip install dbt-meta

# Verify installation
dbt-meta --version
# or use shorter alias
meta --version
```

### Development Installation

```bash
# Clone repository
git clone https://github.com/Filianin/dbt-meta.git
cd dbt-meta

# Install in development mode
pip install -e .

# Or install with dev dependencies
pip install -e ".[dev]"

# Verify installation
meta --version
```

### Requirements

- **Python** 3.9+ (3.12+ recommended for best performance)
- **dbt** project with `manifest.json`
- Optional: **jq** for advanced JSON processing

## 🚀 Quick Start

### Simple Mode (no configuration)

```bash
# Step 1: Compile your dbt project
dbt compile

# Step 2: Use dbt-meta immediately!
meta schema customers           # → your_project.analytics.customers
meta columns -j orders          # → JSON array of columns
meta list tag:daily             # → Filter models by tag
meta search "customer"          # → Find models by description

# Get comprehensive help with examples
meta --help
meta
```

### Production Mode (with defer workflow)

To organize a **dev environment**, you need to have the current version 
of the prod `manifest.json`, which will be regularly updated to the latest state.
For example, you can regularly compile your manifest and upload it to some cloud storage. 
From there, you can download this file to your machine in any way you like. 
If you generate documentation and upload it as a static website, then `catalog.json` is generated 
as part of this process, which is recommended to be uploaded along with the manifest. 
This file contains data about columns and data types that are missing from the manifest.

```bash
# One-time setup: Create config file
meta settings init

# Edit ~/.config/dbt-meta/config.toml:
prod_manifest_path = "~/dbt-state/manifest.json"
dev_schema = "personal_myname"

# Now works from any directory!
cd /tmp && meta schema customers  # → Uses production manifest

# For dev models (after defer run):
defer run --select customers
meta schema --dev customers      # → personal_myname.customers
meta columns -dj customers       # → Dev columns with JSON output
```

### Combined Flags (faster typing)

```bash
meta schema -dj customers                    # → Dev + JSON
meta parents -ajd model                      # → All ancestors + JSON + Dev
meta columns -j --manifest ~/path.json m     # → JSON + Custom manifest
```

## 📚 Commands Reference

All commands accept `-h/--help` for detailed per-command help.

### Core Metadata

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `schema <model>` | Full table name (`database.schema.table`) | `-j`, `-d` | `meta schema customers` |
| `path <model>` | Relative file path to .sql file | `-j`, `-d` | `meta path customers` |
| `columns <model>` | Column names and types | `-j`, `-d` | `meta columns -dj customers` |
| `config <model>` | Full dbt config (partition_by, cluster_by, incremental, etc.) | `-j`, `-d` | `meta config -j customers` |
| `sql <model>` | Compiled SQL (default) or raw with `--jinja` | `-j`, `-d`, `--jinja` | `meta sql --jinja customers` |
| `context <model> [<model> ...]` | Full queryable-shape bundle (FQN, partition/cluster/unique_key, stats, columns with type+description) for one or more models, before a BigQuery query | `-j`, `-d` | `meta context -j orders customers` |

### Lineage (model-level)

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `parents <model>` | Upstream dependencies (direct or all ancestors) | `-j`, `-d`, `-a` | `meta parents -aj customers` |
| `children <model>` | Downstream dependencies (direct or all descendants) | `-j`, `-d`, `-a` | `meta children -a customers` |

- Without `-a`: direct parents/children only (classic format).
- With `-a -j` and ≤20 nodes: nested JSON with `children` key; otherwise flat array.

### Column-level lineage (`meta lineage`)

Backed by SQLGlot 30.7+ (`sqlglot.lineage`, all-columns mode) and rustworkx for native graph traversal. The graph is built once into a JSON artifact (`~/dbt-state/lineage.json`), then queried in sub-10 ms.

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `lineage build` | Build the lineage artifact from manifest + catalog | `-j`, `-v`, `--timeout N`, `-o PATH`, `--manifest`, `--catalog` | `meta lineage build -v` |
| `lineage column <model>.<col>` | Direct + transitive upstream lineage for a column | `-j`, `--artifact` | `meta lineage column core_clients.client_id` |
| `lineage downstream <model>.<col>` | Direct + transitive downstream impact for a column | `-j`, `--artifact` | `meta lineage downstream raw.events.user_id` |
| `lineage stats` | Print artifact summary (nodes, edges, generated_at, warnings) | `-j`, `--artifact` | `meta lineage stats -j` |

Lineage is a **prod-only** concept (column-level lineage of the deployed state); there is no `--dev` variant. Artifact resolution: `--artifact` → `DBT_PROD_LINEAGE_PATH` → `~/dbt-state/lineage.json`.

**`build` flags:**
- `-v, --verbose` — print per-model progress (`[123/934] model_name (0.42s) ok`)
- `--timeout N` — per-model SIGALRM budget in seconds (default 30, 0 disables)
- `-o PATH` — explicit output path (default: next to the prod manifest, i.e. `~/dbt-state/lineage.json`)

For best performance install the mypyc-compiled SQLGlot: `pip install 'sqlglot[c]'` — gives 2-4× speedup on large projects (~470 s → ~150 s for 934 models).

### Optimization advisors (`meta optimize`)

Column-usage-aware advisors. They read each downstream model's compiled SQL via SQLGlot and apply explainable heuristics. No `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, no LLM, no extra build step — pure AST analysis on existing artifacts. On-demand only (no extra CI artifact).

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `optimize cluster <model>` | Recommend BigQuery clustering keys (≤4) from downstream WHERE/JOIN/GROUP BY usage | `-d`, `-j`, `--top N` | `meta optimize cluster core_sessions` |
| `optimize partition <model>` | Recommend a single partition column (DATE/DATETIME/TIMESTAMP/INT64) + granularity | `-d`, `-j` | `meta optimize partition core_clients` |
| `optimize refresh [<models>...]` | Column-aware `--full-refresh` planner with chain-aware propagation | `-m`, `--base BRANCH`, `--cols MODEL:c1,c2`, `-d`, `-j`, `--no-compile` | `meta optimize refresh -m` |

**`optimize cluster` heuristic:** per-column score = `WHERE eq×3.0 + WHERE in×2.5 + WHERE between/range×2.0 + JOIN×2.0 + GROUP BY×1.0 + WHERE fn-wrapped×0.5`, multiplied by `log2(downstream_count_using_column + 1)`. Excludes the model's own `partition_by` column and types unfit for clustering (`STRUCT`, `ARRAY`, `GEOGRAPHY`, `JSON`).

**`optimize partition` heuristic:** range/equality filter weights × type bonus (TIMESTAMP/DATE × 1.5, DATETIME × 1.3, INT64 × 1.0). Granularity heuristic: TIMESTAMP/DATE → `DAY`, INT64 → `RANGE_BUCKET`. Returns one primary recommendation + up to 4 alternatives + `pruning_impact_pct` (% of downstream queries that would benefit).

**`optimize refresh` algorithm:** topological BFS over transitive downstream. Affectedness propagates through the chain — a 3-level descendant `C` that never directly mentions changed `A` is still classified correctly when `A → B → C` and `B` consumes `A`'s changed columns. `SELECT *` propagates whole-row affectedness through the chain via SQLGlot AST detection (not regex). Per-model bucket: `full_refresh` (incremental key collision, SELECT *, or non-incremental materialization) / `incremental` (incremental + only safe columns affected) / `skip`.

**`optimize refresh` flags:**
- `-m, --modified` — auto-detect changed models from git (committed-vs-base + uncommitted + untracked)
- `--base BRANCH` — explicit base for git diff (auto: `origin/main` → `origin/master` → `main` → `master`)
- `--cols MODEL:c1,c2` — column-level diff for precise propagation; without it the planner conservatively treats the entire changed model as affected
- `--no-compile` — skip the on-demand `dbt compile` fallback (level 3 of compiled-SQL chain)

**Compiled-SQL fallback chain** (used by `optimize refresh`): manifest `compiled_code` → `<project>/target/compiled/<pkg>/<path>.sql` on disk → one bulk `dbt compile --select <downstream models>` per run. Only triggers when project root is found (`dbt_project.yml`), `dbt` is in PATH, and >50 % of a sampled downstream slice lacks SQL.

**Output (text mode)** ends with a ready-to-paste shell command:

```
Suggested commands:
  dbt run -fs core_sessions stg_clients core_orders ...
  dbt run -s some_incremental_model
```

The line is emitted via plain `print()` (not Rich) so terminal copy-paste returns one uninterrupted command.

### Discovery & Filtering

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `list [selectors…]` | Filter with selectors (`tag:`, `config.`, `path:`, `package:`) | `-j`, `-d`, `-m`, `-f`, `-a`, `--and`, `--group` | `meta list tag:daily --and tag:core` |
| `models [pattern]` | Simple substring search over model names | `-j` | `meta models staging` |
| `search <query>` | Search models by name or description | `-j` | `meta search "customer" -j` |

**`list` selectors:**
- `tag:name` — filter by tag (OR logic by default)
- `config.key:value` — filter by config value (e.g. `config.materialized:incremental`); supports nested keys via dotted path (e.g. `config.meta.domain:its`)
- `path:dir/` — filter by file path prefix
- `package:name` — filter by package

**`list` flags:**
- `--and` — require ALL selectors to match (default: OR)
- `--group` — group output by tag combinations (headers)
- `-m, --modified` — git-aware: only changed/new models

For refresh planning of changed models, use `meta optimize refresh` (column-aware chain analysis).

### SQL Validation & Cost

Uses BigQuery dry run (no rows processed, no charge for dry run itself). Both commands fall back through manifest → `target/compiled/` → auto `dbt compile` (with `--dev`) — see [details below](#validating-sql-and-estimating-scan-size).

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `validate <model>` | Validate compiled SQL syntax | `-j`, `-d` | `meta validate customers` |
| `scan <model>` | Estimate query scan size (🟢 <1 GB, 🟡 1–10 GB, 🔴 ≥10 GB) | `-j`, `-d` | `meta scan --dev -j customers` |

### Optimization (requires `dbt_bigquery_monitoring`)

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `hotspots` | Rank all tables by optimization score (cost, partitioning, clustering, cache) | `-j`, `-n/--limit`, `--min-gb` | `meta hotspots -n 20 --min-gb 10` |
| `analyze <model>` | Deep analysis of a single model — storage, usage, recommendations | `-j` | `meta analyze -j customers` |
| `branch <model>` | Branch-level analysis — upstream/downstream partitioning alignment | `-j` | `meta branch customers` |

**`hotspots` flags:**
- `-n, --limit N` — number of hotspots to display (default: 10)
- `--min-gb GB` — minimum table size in GB (default: 1.0)

### Integration — Power BI

Requires Azure AD Service Principal with Power BI Admin API access. `artifacts`
scans workspaces and builds a compact queryable index in one shot; `find` / `show`
answer "which BigQuery tables/models sit behind this dashboard?".

Both 1:1 navigation imports **and** custom Native SQL queries are parsed (via
SQLGlot) to recover the full list of imported BigQuery tables. Native SQL is also
analyzed separately (filters / joins / group-by) because it is logic living
outside the dbt project.

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `powerbi artifacts` | Scan workspaces + build compact index (incl. per-page visual layout) in one shot | `--raw`, `-o/--output`, `--manifest`, `--no-layouts`, `-j` | `meta powerbi artifacts` |
| `powerbi list` | List all reports (workspace \| report \| dataset \| tables) | `--artifact`, `-j` | `meta powerbi list` |
| `powerbi find` | Find reports / metrics behind a name | `--artifact`, `-j` | `meta powerbi find "organic leads"` |
| `powerbi show` | Full breakdown of one report (tables, SQL analysis) | `--artifact`, `-j` | `meta powerbi show "Organic Leads"` |
| `powerbi layout` | Visual semantics: pages → visuals → fields, titles, filters | `--artifact`, `-j` | `meta powerbi layout "Organic Leads"` |
| `powerbi reports` | Reverse: dbt model → PBI reports using it | `--artifact`, `-j` | `meta powerbi reports core_amas_accounts` |
| `powerbi cost` | Per-table query cost (7-day) for a report | `--artifact`, `-j` | `meta powerbi cost "Organic Leads"` |
| `powerbi lineage` | Column-level upstream paths for a report's filter/join columns | `--artifact`, `--lineage`, `-j` | `meta powerbi lineage "Organic Leads"` |
| `powerbi measures` | DAX measures + expressions + `dax_refs` for a report | `--raw`, `-j` | `meta powerbi measures "Retain"` |
| `powerbi source` | Power Query M-expressions for a report | `--raw`, `-j` | `meta powerbi source "Retain"` |
| `powerbi owners` | Report owners + last modified | `--raw`, `-j` | `meta powerbi owners "Organic Leads"` |

Power BI artifacts are **prod-only**. `artifacts` writes to the same paths the
cron-managed sync uses by default (`~/dbt-state/powerbi_raw.json`,
`~/dbt-state/powerbi_index.json`), so a manual run overwrites them in place.
Override with `--raw` (raw output) and `-o` (index output).

Index artifact discovery (for `find` / `show` / `reports` / `cost` / `lineage`):
`--artifact` → `DBT_PROD_POWERBI_PATH` → `~/dbt-state/powerbi_index.json`.

`cost` requires `dbt_bigquery_monitoring` (the same dataset used by `meta hotspots`).
`lineage` requires a built lineage artifact (`meta lineage build`).

### Artifacts & Settings

| Command | Description | Key flags | Example |
|---------|-------------|-----------|---------|
| `refresh` | Sync production artifacts from remote storage | `-d/--dev` (parse local via `dbt parse`) | `meta refresh` |
| `settings init` | Create config file from template | `-f/--force` (overwrite existing) | `meta settings init` |
| `settings show` | Display merged configuration (TOML + env) | `-j` | `meta settings show -j` |
| `settings validate` | Validate active config file | — | `meta settings validate` |
| `settings path` | Show active config file path | — | `meta settings path` |
| `examples` | Show usage examples for all commands | — | `meta examples` |
| `config-help` | Show env vars and TOML configuration reference | — | `meta config-help` |

### Global Flags

| Flag | Description | Applies to |
|------|-------------|-----------|
| `-h, --help` | Show help | All commands |
| `-v, --version` | Show version and exit | Main app |
| `--manifest PATH` | Explicit path to manifest.json (takes precedence over `--dev`) | All metadata commands |
| `-d, --dev` | Use dev manifest/schema (`./target/manifest.json`, `personal_USERNAME`) | Most metadata commands |
| `-j, --json` | Output as JSON (AI-friendly structured data) | Most commands |

**Combined short flags** work in any order: `-dj`, `-adj`, `-mf`, `-fa`, etc.

**Note:** `--manifest` has no short form. If `--manifest` and `--dev` are both provided, `--dev` is ignored with a warning.

## 💡 Common Use Cases

### Querying BigQuery with Correct Table Names

```bash
# Get production table name (eliminates AI hallucinations)
TABLE=$(meta schema customers)
bq query "SELECT * FROM $TABLE LIMIT 10"
# → SELECT * FROM your_project.analytics.dim_customers LIMIT 10

# Or with JSON output
TABLE=$(meta schema -j customers | jq -r '.full_name')
bq query "SELECT * FROM $TABLE LIMIT 10"
```

### Finding All Columns for a Model

```bash
# Get column list for WHERE clauses
meta columns -j orders | jq -r '.[] | .name'
# → order_id, customer_id, order_date, status, amount

# Get column types for schema validation
meta columns -j orders | jq -r '.[] | "\(.name): \(.data_type)"'
# → order_id: INTEGER, customer_id: INTEGER, order_date: DATE, ...
```

### Analyzing Dependencies

```bash
# Get all upstream models (for CI/CD impact analysis)
meta parents -aj customers | jq -r '.[] | .path'
# → staging/customers.sql, staging/orders.sql, staging/payments.sql

# Find downstream impact of model changes
meta children -a customers
# → Shows all models that depend on customers
```

### Working with Dev Models

```bash
# Build dev model
defer run --select customers

# Query dev table (not production)
TABLE=$(meta schema --dev customers)
bq query "SELECT * FROM $TABLE LIMIT 10"
# → SELECT * FROM personal_USERNAME.customers LIMIT 10
```

### Validating SQL and Estimating Scan Size

```bash
# Validate SQL syntax before running (uses BigQuery dry run)
meta validate customers
# → ✅ Valid

# Check for syntax errors
meta validate broken_model
# → ❌ Error: Unrecognized name: unknown_column at [3:5]

# Estimate query scan size
meta scan customers
# → Scan size: 3.2 GB

# JSON output for scripting
meta scan -j customers | jq -r '.formatted'
# → 3.2 GB

# Use in CI/CD to prevent expensive queries
BYTES=$(meta scan -j customers | jq -r '.bytes')
if [ "$BYTES" -gt 10000000000 ]; then  # 10 GB limit
  echo "Query too expensive: $BYTES bytes"
  exit 1
fi

# With local changes: --dev auto-compiles if needed
meta validate --dev my_model
# → ℹ️  No compiled SQL for 'my_model'. Running `dbt compile --select my_model --target dev`...
# → ✅ Valid
```

**Compiled SQL lookup (validate / scan):** 3-level fallback strategy:
1. `model['compiled_code']` from manifest (always works after `dbt compile`/`dbt run`)
2. `target/compiled/{package}/{original_file_path}` on disk (works if you ran `dbt compile` separately)
3. With `--dev`: auto-runs `dbt compile --select <model> --target dev` and re-reads from disk

The third level only fires when `--dev` is set, since auto-compilation is only safe in your local project. If `dbt` isn't on PATH, compilation fails, or the project root can't be found, you get a clear error with a suggested manual command.

### Refreshing Artifacts

```bash
# Sync production manifest.json + catalog.json from remote storage
meta refresh
# → Downloads to ~/dbt-state/ (always --force)

# Parse local project to ./target/manifest.json (use after editing models)
meta refresh --dev
# → Runs: dbt parse --target dev
```

### Optimization Analysis

```bash
# Find top optimization opportunities
meta hotspots --limit 10
# → Shows tables with highest optimization potential (scoring_details included)

# Deep analysis of specific model
meta analyze customers
# → Storage metrics, query costs, partition info, recommendations

# Analyze branch impact before merging
meta branch
# → Shows optimization impact of current branch changes

# JSON output for AI agents (includes recommendations)
meta hotspots -j | jq '.hotspots[0].scoring_details'
# → [{"criterion": "no_partition", "points": 60, "recommendation": "Add partition_by config"}]
```

### Column-Level Lineage

```bash
# Build the lineage artifact (one-time, runs SQLGlot on every model's compiled SQL)
meta lineage build --verbose
# → Wrote ~/dbt-state/lineage.json (8.7 MB, 27,059 nodes, 23,815 edges in 470 s)

# Where does a column come from?
meta lineage column core_internal_tracking__sessions.session_channel
# → Direct: stg_traffic_channel__mapping.assigned_channel
# → Ancestors: source.raw_traffic_channel.traffic_channel_mapping.assigned_channel, …

# What breaks if I change this column?
meta lineage downstream stg_traffic_channel__mapping.assigned_channel
# → 30 direct downstream columns across core_client__*, core_plausible__*, …

# Artifact stats (size, age, warnings)
meta lineage stats
# → schema 1.0, 27,059 nodes, 23,815 edges, generated 2026-05-08T10:20:03+00:00
```

### Refresh Planner — Minimum `--full-refresh` Set

```bash
# Detect changed models from git, classify their downstream
cd ~/Projects/reports
meta optimize refresh -m
# → 12 full_refresh, 0 incremental, 99 skip (out of 110 transitive downstream)
# → Suggested: dbt run -fs core_internal_tracking__events core_plausible__pageviews …

# Column-precision (drastically narrows the set when you know the diff)
meta optimize refresh --cols core_internal_tracking__events:event_type
# → 1 full_refresh, 1 incremental (sessions uses event_type in WHERE), 103 skip

# Override base branch (auto-detects origin/main → origin/master → main → master)
meta optimize refresh -m --base origin/develop

# Programmatic use
meta optimize refresh -m -j | jq -r '.dbt_commands.full_refresh'
# → "dbt run -fs core_internal_tracking__events …"
```

### Clustering & Partitioning Recommendations

```bash
# What columns should I cluster `core_sessions` on?
meta optimize cluster core_internal_tracking__sessions
# → match_utm_source (JOIN ×30 in 6 models, score 168.44)
# → match_utm_medium (JOIN ×30, score 168.44)
# → … (excluded: existing partition column, STRUCT/ARRAY/JSON columns)

# What's the right partition column for this model?
meta optimize partition amas_client_profiles
# → status (INT64) RANGE_BUCKET, pruning ~4.5%
# → Alternatives: client_id (INT64) score=1.5

# JSON for tooling
meta optimize cluster core_sessions -j | jq '.recommendations[].column'
```

### Power BI Integration

```bash
# 1. Scan configured workspaces + build the compact index in one shot
meta powerbi artifacts
# → ~/dbt-state/powerbi_raw.json   (navigation imports + native SQL expressions)
# → ~/dbt-state/powerbi_index.json (reports → BQ tables → dbt model/source/external,
#                                   + per-page visual layout under `pages`)

# By default a second pass calls Fabric getDefinition per report to attach
# per-page visual layout: pages[].visuals[].{type, fields{role: [{table,column,kind}]}}.
# Needs a Fabric-scoped service principal that is a *member* of the workspaces.
# Skip it for the fast path:
meta powerbi artifacts --no-layouts

# 3a. Find reports / metrics behind a dashboard name
meta powerbi find "organic leads"
# → matching reports (workspace / dataset / table count) + metrics

# 3b. Full breakdown of one report: tables, dbt mapping, native-SQL analysis
meta powerbi show "Organic Leads"
# → BigQuery Table | Status (model/source/external) | dbt Model
#   + SQL analysis (filters / joins / group-by — logic living outside dbt)

# 3c. Visual semantics of one report: pages, visuals, titles, filters
meta powerbi layout "Organic Leads"
# → pages → visuals (type, fields by role, explicit title)
#   + filters at report / page / visual scope (op + values + summary)

# JSON output for automation
meta powerbi find "leads" -j | jq '.reports[].tables[] | select(.status == "external") | .bq'
# → BigQuery tables used in Power BI but not tracked in dbt

# Explicit index artifact path (otherwise: DBT_PROD_POWERBI_PATH →
# ~/dbt-state/powerbi_index.json)
meta powerbi show "Organic Leads" --artifact ~/dbt-state/powerbi_index.json

# 4. Reverse lookup: which reports use a given dbt model?
meta powerbi reports core_amas_accounts
# → workspace / report / dataset / matched BQ tables

# 5. DAX measures + expressions for a report (reads powerbi_raw.json)
meta powerbi measures "Retain"
# → table | measure name | hidden | DAX expression

# 6. Power Query M-expressions — how tables are loaded / transformed
meta powerbi source "Retain"
# → table name + full M-expression text

# 7. Report owners and last-modified metadata
meta powerbi owners "Organic Leads"
# → owners (Owner access) + modified_by + modified_at

# 8. Per-table query cost (7-day, live BigQuery) for a report's tables
meta powerbi cost "Organic Leads"
# → BQ table | dbt model | query_cost_usd | query_count | cache_hit_ratio

# 9. Column-level upstream lineage for a report's filter/join columns
meta powerbi lineage "Organic Leads"
# → dbt model | bq column | upstream ancestors (requires `meta lineage build`)

# Configuration (in ~/.config/dbt-meta/config.toml)
[powerbi]
enabled = true
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-secret"
workspaces = ["workspace-id-1", "workspace-id-2"]
```

### Search and Discovery

```bash
# Simple substring search (old command)
meta models staging

# Advanced filtering with selectors
meta list tag:daily                           # Models with 'daily' tag
meta list config.materialized:incremental     # Incremental models
meta list config.meta.domain:its              # Nested config key (dotted path)
meta list path:models/core/                   # Models in specific folder

# Multiple selectors with OR logic (default)
meta list tag:daily tag:core
# → Returns models with 'daily' OR 'core' tag

# Multiple selectors with AND logic
meta list tag:daily tag:core --and
# → Returns models with both 'daily' AND 'core' tags

# Git-aware filtering
meta list -m                                  # Show modified models
# For chain-aware refresh planning: `meta optimize refresh -m`

# Group by tag combinations
meta list tag:daily tag:core --group
# → Groups results by tag combinations

# JSON output for scripting
meta list tag:daily -j | jq -r '.[].model'

# Search models by description (different from list)
meta search "customer dimension" -j | jq -r '.[] | .name'

# Get file path for editing
meta path customers
# → models/marts/customers.sql
```

## ⚙️ Configuration

**Priority:** CLI flags > TOML config > Environment variables > Defaults

### Simple Mode (Zero Configuration)

**No configuration needed!** Just run `dbt compile` and start using:

```bash
cd ~/my-dbt-project
dbt compile
meta schema customers  # ✓ Works immediately with ./target/manifest.json
```

### TOML Configuration (Recommended)

```bash
# Create config file with template
meta settings init
```

Edit `~/.config/dbt-meta/config.toml`:

```toml
# Manifest paths
prod_manifest_path = "~/dbt-state/manifest.json"
dev_manifest_path = "./target/manifest.json"

# Dev environment
dev_schema = "personal_myname"

# Fallback behavior
fallback_dev_enabled = true      # Try dev manifest if model not in prod
fallback_bigquery_enabled = true # Query BigQuery if model not in manifests

# Production naming (optional)
prod_table_name_strategy = "alias_or_name"  # alias_or_name | name | alias
prod_schema_source = "config_or_model"      # config_or_model | model | config
```

**Config file locations** (priority order):
1. `./.dbt-meta.toml` - Project-local config
2. `~/.config/dbt-meta/config.toml` - User config (XDG standard)
3. `~/.dbt-meta.toml` - Fallback location

**Settings commands:**

```bash
meta settings show      # View current configuration
meta settings validate  # Check config file for errors
meta settings path      # Show active config file path
```

### Environment Variables (Alternative)

All TOML settings can be set via environment variables. **CLI flags > TOML > env vars > defaults.**

**Manifest & catalog:**

| TOML key | Environment variable | Default |
|----------|---------------------|---------|
| `prod_manifest_path` | `DBT_PROD_MANIFEST_PATH` | `~/dbt-state/manifest.json` |
| `dev_manifest_path` | `DBT_DEV_MANIFEST_PATH` | `./target/manifest.json` |
| `prod_catalog_path` | `DBT_PROD_CATALOG_PATH` | `~/dbt-state/catalog.json` |
| `dev_catalog_path` | `DBT_DEV_CATALOG_PATH` | `./target/catalog.json` |
| `dev_schema` | `DBT_DEV_SCHEMA` | `personal_{USER}` |

**Fallback behavior:**

| TOML key | Environment variable | Default |
|----------|---------------------|---------|
| `fallback_dev_enabled` | `DBT_FALLBACK_TARGET` | `true` |
| `fallback_bigquery_enabled` | `DBT_FALLBACK_BIGQUERY` | `true` |
| `fallback_catalog_enabled` | `DBT_FALLBACK_CATALOG` | `true` |

**Production naming strategy:**

| TOML key | Environment variable | Options |
|----------|---------------------|---------|
| `prod_table_name_strategy` | `DBT_PROD_TABLE_NAME` | `alias_or_name` (default), `name`, `alias` |
| `prod_schema_source` | `DBT_PROD_SCHEMA_SOURCE` | `config_or_model` (default), `model`, `config` |

**Power BI integration (optional):**

| TOML key | Environment variable |
|----------|---------------------|
| `powerbi.enabled` | `POWERBI_ENABLED` |
| `powerbi.tenant_id` | `POWERBI_TENANT_ID` |
| `powerbi.client_id` | `POWERBI_CLIENT_ID` |
| `powerbi.client_secret` | `POWERBI_CLIENT_SECRET` |
| `powerbi.workspaces` | `POWERBI_WORKSPACES` (comma-separated) |

Index artifact path for `powerbi find` / `powerbi show`: `DBT_PROD_POWERBI_PATH`
(default discovery: `~/dbt-state/powerbi_index.json`).

## 🧪 Development

### Running Tests

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage
pytest --cov=dbt_meta --cov-report=html

# Run specific test categories
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests only
pytest -m performance       # Performance benchmarks

# Run tests in parallel
pytest -n auto
```

### Code Quality

```bash
# Type checking
mypy src/dbt_meta

# Linting
ruff check src/dbt_meta

# Formatting
ruff format src/dbt_meta
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Write tests for new features (maintain 90%+ coverage)
- Follow type hints (mypy strict mode)
- Use ruff for formatting and linting
- Add docstrings for public APIs
- Update README with new features

## 📄 License

Copyright © 2025 [Pavel Filianin](https://github.com/Filianin)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

---

**Built with ❤️ for the dbt community**
