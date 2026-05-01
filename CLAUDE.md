# CLAUDE.md

## Project Overview

**dbt-meta** is an AI-first CLI tool for extracting metadata from dbt's `manifest.json`.

**Key Design Principles:**
- Performance-first: LRU caching, orjson parser, lazy loading
- AI-optimized: JSON output mode, deterministic responses
- Production-first: Automatically prioritizes production manifest
- Fallback-enabled: BigQuery fallback when models missing from manifest

## Command Inventory

Complete list of commands, implemented in `cli.py` and routed to `command_impl/*` (or `commands.py` wrappers). All commands accept `-h/--help`; almost all accept `-j/--json`. Global `--manifest PATH` and `-d/--dev` are available on commands that read manifest data.

**Core metadata** (require model name, support `-j`, `-d`):
- `info <model>` — name, database, schema, table, full_name, materialized, file, tags
- `schema <model>` — single full table name (e.g. `project.schema.table`)
- `columns <model>` — column names + data types (catalog → BigQuery fallback)
- `config <model>` — full dbt config dict
- `sql <model>` — compiled SQL; `--jinja` returns raw SQL
- `path <model>` — relative `.sql` file path
- `docs <model>` — column names + types + descriptions
- `deps <model>` — `{refs, sources, macros}`

**Lineage** (require model name, support `-j`, `-d`, `-a/--all`):
- `parents <model>` — direct parents; `-a` for all ancestors (tree/nested JSON ≤20 nodes)
- `children <model>` — direct children; `-a` for all descendants

**Discovery** (no model name required):
- `list [selectors…]` — filter by selectors (`tag:`, `config.key:value`, `path:dir/`, `package:name`)
  - Flags: `-j`, `-d`, `-m/--modified`, `-f/--full-refresh`, `-a/--all` (tree view, only with `-f`), `--and`, `--group`
  - Default output: space-separated model names (copy/paste-friendly)
  - `--group`: grouped by tag combinations with headers
- `models [pattern]` — simple substring search; `-j`
- `search <query>` — search by name or description; `-j`

**SQL validation** (BigQuery dry run; support `-j`, `-d`):
- `validate <model>` — syntax check (✅ Valid / ❌ Error, exit 1)
- `scan <model>` — scan size estimate (🟢 <1GB, 🟡 1–10GB, 🔴 ≥10GB)

**Optimization** (require `dbt_bigquery_monitoring`; support `-j`):
- `hotspots` — ranked optimization candidates
  - `-n/--limit N` (default: 10)
  - `--min-gb GB` (default: 1.0)
- `analyze <model>` — deep single-model analysis
- `branch <model>` — upstream/downstream partitioning/clustering alignment

**Integration** (support `-j`):
- `powerbi [workspace_id]` — Power BI datasets → BigQuery tables → dbt models
  - `--measures` — include DAX measure expressions
  - `--columns` — include column schemas
  - `--full` — include all metadata (measures + columns)
  - `--by-table` — aggregated view grouped by BigQuery table

**Artifacts:**
- `refresh` — sync `~/dbt-state/` from remote storage (always `--force`)
- `refresh --dev` — run `dbt parse --target dev` to populate `./target/manifest.json`

**Settings** (subcommands under `meta settings`):
- `settings init [-f/--force]` — create `~/.config/dbt-meta/config.toml` from template
- `settings show [-j]` — display merged configuration (TOML + env)
- `settings validate` — validate active config file
- `settings path` — print path to active config file

**Global flags** (main app):
- `-v/--version` — print version and exit
- `-h/--help` — show help panels
- `--manifest PATH` — explicit manifest path (takes precedence over `-d/--dev`; no short form)
- `-d/--dev` — use dev manifest and dev schema (`personal_USERNAME`)
- `-j/--json` — JSON output (available on virtually every command)

**Combined short flags** work in any order: `-dj`, `-adj`, `-mf`, `-fa`, etc.

## Development Setup

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests (90%+ coverage required)
pytest --cov=dbt_meta

# Type checking + linting
mypy src/dbt_meta && ruff check src/dbt_meta
```

## Development Guidelines

### File Management

- **NO temporary files in `/tmp/`** - Save all files in project root instead
- Temporary files should be visible in git (easy to review and discard)
- Test scripts, debug files, analysis files - all go in project root
- Example: `./test_catalog_fallback.sh` instead of `/tmp/test_catalog_fallback.sh`

## Architecture

### Module Structure

```
src/dbt_meta/
├── cli.py                 # Typer CLI + Rich formatting (help panels, command wiring)
├── commands.py            # Thin command wrappers + BigQuery fallback helpers
├── errors.py              # Exception hierarchy (DbtMetaError + subclasses)
├── config.py              # Config (TOML + env, XDG), with Power BI section
├── fallback.py            # 3-level fallback strategy (prod → dev → BigQuery)
├── command_impl/          # Per-command implementations
│   ├── base.py            # Shared command orchestration (state detection, fallback)
│   ├── info.py, schema.py, columns.py, config.py, deps.py, sql.py, path.py
│   ├── parents.py, children.py, lineage_utils.py
│   ├── validate.py, scan.py
│   ├── analyze.py, hotspots.py, branch.py
│   └── powerbi.py
├── manifest/
│   ├── parser.py          # Fast manifest parsing (orjson + caching)
│   └── finder.py          # Manifest discovery (prod > dev > explicit)
├── catalog/
│   └── parser.py          # catalog.json parser (fast column lookup, mtime-based staleness)
├── utils/
│   ├── __init__.py        # Parser caching, warnings
│   ├── bigquery.py        # `bq` CLI wrapper, dry run, retry, path discovery
│   ├── compiled_sql.py    # 3-level compiled SQL lookup (manifest → disk → dbt compile)
│   ├── monitoring.py      # dbt_bigquery_monitoring queries
│   ├── powerbi.py         # Power BI Admin API client (OAuth via curl)
│   ├── git.py             # Git state detection (modified/new/deleted)
│   ├── dev.py             # Dev schema resolution
│   └── model_state.py     # Model state classification
└── templates/
    └── dbt-meta.toml      # Config template used by `settings init`
```

### Key Patterns

#### 1. Three-Level Caching Strategy

```python
# Level 1: Parser instance caching (commands.py:20-34)
@lru_cache(maxsize=1)
def _get_cached_parser(manifest_path: str) -> ManifestParser

# Level 2: Manifest lazy loading (manifest/parser.py:28-58)
@cached_property
def manifest(self) -> Dict[str, Any]

# Level 3: orjson for fast parsing (6-20x faster than stdlib)
```

**Result:** Sub-10ms response times after first command.

**CRITICAL:** Always use `_get_cached_parser()`, never instantiate `ManifestParser` directly.

#### 2. Manifest Discovery (3-level priority)

```
1. --manifest PATH (explicit CLI flag - highest priority)
2. DBT_DEV_MANIFEST_PATH (when --dev flag used, default: ./target/manifest.json)
3. DBT_PROD_MANIFEST_PATH (production, default: ~/dbt-state/manifest.json)
```

**Critical distinction:**
- Production manifest uses `config.alias` for table names
- Dev manifest uses SQL filename for table names
- When both `--manifest` and `--dev` are used, `--dev` is ignored with a warning
- Always use `--dev` flag for dev tables

#### 3. BigQuery Fallback Pattern

```python
model = parser.get_model(model_name)
if not model:
    if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):
        dataset, table = _infer_table_parts(model_name)
        bq_metadata = _fetch_table_metadata_from_bigquery(dataset, table)
        # Return partial metadata with warning to stderr
```

**Supported:** `schema`, `columns`, `info`, `config`
**Not supported:** `deps`, `sql`, `parents`, `children` (dbt-specific)

#### 3a. Catalog Staleness Logic

For `meta columns`, catalog.json is used as a fast alternative to BigQuery (~10ms vs ~3s).

**Staleness detection uses FILE mtime, not internal generated_at:**

```python
# File mtime > 24h → fallback to BigQuery (CI/CD might be broken)
file_age = parser.get_file_age_hours()
if file_age > 24:
    return fallback_to_bigquery()

# Internal generated_at > 7 days → info message only (no fallback)
internal_age = parser.get_age_hours()
if internal_age > 168:
    print(f"ℹ️  Catalog was generated {days}d {hours}h ago")
```

**Why this design:**
- Catalog is synced from CI/CD on each merge to master
- File mtime indicates when sync happened (fresh = CI working)
- Internal `generated_at` can be old if no schema changes occurred
- Old internal age is not a problem if file is regularly synced

Location: `catalog/parser.py:201-217`, `command_impl/columns.py:270-282`

#### 3b. BigQuery Fallback Schema Resolution

**CRITICAL FIX (v0.1.3):** BigQuery fallback now correctly uses production schema for `MODIFIED_UNCOMMITTED` models.

**Problem solved:**
```
# Before fix (WRONG):
Failed to fetch from: personal_pavel_filianin.stg_google_play__installs_app_version

# After fix (CORRECT):
Fetching from: staging_google_play.installs_app_version
```

**Schema resolution logic in `_fetch_from_bigquery_with_model()`:**

```python
if self.use_dev:
    # Dev mode: use model's schema (from dev manifest)
    schema = model.get('schema', '')
    table = self.model_name  # Full model name
else:
    # Production mode: ALWAYS use prod_model for schema
    # Even if model came from dev manifest fallback
    source_model = prod_model if prod_model else model
    schema = source_model.get('schema', '')
    table = source_model.get('alias') or source_model.get('name', '')
```

**Key principle:**
- `prod_model` is fetched from production manifest during state detection
- Passed to fallback methods for correct schema resolution
- For `MODIFIED_UNCOMMITTED` without `--dev`: uses production schema
- For `NEW_*` states: uses dev schema (correct - they only exist in dev)

Location: `command_impl/columns.py:92-94, 160-167, 200-221`

#### 4. Dev Schema Resolution (2-level priority)

**Dev schema resolution (2-level):**

```python
1. DBT_DEV_SCHEMA - Direct schema name (highest priority)
2. Default: "personal_{username}"
```

**Backward compatibility:** Old variables (`DBT_DEV_DATASET`, `DBT_DEV_SCHEMA_TEMPLATE`, `DBT_DEV_SCHEMA_PREFIX`) show deprecation warning.

Location: `config.py:28-35`, `utils/dev.py:81-93`

#### 5. Exception Hierarchy

**Consistent error handling with typed exceptions**:

```python
# src/dbt_meta/errors.py

DbtMetaError (base)
├── ModelNotFoundError        # Model not in manifest/BigQuery
├── ManifestNotFoundError     # manifest.json not found
├── ManifestParseError        # Invalid JSON in manifest
├── BigQueryError             # BigQuery operation failed
├── GitOperationError         # Git command failed
└── ConfigurationError        # Invalid configuration
```

**All exceptions include:**
- `message`: Human-readable error description
- `suggestion`: Actionable fix (optional)
- Structured data for programmatic handling

**CLI error handling** (`cli.py:45-66`):
```python
try:
    result = commands.schema(manifest_path, model_name)
    # ... handle result
except DbtMetaError as e:
    handle_error(e)  # Rich formatted output with suggestion
```

**Example error output:**
```
Error: Model 'core__clients' not found

Suggestion: Searched in: production manifest, dev manifest
Try: meta list core
```

**Benefits:**
- Consistent error messages across all commands
- Actionable suggestions for users
- Easy to catch and handle in tests
- AI-friendly structured errors

#### 6. Configuration Management

**Centralized configuration with TOML and env var support** (v0.1.0):

```python
# src/dbt_meta/config.py

from dbt_meta.config import Config

# Load from TOML config file (recommended)
config = Config.from_config_or_env()
# Priority: TOML config > Environment variables > Defaults
# Searches: ./.dbt-meta.toml, ~/.config/dbt-meta/config.toml, ~/.dbt-meta.toml

# Load from environment variables only
config = Config.from_env()

# Load from TOML file directly
config = Config.from_toml("/path/to/config.toml")

# Access configuration
config.prod_manifest_path       # ~/dbt-state/manifest.json
config.dev_manifest_path        # ./target/manifest.json
config.fallback_dev_enabled     # True/False
config.fallback_bigquery_enabled # True/False
config.dev_dataset              # personal_username (sanitized)
config.prod_table_name_strategy # alias_or_name | name | alias
config.prod_schema_source       # config_or_model | model | config

# Validate configuration
warnings = config.validate()
for warning in warnings:
    print(f"Warning: {warning}")
```

**Key features:**
- **TOML configuration** - Modern config files with XDG compliance
- **Priority system** - CLI flags > TOML > Env vars > Defaults
- Single source of truth for all configuration
- Automatic path expansion (~ to home directory)
- Boolean parsing with sensible defaults
- Validation with helpful warnings
- Type-safe dataclass with full type hints
- Username sanitization for BigQuery compatibility (replaces all non-alphanumeric chars)

**Dev schema resolution** (2-level):
```python
# Priority 1: Direct schema name
DBT_DEV_SCHEMA = "my_custom_dev_schema"

# Priority 2: Default with username (fallback)
# personal_{username} (from USER env var, sanitized with re.sub(r'[^a-zA-Z0-9_]', '_', username))
```

**Config file locations** (priority order):
1. `./.dbt-meta.toml` - Project-local config
2. `~/.config/dbt-meta/config.toml` - User config (XDG)
3. `~/.dbt-meta.toml` - Fallback

**Settings commands** (v0.1.0):
- `meta settings init` - Create config file from template
- `meta settings show` - Display merged configuration
- `meta settings validate` - Validate config file
- `meta settings path` - Show active config file path

Location: `config.py:24-473`

#### 7. CLI and User Experience

**Help system improvements** (v0.1.0):
```python
# Enable -h short flag for all commands and subcommands
app = typer.Typer(
    name="dbt-meta",
    context_settings={"help_option_names": ["-h", "--help"]},
)

settings_app = typer.Typer(
    help="CLI settings management",
    context_settings={"help_option_names": ["-h", "--help"]},
)
```

**Benefits:**
- Both `-h` and `--help` work for all commands
- Consistent UX across main app and subcommands
- Standard CLI convention

**Minimalist text output** (v0.2.2):
Commands `schema`, `path`, `sql` return clean output without headers or blank lines:

```bash
# schema - simple table name
meta schema core_client__client_info
# → admirals-bi-dwh.core_client.client_info

# path - simple file path
meta path core_client__client_info
# → models/core_client/client_info.sql

# sql - clean SQL code
meta sql core_client__client_info
# → WITH ... (SQL without headers)

# JSON mode - structured output (unchanged)
meta schema -j core_client__client_info
# → {"model_name": "core_client__client_info", "full_name": "admirals-bi-dwh.core_client.client_info"}
```

**Purpose:** Minimalist output optimized for shell scripting (`TABLE=$(meta schema model)`) and AI consumption

**Username sanitization** (v0.1.0):
```python
# config.py:36-57 - _calculate_dev_schema()
username_sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', username)
# Replaces ALL non-alphanumeric characters (dots, hyphens, @, etc.)
# Examples:
# "pavel.filianin" → "pavel_filianin"
# "john-doe" → "john_doe"
# "user@example.com" → "user_example_com"
```

**Why:** BigQuery dataset names only allow letters, numbers, and underscores

Location: `cli.py:26-39`, `config.py:36-57`

#### 8. Fallback Strategy

**3-level fallback system with clean interface**:

```python
# src/dbt_meta/fallback.py

from dbt_meta.fallback import FallbackStrategy, FallbackLevel, FallbackResult
from dbt_meta.config import Config

config = Config.from_env()
strategy = FallbackStrategy(config)

# Try to get model with automatic fallback
result = strategy.get_model(
    model_name="core__clients",
    prod_parser=parser,
    allowed_levels=[
        FallbackLevel.PROD_MANIFEST,
        FallbackLevel.DEV_MANIFEST,
        FallbackLevel.BIGQUERY  # Optional - exclude for deps/sql commands
    ]
)

if result.found:
    print(f"Found in: {result.level.value}")
    print(f"Data: {result.data}")

    # Show warnings (e.g., "Using dev manifest")
    for warning in result.warnings:
        print(f"Warning: {warning}")
else:
    # ModelNotFoundError raised if not found
    pass
```

**Fallback levels (in priority order):**
1. `PROD_MANIFEST` - Production manifest (default source)
2. `DEV_MANIFEST` - Dev manifest (if enabled via `DBT_FALLBACK_TARGET`)
3. `BIGQUERY` - BigQuery metadata (if enabled via `DBT_FALLBACK_BIGQUERY`)

**Key features:**
- Consolidates logic previously duplicated across 10+ commands
- Automatic warning collection at each level
- Configurable allowed levels per command
- Clean error handling with `ModelNotFoundError`
- Type-safe enums and dataclasses

**Usage pattern for commands:**
```python
# commands with BigQuery support (schema, columns, info, config)
allowed_levels = [FallbackLevel.PROD_MANIFEST, FallbackLevel.DEV_MANIFEST, FallbackLevel.BIGQUERY]

# commands without BigQuery support (deps, sql, parents, children)
allowed_levels = [FallbackLevel.PROD_MANIFEST, FallbackLevel.DEV_MANIFEST]
```

Location: `fallback.py:18-198`

**Note:** BigQuery fallback (`_fetch_from_bigquery`) is currently a placeholder (returns None). Full implementation will be added when refactoring `commands.py` in Task 3.

## SQL Validation and Cost Estimation

Two commands use BigQuery dry run (`bq query --dry_run`) for SQL analysis. Both share the same compiled-SQL fallback strategy via `utils/compiled_sql.py:get_compiled_sql()`:

1. **Manifest** — `model['compiled_code']` (populated by `dbt compile`/`dbt run`/`defer run`)
2. **Disk** — `{project_root}/target/compiled/{package_name}/{original_file_path}` (works when user ran `dbt compile` separately from `meta refresh --dev`)
3. **Auto-compile** (only when `--dev`) — runs `dbt compile --select <model> --target dev` (cwd = project root, 180s timeout), then re-reads from disk

Project root is found by walking up from the manifest path looking for `dbt_project.yml`. Package name is extracted from `model['package_name']` or parsed from `unique_id` (`model.<pkg>.<name>`).

**Failure modes** (each returns a clear actionable error):
- `dbt` not on PATH → `"dbt CLI not found in PATH"`
- Compilation fails → captured stderr + suggestion to run manually
- Compile timeout (180s default)
- No `dbt_project.yml` found → suggests running from project directory
- Without `--dev`, no fallback is run → suggests `meta validate --dev <model>` for local changes

**Location:** `utils/compiled_sql.py`, `command_impl/validate.py:55-66`, `command_impl/scan.py:56-67`

### `meta validate` - Validate SQL syntax

Validates compiled SQL against BigQuery without executing.

```bash
meta validate model_name          # Validate production SQL
meta validate --dev model_name    # Validate dev SQL
meta validate -j model_name       # JSON output
```

**Output:**
- Success: `✅ Valid`
- Error: `❌ Error: Unrecognized name: column at [1:8]`

**Returns (JSON):**
```json
{"model": "model_name", "valid": true, "error": null}
```

### `meta scan` - Estimate query scan size

Shows estimated bytes scanned with color-coded output.

```bash
meta scan model_name              # Show scan size
meta scan --dev model_name        # Dev SQL scan size
meta scan -j model_name           # JSON output
```

**Color scheme:**
| Size | Color |
|------|-------|
| < 1 GB | 🟢 green |
| 1-10 GB | 🟡 yellow |
| ≥ 10 GB | 🔴 red |

**Returns (JSON):**
```json
{"model": "model_name", "bytes": 2410311931, "formatted": "2.2 GB", "error": null}
```

**Implementation:**
- `utils/bigquery.py`: `run_dry_run_query()`, `format_bytes()`
- `utils/compiled_sql.py`: `get_compiled_sql()` — 3-level fallback (manifest → disk → dbt compile)
- `command_impl/validate.py`: ValidateCommand
- `command_impl/scan.py`: ScanCommand

## Optimization Commands

Three commands for BigQuery cost optimization using `dbt_bigquery_monitoring` data:

### `meta hotspots` - Find optimization opportunities

Analyzes all tables and scores them for optimization potential.
Returns two blocks: **Top by Score** + **Top Storage Savings**.

```bash
meta hotspots                     # Top 10 by score + top 10 by savings
meta hotspots --limit 20          # Top 20 each
meta hotspots --min-gb 1.0        # Only tables >1GB
meta hotspots -j                  # JSON output
```

**Scoring algorithm (v4) - calibrated in cents (€0.01 = 1pt):**

| Criterion | Points | Threshold |
|-----------|--------|-----------|
| `query_cost` | cost_7d × 100 | direct spend, €0.01/week = 1pt (PRIMARY) |
| `high_scan` | base × log2(freq), max 100 | >10GB/q=20, >1GB=10, >100MB=3 |
| `high_slot` | base × log2(freq), max 75 | >10min/q=15, >2min=8, >30sec=3 |
| `no_partition` | base × log2(freq), max 75 | >100GB=15, >10GB=8, >1GB=3 |
| `no_clustering` | base × log2(freq), max 50 | >100GB=10, >10GB=5, >1GB=2 |
| `low_cache` | wasted_cost × 100, max 200 | cache_hit < 30% |
| `unused` | storage_cost × 100, max 200 | unused >30 days |

**Returns (JSON):**
```json
{
  "hotspots": [
    {
      "model": "core_client__events",
      "table": "core_client.events",
      "score": 115,
      "scoring_details": [
        {
          "criterion": "no_partition",
          "points": 60,
          "value": "12.5 GB",
          "recommendation": "Add partition_by config"
        },
        {
          "criterion": "high_query_cost",
          "points": 50,
          "value": "€3.42/week",
          "recommendation": "Optimize query patterns"
        }
      ],
      "total_gb": 12.5,
      "query_cost_7d": 3.42,
      "slot_hours_7d": 0.5,
      "storage_savings_eur": 0.04
    }
  ],
  "top_storage_savings": [
    {
      "model": "raw_data__events",
      "table": "raw_data.events",
      "total_gb": 450.2,
      "storage_savings_eur": 5.50
    }
  ],
  "summary": {
    "total_tables_analyzed": 450,
    "tables_with_issues": 85,
    "total_size_gb": 21794.4,
    "total_slot_hours_7d": 4.0,
    "total_storage_savings_eur": 39.21,
    "total_query_cost_7d": 44.99
  }
}
```

### `meta analyze` - Deep analysis of single model

Detailed optimization analysis for a specific model.

```bash
meta analyze core_client__events      # Analyze model
meta analyze -j core_client__events   # JSON output
```

**Returns:** Storage metrics, partition info, query patterns, optimization recommendations.

### `meta branch` - Branch-level optimization

Analyze optimization impact of current branch changes.

```bash
meta branch                       # Analyze modified models
meta branch -j                    # JSON output
```

**Implementation:**
- `utils/monitoring.py`: BigQuery monitoring queries
- `command_impl/hotspots.py`: HotspotsCommand
- `command_impl/analyze.py`: AnalyzeCommand
- `command_impl/branch.py`: BranchCommand

## Power BI Integration

Extract BigQuery table usage from Power BI dashboards and map to dbt models.

### Setup

1. Create App Registration in Azure AD with API permissions:
   - `Tenant.Read.All` (Admin consent required)
   - `Dataset.Read.All`

2. Add Service Principal to Security Group and enable in Power BI Admin Portal:
   - "Service principals can call Fabric public APIs"
   - "Service principals can access read-only admin APIs"

3. Configure dbt-meta via TOML or environment variables:

**TOML config (`~/.config/dbt-meta/config.toml`):**
```toml
[powerbi]
enabled = true
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"  # Or use env var
workspaces = ["677db568-5923-4c5b-9b45-f14ec16a2b62"]
```

**Environment variables (override TOML):**
```bash
export POWERBI_ENABLED=true
export POWERBI_TENANT_ID=your-tenant-id
export POWERBI_CLIENT_ID=your-client-id
export POWERBI_CLIENT_SECRET=your-client-secret
export POWERBI_WORKSPACES=workspace1-id,workspace2-id
```

### `meta powerbi` - Extract Power BI table mappings

```bash
meta powerbi                      # Default: datasets → tables (from config workspace)
meta powerbi <workspace_id>       # Specific workspace
meta powerbi -j                   # JSON output (always full metadata)
meta powerbi --by-table           # Group by BigQuery tables (usage view)
meta powerbi --by-table -j        # Table usage as JSON

# Extended metadata flags
meta powerbi --measures           # Include DAX measure expressions (first 3 shown in text)
meta powerbi --columns            # Include column schemas (name, data_type, is_hidden)
meta powerbi --full               # Include both measures and columns
meta powerbi --full -j            # All metadata as JSON
```

**Default view:** Dataset -> Reports -> Tables hierarchy

**Table usage view (`--by-table`):** Aggregates table usage across all datasets/reports

**Metadata flags:**
- `--measures` — DAX expressions (parsed via `parse_dax_references()` for cross-table deps)
- `--columns` — column schemas with data type and visibility
- `--full` — both measures + columns combined
- With `-j`, all metadata is always returned in the JSON output regardless of these flags

**Text output (default):**
```
BI Marketing (12 datasets, 30 reports, 43 BigQuery tables)

Traffic Performance (23 refreshes/30d, last: 2026-01-14 08:00)
   Report: Traffic Performance
   Report: [App] Traffic Performance
   Tables:
      core_appsflyer.app_installs -> core_appsflyer__app_installs
      core_balance_operations.primary_deposits -> core_balance_operations__primary_deposits
      staging_amas.applications (not in manifest)

Summary: 38/43 tables in dbt manifest (88%)
```

**Text output (`--by-table`):**
```
BI Marketing (43 BigQuery tables, 30 reports, 12 datasets)

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ BigQuery Table            ┃ Reports ┃ Datasets ┃ dbt Model               ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ core_appsflyer.app_insta… │       5 │        2 │ core_appsflyer__app_in… │
│ staging_amas.applications │       3 │        1 │ (not in manifest)       │
│ core_balance.deposits     │       2 │        1 │ core_balance__deposits  │
└───────────────────────────┴─────────┴──────────┴─────────────────────────┘

Summary: 38/43 tables in dbt manifest (88%)
```

**JSON output (default):**
```json
{
  "workspace": "BI Marketing",
  "workspace_id": "677db568-...",
  "datasets": [
    {
      "name": "Traffic Performance",
      "id": "dataset-id",
      "reports": ["Traffic Performance", "[App] Traffic Performance"],
      "tables": [
        {"bigquery_table": "core_appsflyer.app_installs", "dbt_model": "core_appsflyer__app_installs", "in_manifest": true},
        {"bigquery_table": "staging_amas.applications", "dbt_model": null, "in_manifest": false}
      ],
      "refresh_count_30d": 23,
      "last_refresh": "2026-01-14T08:00:00Z"
    }
  ],
  "summary": {
    "total_datasets": 12,
    "total_reports": 30,
    "total_tables": 43,
    "tables_in_manifest": 38,
    "tables_not_in_manifest": 5
  }
}
```

**JSON output (`--by-table`):**
```json
{
  "workspace": "BI Marketing",
  "workspace_id": "677db568-...",
  "view": "by_table",
  "tables": [
    {
      "bigquery_table": "core_appsflyer.app_installs",
      "dbt_model": "core_appsflyer__app_installs",
      "in_manifest": true,
      "report_count": 5,
      "dataset_count": 2,
      "datasets": ["Traffic Performance", "User Analytics"],
      "reports": ["Report 1", "Report 2", "Report 3", "Report 4", "Report 5"]
    },
    {
      "bigquery_table": "staging_amas.applications",
      "dbt_model": null,
      "in_manifest": false,
      "report_count": 3,
      "dataset_count": 1,
      "datasets": ["User Analytics"],
      "reports": ["Report 1", "Report 2", "Report 3"]
    }
  ],
  "summary": {
    "total_tables": 43,
    "total_reports": 30,
    "total_datasets": 12,
    "tables_in_manifest": 38,
    "tables_not_in_manifest": 5
  }
}
```

**Implementation:**
- `utils/powerbi.py`: Power BI Admin API functions
- `command_impl/powerbi.py`: PowerBiCommand
- `config.py`: Power BI configuration fields

## Model Listing and Filtering (`meta list`)

**New command (Unreleased):** `meta list` replaces `dbt ls` functionality with AI-optimized output.

**Note:** The old `list` command (simple substring search) has been renamed to `models` for clarity:
- `meta models staging` - Simple substring search in model names
- `meta list tag:daily` - Advanced filtering with selectors

### Features

**Selectors:**
- `tag:name` - Filter by tags (OR logic by default, AND with `--and`)
- `config.materialized:table` - Filter by config values
- `path:models/staging/` - Filter by file path
- `package:dbt_utils` - Filter by package name

**Git-aware filtering:**
- `-m, --modified` - Show only changed/new models
- `-f, --full-refresh` - Show models needing `--full-refresh` (modified + intermediate + downstream)

**Output modes:**
- Default: Space-separated model names (for terminal copy-paste)
- `--group`: Group by tag combinations with headers
- `-j/--json`: Structured metadata (list or dict if `--group`)

### Examples

```bash
# List models with at least ONE tag (OR logic)
meta list tag:verified tag:active

# List models with ALL tags (AND logic)
meta list tag:verified tag:active --and

# Group by tag combinations
meta list tag:verified tag:active --group

# Filter by config
meta list config.materialized:incremental

# Git-aware: show only modified models
meta list -m

# Git-aware: show all models needing --full-refresh
# Includes: modified models + downstream + intermediate models on paths between modified
meta list -f

# JSON output for AI agents
meta list tag:verified -j
```

### `--full-refresh` Algorithm

Determines which models need `--full-refresh` when rebuilding:

1. **Find modified models** - Uses git to detect changed/new models
2. **Find downstream models** - Includes all descendants of modified models
3. **Find intermediate models** - Uses BFS to find models on dependency paths between modified models

**Use case:** When model A and model D are both modified, and there's a dependency chain A → B → C → D, running `meta list -f` returns: `[A, B, C, D]` (all models that need rebuilding for consistent data).

**Implementation:**
- `ls()` - Main command function with all filtering logic
- `_filter_refresh_models()` - Main orchestration for `--full-refresh` flag
- `_find_intermediate_models()` - Find models between modified pairs
- `_find_path_between()` - BFS pathfinding in dependency graph

**Location:** `commands.py:478-880` (full `list` command implementation)

### Output Format Comparison

| Mode | Text Output | JSON Output |
|------|-------------|-------------|
| Default | `model1 model2 model3` | `[{"model": "...", "table": "...", "tags": [...]}]` |
| `--group` | `tag:verified:\nmodel1 model2\n\ntag:active:\nmodel3` | `{"tag:verified": [...], "tag:active": [...]}` |

## Adding a New Command

### 1. Add command function in `commands.py`

```python
def new_command(manifest_path: str, model_name: str) -> Optional[Dict]:
    """Extract metadata"""
    parser = _get_cached_parser(manifest_path)  # MUST use cached parser
    model = parser.get_model(model_name)

    if not model:
        return None  # Or add BigQuery fallback if applicable

    return {'field': model.get('field', '')}
```

### 2. Add CLI command in `cli.py`

```python
@app.command()
def new_command(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json"),
):
    """Command description"""
    manifest_path = get_manifest_path()
    result = commands.new_command(manifest_path, model_name)
    handle_command_output(result, json_output)
```

### 3. Add tests in `test_commands.py`

```python
def test_new_command(prod_manifest, test_model):
    result = commands.new_command(prod_manifest, test_model)
    assert result is not None
    assert 'field' in result
```

### 4. Update help text in `cli.py`

Add to `_build_commands_panel()` if needed.

## Testing Strategy

**Coverage requirement:** 90%+ (pyproject.toml:79)
**Current coverage:** 91% (678 tests)

**Test markers:**
- `@pytest.mark.unit` - Fast unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.performance` - Performance benchmarks
- `@pytest.mark.critical` - Critical security/correctness tests

**Test structure (18 files, consolidated v0.1.4):**

*Core tests:*
- `test_commands.py` (1846 lines) - Command implementations
- `test_infrastructure.py` (1405 lines) - Manifest discovery, warnings, message formatting
- `test_config.py` (548 lines) - Configuration management
- `test_fallback.py` (303 lines) - Fallback strategy

*Feature tests:*
- `test_bigquery.py` (475 lines) - BigQuery utilities, retry logic, path search
- `test_git.py` (480 lines) - Git operations, path validation, security
- `test_catalog.py` (391 lines) - Catalog parser
- `test_table_resolution.py` (312 lines) - Prod/dev table resolution
- `test_model_states.py` (205 lines) - Model state detection
- `test_decision_tree_scenarios.py` (526 lines) - Decision tree scenarios

*Quality tests:*
- `test_errors.py` (418 lines) - Exception hierarchy and handling
- `test_edge_cases.py` (470 lines) - Edge cases and gap coverage
- `test_utils_coverage.py`, `test_command_coverage.py`, etc. - Specific coverage tests

*Fixtures:*
- `conftest.py` - Shared fixtures (uses dynamic `test_model` fixture)

**Test consolidation (v0.1.4):**
- Reduced files: 24 → 18 (-25%)
- Grouped by functional area (BigQuery, Git, Errors)
- Better organization and navigation

**Excluded from coverage:**
- `cli.py` - UI layer (tested manually)
- `manifest/finder.py` - Auto-discovery logic

## Important Code Locations

| Feature | Location |
|---------|----------|
| Exception hierarchy | `errors.py:13-203` |
| Error handler (CLI) | `cli.py:45-66` |
| Configuration management | `config.py:12-139` |
| Fallback strategy | `fallback.py:18-198` |
| Manifest discovery | `manifest/finder.py:26-89` |
| Parser caching | `commands.py:20-34`, `manifest/parser.py:28-58` |
| BigQuery fallback | `commands.py:399-446` |
| BigQuery schema resolution | `command_impl/columns.py:92-94, 160-167, 200-221` |
| BigQuery dry run | `utils/bigquery.py:311-398` |
| Compiled SQL fallback (3-level) | `utils/compiled_sql.py:get_compiled_sql()` |
| SQL validation | `command_impl/validate.py` |
| Scan estimation | `command_impl/scan.py` |
| Optimization hotspots | `command_impl/hotspots.py` |
| Model analysis | `command_impl/analyze.py` |
| Branch analysis | `command_impl/branch.py` |
| Monitoring queries | `utils/monitoring.py` |
| Power BI API | `utils/powerbi.py` |
| Power BI command | `command_impl/powerbi.py` |
| Dev schema resolution | `commands.py:934-1042` (deprecated, use `config.py`) |
| Prod table naming | `commands.py:452-493` |
| Lineage traversal | `commands.py:773-805` |
| Help formatting | `cli.py:106-157` |
| Minimalist CLI output | `cli.py:665-674` (schema), `cli.py:849-850` (sql), `cli.py:958-960` (path) |

## Environment Variables

**Preferred access:** Use `Config.from_env()` for centralized configuration management with validation.

**Manifest:**
- `DBT_PROD_MANIFEST_PATH` - Production manifest path (default: `~/.dbt-state/manifest.json`)
- `DBT_DEV_MANIFEST_PATH` - Dev manifest path (default: `./target/manifest.json`)

**Catalog:**
- `DBT_PROD_CATALOG_PATH` - Production catalog path (default: `~/dbt-state/catalog.json`)
- `DBT_DEV_CATALOG_PATH` - Dev catalog path (default: `./target/catalog.json`)

**Fallback control:**
- `DBT_FALLBACK_TARGET` - Enable dev manifest fallback (default: `true`)
- `DBT_FALLBACK_BIGQUERY` - Enable BigQuery fallback (default: `true`)
- `DBT_FALLBACK_CATALOG` - Enable catalog fallback for columns (default: `true`)

**Naming:**
- `DBT_PROD_TABLE_NAME` - `alias_or_name` (default), `name`, `alias`
- `DBT_PROD_SCHEMA_SOURCE` - `config_or_model` (default), `model`, `config`
- `DBT_DEV_SCHEMA` - Direct dev schema name (overrides default `personal_{username}`)
- `DBT_USER` - Override username for dev schema (default: `$USER`)

**Power BI:**
- `POWERBI_ENABLED` - Enable Power BI integration (default: `false`)
- `POWERBI_TENANT_ID` - Azure AD tenant ID
- `POWERBI_CLIENT_ID` - App registration client ID
- `POWERBI_CLIENT_SECRET` - App registration client secret
- `POWERBI_WORKSPACES` - Comma-separated list of workspace IDs

**Deprecated:**
- `DBT_DEV_DATASET` - Use `DBT_DEV_SCHEMA` instead (backward compatible with warning)
- `DBT_DEV_SCHEMA_TEMPLATE` - Use `DBT_DEV_SCHEMA` instead (no longer supported)
- `DBT_DEV_SCHEMA_PREFIX` - Use `DBT_DEV_SCHEMA` instead (no longer supported)

## Type Checking

**Strict mode enabled** - All functions must have type hints.

```python
from typing import Dict, List, Optional, Any

def command(manifest_path: str, model_name: str) -> Optional[Dict[str, Any]]:
    """Returns None if model not found"""
    ...

def search(manifest_path: str, query: str) -> List[Dict[str, str]]:
    """Always returns list (empty if no results)"""
    ...
```

## Data Source Decision Logic

For understanding how dbt-meta determines which data source to use (prod/dev manifest, BigQuery fallback):

**Reference documentation:**
- `.qa/decision_tree_visual.txt` - Visual decision tree with 5 critical scenarios
- `.qa/data_source_logic.md` - Detailed logic specification

**Key principle:**
When BigQuery fallback is needed (empty columns), ALWAYS use schema from the FOUND model:
- Model found in dev manifest → use dev schema (`personal_user`)
- Model found in prod manifest → use prod schema
- Never re-search production manifest after model is found

**Implementation:**
- `command_impl/base.py:107-151` - Main fallback orchestration
- `command_impl/columns.py:71-87` - BigQuery fallback with correct schema
- `utils/git.py:77-169` - Git status detection and warnings

## Publishing Checklist

1. `pytest && mypy src/dbt_meta && ruff check src/dbt_meta`
2. Update version in `pyproject.toml`, `src/dbt_meta/__init__.py`
3. Update `CHANGELOG.md` with version and date
4. Test: `pip install -e . && meta --version`
5. Tag: `git tag v0.x.0`

## Performance Benchmarks

- First command: 30-60ms (manifest parsing + caching)
- Subsequent commands: 5-10ms (cached parser)
- 865+ models parsed in ~35ms median

**Optimization rules:**
1. Always use `_get_cached_parser()` - never instantiate `ManifestParser` directly
2. Cache results in local variables - avoid repeated manifest access
3. Use generator expressions over list comprehensions when possible
4. Use `@lru_cache` for expensive helper functions
