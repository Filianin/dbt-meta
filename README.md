# dbt-meta

> ⚡ AI-first CLI for dbt metadata extraction

[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://github.com/Filianin/dbt-meta/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Built for AI](https://img.shields.io/badge/Built_for-AI_Agents-blueviolet.svg)](#-built-for-ai-workflows)
[![Anthropic Recommended](https://img.shields.io/badge/Anthropic-CLI_Tools-success.svg)](https://docs.anthropic.com/en/docs/build-with-claude/tool-use)

**dbt-meta** is a lightning-fast command-line tool that extracts metadata from dbt's `manifest.json`, eliminating the need to parse `.sql` files or query your data warehouse for schema information.

## ✨ Features

- **⚡ Lightning fast** - Optimized Python with LRU caching and orjson parser
- **🎯 Production-first** - Automatically prioritizes `.dbt-state/manifest.json` (production) over `target/` (dev)
- **🔍 Rich metadata** - Schema, columns, dependencies, config, compiled SQL
- **📊 JSON output** - Machine-readable format for scripting with `jq`
- **🌳 Dependency navigation** - Trace upstream/downstream models
- **🔎 Smart search** - Find models by name or description
- **🎨 Beautiful UI** - Rich terminal formatting with helpful examples
- **⚙️ Flexible naming** - Configurable schema and table naming for any project

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

```bash
# Get production table name (use 'dbt-meta' or shorter 'meta' alias)
dbt-meta schema jaffle_shop__customers
# Output: analytics.jaffle_shop.customers

# Get column list with types
meta columns -j jaffle_shop__orders | jq -r '.[] | "\(.name): \(.type)"'
# Output: order_id: INTEGER, customer_id: INTEGER, order_date: DATE, ...

# Get model dependencies
meta deps -j jaffle_shop__customers | jq '.refs[]'
# Output: ["staging__customers", "staging__orders", "staging__payments"]

# View compiled SQL
meta sql jaffle_shop__customers | less

# Find all models in schema
meta list jaffle_shop

# Search for models
meta search "customers"

# Get comprehensive help with examples
meta --help
```

## 🤖 Built for AI Workflows

**dbt-meta** was specifically designed to eliminate AI agent hallucinations when working with dbt projects.

### The Problem

AI agents (like Claude Code, GitHub Copilot, ChatGPT) often hallucinate when working with dbt:
- ❌ **Wrong table names** - Confusing alias vs filename (`customers` vs `dim_customers`)
- ❌ **Unknown dependencies** - Missing refs/sources in lineage
- ❌ **Incorrect column types** - Using wrong data types in WHERE clauses
- ❌ **Non-existent fields** - Querying columns that don't exist

### The Solution

Following [Anthropic's recommendation](https://docs.anthropic.com/en/docs/build-with-claude/tool-use) to use CLI tools over MCP for AI agents, **dbt-meta** provides:

✅ **Fast** - Optimized Python with caching, no repeated manifest parsing
✅ **Deterministic JSON** - No parsing ambiguity, structured output
✅ **Schema validation** - Prevents hallucinations by providing accurate metadata
✅ **Type-safe** - Mypy strict mode, comprehensive test coverage (95%+)

### Integration

**dbt-meta** integrates seamlessly with:
- Claude Code (Anthropic) - Add to allowed commands in `.claude/settings.local.json`
- GitHub Copilot - Use in terminal and inline suggestions
- ChatGPT / Custom GPTs - Execute commands and parse JSON output
- Other AI agents - Standard CLI interface with JSON output

### Why CLI over MCP?

Anthropic [recommends CLI tools](https://docs.anthropic.com/en/docs/build-with-claude/tool-use) for AI agents because they:
- Have deterministic, structured output
- Are faster and more reliable
- Work in any environment
- Don't require additional infrastructure

**dbt-meta** follows this best practice, providing a lightning-fast, reliable interface for AI agents to access dbt metadata.

## 📚 Commands Reference

### Core Commands

| Command | Description | Example |
|---------|-------------|---------|
| `info <model>` | Model summary (name, schema, table, materialization, tags) | `meta info -j customers` |
| `schema <model>` | Production table name (database.schema.table) | `meta schema jaffle_shop__orders` |
| `schema-dev <model>` | Dev table name (personal_USERNAME.filename) | `meta schema-dev jaffle_shop__orders` |
| `columns <model>` | Column names and types | `meta columns -j customers` |
| `docs <model>` | Column names, types, and descriptions | `meta docs customers` |

### Advanced Metadata

| Command | Description | Example |
|---------|-------------|---------|
| `config <model>` | Full dbt config (29 fields: partition_by, cluster_by, etc.) | `meta config -j model_name` |
| `deps <model>` | Dependencies by type (refs, sources, macros) | `meta deps -j model_name` |
| `sql <model> [--jinja]` | Compiled SQL (default) or raw SQL with `--jinja` | `meta sql model_name` |
| `path <model>` | Relative file path to .sql file | `meta path model_name` |
| `node <model>` | Full node metadata (all fields) | `meta node -j model_name` |

### Dependency Navigation

| Command | Description | Example |
|---------|-------------|---------|
| `parents <model> [--all]` | Upstream dependencies (direct or all ancestors) | `meta parents -j --all model_name` |
| `children <model> [--all]` | Downstream dependencies (direct or all descendants) | `meta children model_name` |

### Search & Discovery

| Command | Description | Example |
|---------|-------------|---------|
| `list [pattern]` | List models (optionally filter by pattern) | `meta list jaffle_shop` |
| `search <query>` | Search by name or description | `meta search "customers" --json` |

### Utilities

| Command | Description | Example |
|---------|-------------|---------|
| `refresh` | Refresh manifest (runs `dbt parse`) | `meta refresh` |

### Global Options

| Option | Description |
|--------|-------------|
| `--json, -j` | Output in JSON format (supported by all commands) |
| `--manifest PATH` | Use specific manifest.json file |
| `--version, -v` | Show version |
| `--help, -h` | Show help with examples |

### Command-Specific Options

| Option | Command | Description |
|--------|---------|-------------|
| `--all` | `parents`, `children` | Recursive mode (get all ancestors/descendants) |
| `--jinja` | `sql` | Show raw SQL with Jinja templates |

## ⚙️ Configuration

### Environment Variables

```bash
# Add to ~/.bashrc or ~/.zshrc

# Path to dbt project root (recommended for global access)
export DBT_PROJECT_PATH=~/Projects/reports

# Production manifest directory (default: .dbt-state)
# If you already download prod manifest for defer functionality, set this path
export DBT_PROD_STATE_PATH=.dbt-state

# Optional: Override manifest.json location completely (highest priority)
export DBT_MANIFEST_PATH=/path/to/custom/manifest.json
```

### Naming Configuration

**dbt-meta** provides flexible naming configuration to match your project's conventions.

#### Production Table Naming

Control how production table names are resolved:

```bash
# Strategy 1: alias_or_name (DEFAULT)
# Uses alias if present, otherwise model name
meta schema model_name
# Model with alias:    → client_profiles_events (from config.alias)
# Model without alias: → DW_report (from model.name)

# Strategy 2: name
# Always prefer model name over alias
export DBT_PROD_TABLE_NAME="name"
meta schema model_name
# Model with alias:    → core_client__client_profiles_events (model.name)
# Model without alias: → DW_report (model.name)

# Strategy 3: alias
# Always prefer alias over model name
export DBT_PROD_TABLE_NAME="alias"
meta schema model_name
# Model with alias:    → client_profiles_events (config.alias)
# Model without alias: → DW_report (fallback to model.name)
```

**Smart Fallback**: All strategies automatically fallback if the preferred value is missing.

#### Production Schema/Database Naming

Control how production schema and database are resolved:

```bash
# Strategy 1: config_or_model (DEFAULT)
# Uses config.schema/database if present, otherwise model.schema/database
meta schema model_name
# → Uses config.schema or model.schema (whichever is set)

# Strategy 2: model
# Always use model.schema and model.database
export DBT_PROD_SCHEMA_SOURCE="model"
meta schema model_name
# → Always uses model.schema and model.database

# Strategy 3: config
# Prefer config.schema/database, fallback to model values
export DBT_PROD_SCHEMA_SOURCE="config"
meta schema model_name
# → Prefers config.schema, falls back to model.schema if not set
```

**Why this matters**: In some dbt projects, schema/database can be overridden in `config` blocks. By default, dbt-meta checks both config and model, preferring config values when present.

#### Dev Schema Naming

Control how dev schema names are generated (4 priority levels):

```bash
# Priority 1: Full schema override (HIGHEST)
export DBT_DEV_SCHEMA="my_custom_schema"
meta schema-dev model_name
# → my_custom_schema.table_name
# Ignores all other variables

# Priority 2: Template with {username} placeholder
export DBT_DEV_SCHEMA_TEMPLATE="dev_{username}"
meta schema-dev model_name
# → dev_pavel_filianin.table_name

# Template examples:
export DBT_DEV_SCHEMA_TEMPLATE="{username}_sandbox"
# → pavel_filianin_sandbox.table_name

export DBT_DEV_SCHEMA_TEMPLATE="analytics_{username}_v2"
# → analytics_pavel_filianin_v2.table_name

export DBT_DEV_SCHEMA_TEMPLATE="{username}"
# → pavel_filianin.table_name (no prefix)

# Priority 3: Simple prefix
export DBT_DEV_SCHEMA_PREFIX="sandbox"
meta schema-dev model_name
# → sandbox_pavel_filianin.table_name

# Empty prefix = no prefix
export DBT_DEV_SCHEMA_PREFIX=""
meta schema-dev model_name
# → pavel_filianin.table_name

# Priority 4: Default (LOWEST)
# Uses "personal_{username}" if nothing else is set
meta schema-dev model_name
# → personal_pavel_filianin.table_name
```

#### Username Configuration

```bash
# Default: Uses system $USER
meta schema-dev model_name
# username = $USER (from system)

# Override username
export DBT_USER="custom_user"
meta schema-dev model_name
# username = custom_user

# Dots are automatically replaced with underscores (BigQuery compatibility)
export DBT_USER="john.doe"
meta schema-dev model_name
# username = john_doe (dots → underscores)
```

#### BigQuery Fallback (Default: Enabled)

**NEW**: `meta schema-dev` now includes **BigQuery fallback** for tables built outside the manifest.

**Use case**: You have a table in `personal_pavel_filianin.conversions_base`, but the model was removed from the project or not yet parsed into `target/manifest.json`.

```bash
# Default behavior (fallback enabled)
meta schema-dev core_google_ads__conversions_base
# 1. Checks target/manifest.json first
# 2. If not found → queries BigQuery: bq show personal_pavel_filianin.conversions_base
# 3. If table exists → returns schema.table
# Output: {"schema": "personal_pavel_filianin", "table": "conversions_base", ...}
# stderr: ⚠️  Model not in manifest, using BigQuery table: personal_pavel_filianin.conversions_base

# Disable fallback (strict manifest-only mode)
export DBT_FALLBACK_BIGQUERY="false"
meta schema-dev core_google_ads__conversions_base
# Error: Model 'core_google_ads__conversions_base' not found
```

**How fallback works**:
1. Tries two naming strategies:
   - Full model name: `core_google_ads__conversions_base` → `personal_pavel.core_google_ads__conversions_base`
   - Last segment only: `core_google_ads__conversions_base` → `personal_pavel.conversions_base`
2. Runs `bq show {dev_schema}.{table_name}` for each candidate (10s timeout)
3. Returns first match, or None if neither exists

**Benefits**:
- ✅ Works with manually created tables in dev schema
- ✅ Supports tables built before manifest was updated
- ✅ Gracefully handles deleted models that still have data
- ✅ No breaking changes (enabled by default)

#### BigQuery Validation (Optional)

**dbt-meta** includes optional BigQuery compatibility validation for schema names. This feature is **opt-in** and won't affect users of other data warehouses (Snowflake, Redshift, etc.).

```bash
# Enable BigQuery validation
export DBT_VALIDATE_BIGQUERY="true"
meta schema-dev model_name

# Validation rules:
# - Replaces invalid characters (dots, @, spaces, etc.) with underscores
# - Ensures name starts with letter or underscore
# - Truncates to 1024 characters max (BigQuery limit)
# - Prints warnings to stderr when sanitization occurs
```

**Example 1: Invalid characters sanitized**
```bash
export DBT_VALIDATE_BIGQUERY="true"
export DBT_USER="user@company.com"
meta schema-dev core_client__events
# Output: personal_user_company_com.events
# Warning: ⚠️  BigQuery validation: Invalid BigQuery characters replaced: '@', '.'
```

**Example 2: Template with dots**
```bash
export DBT_VALIDATE_BIGQUERY="true"
export DBT_DEV_SCHEMA_TEMPLATE="dev-{username}-v2.0"
meta schema-dev core_client__events
# Output: dev-pavel_filianin-v2_0.events
# Warning: ⚠️  BigQuery validation: Invalid BigQuery characters replaced: '.'
```

**Example 3: Name starting with number**
```bash
export DBT_VALIDATE_BIGQUERY="true"
export DBT_USER="123user"
export DBT_DEV_SCHEMA_PREFIX=""
meta schema-dev core_client__events
# Output: _123user.events
# Warning: ⚠️  BigQuery validation: Name must start with letter or underscore
```

**When to enable BigQuery validation:**
- ✅ Your data warehouse is BigQuery
- ✅ You use special characters in usernames (email addresses, dots)
- ✅ You use custom templates with special characters
- ❌ Leave disabled for Snowflake, Redshift, or other DWH

**Recognized values for DBT_VALIDATE_BIGQUERY:**
- Enable: `true`, `True`, `TRUE`, `1`, `yes`, `Yes`, `YES`
- Disable: `false`, `False`, `0`, `no`, empty string, or unset

#### Configuration Examples

**Example 1: Company uses prefixed dev schemas**
```bash
# Setup
export DBT_DEV_SCHEMA_PREFIX="dev"
export DBT_PROD_TABLE_NAME="alias_or_name"

# Usage
meta schema-dev jaffle_shop__customers
# → dev_pavel_filianin.customers

meta schema jaffle_shop__customers
# → analytics.jaffle_shop.customers
```

**Example 2: Personal sandbox without prefix**
```bash
# Setup
export DBT_DEV_SCHEMA_PREFIX=""
export DBT_USER="alice"

# Usage
meta schema-dev jaffle_shop__orders
# → alice.orders
```

**Example 3: Analytics team template**
```bash
# Setup
export DBT_DEV_SCHEMA_TEMPLATE="analytics_{username}_sandbox"
export DBT_PROD_TABLE_NAME="name"

# Usage
meta schema-dev core_client__events
# → analytics_alice_sandbox.events

meta schema core_client__events
# → admirals-bi-dwh.core_client.core_client__events
```

**Example 4: Full schema override for CI/CD**
```bash
# Setup (in CI environment)
export DBT_DEV_SCHEMA="ci_pr_123"

# Usage
meta schema-dev jaffle_shop__customers
# → ci_pr_123.customers (ignores username)
```

### Environment Variables Summary

| Variable | Purpose | Default | Example |
|----------|---------|---------|---------|
| `DBT_PROJECT_PATH` | dbt project root directory | - | `~/Projects/reports` |
| `DBT_PROD_STATE_PATH` | Production manifest directory | `.dbt-state` | `prod-manifest` |
| `DBT_MANIFEST_PATH` | Full manifest path override | - | `/path/to/manifest.json` |
| `DBT_USER` | Override username | `$USER` | `alice` |
| `DBT_DEV_SCHEMA` | Full dev schema (priority 1) | - | `my_dev_schema` |
| `DBT_DEV_SCHEMA_TEMPLATE` | Schema template (priority 2) | - | `dev_{username}` |
| `DBT_DEV_SCHEMA_PREFIX` | Schema prefix (priority 3) | `personal` | `sandbox` |
| `DBT_PROD_TABLE_NAME` | Prod table strategy | `alias_or_name` | `name`, `alias` |
| `DBT_PROD_SCHEMA_SOURCE` | Prod schema/database strategy | `config_or_model` | `model`, `config` |
| `DBT_VALIDATE_BIGQUERY` | BigQuery name validation (opt-in) | disabled | `true`, `1`, `yes` |
| `DBT_FALLBACK_BIGQUERY` | BigQuery fallback for schema-dev | `true` | `false`, `0`, `no` |

### Manifest Priority (Production vs Dev)

After running `defer build`, **two manifest files** exist:

1. **`{DBT_PROD_STATE_PATH}/manifest.json`** (PRODUCTION - preferred, default: `.dbt-state`)
   - Contains production metadata from latest main branch
   - Uses table **aliases** from config (e.g., `core_client.client_info`)
   - Synced automatically by defer script
   - **If you already download prod manifest for defer**, configure the path via `DBT_PROD_STATE_PATH`

2. **`target/manifest.json`** (DEV)
   - Contains dev metadata after `defer build --target dev`
   - Uses **SQL filename** (e.g., `personal_pavel_filianin.dim_client_info`)
   - Only use with `meta schema-dev`

**Manifest search order** (8 priority levels):

1. `DBT_MANIFEST_PATH` environment variable (explicit override)
2. `./{DBT_PROD_STATE_PATH}/manifest.json` **(PRODUCTION - preferred)**
3. `./target/manifest.json` (current directory dev)
4. `$DBT_PROJECT_PATH/{DBT_PROD_STATE_PATH}/manifest.json` **(PRODUCTION)**
5. `$DBT_PROJECT_PATH/target/manifest.json`
6. Search upward for `{DBT_PROD_STATE_PATH}/manifest.json` **(PRODUCTION)**
7. Search upward for `target/manifest.json`
8. `./target/manifest.json` (fallback)

**Note**: `DBT_PROD_STATE_PATH` defaults to `.dbt-state` but can be configured for your project's needs.

**Why this matters:**

```bash
# ❌ WRONG: Query dev table without schema-dev
defer build --select core_client__events
meta schema core_client__events  # Returns PRODUCTION table!
bq query "SELECT * FROM core_client.events"  # Queries PRODUCTION data

# ✅ CORRECT: Use schema-dev for dev tables
defer build --select core_client__events
meta schema-dev core_client__events  # Returns personal_pavel_filianin.events
bq query "SELECT * FROM personal_pavel_filianin.events"  # Queries YOUR changes
```

## 🔄 BigQuery Fallback

**dbt-meta** can fallback to BigQuery when models are not in manifest.json. This is useful when:
- Working with newly created models not yet in production
- Testing tables that exist in BigQuery but not in your local manifest
- Analyzing external tables not managed by dbt

### Supported Commands

| Command | Fallback | Data Source | Limitations |
|---------|----------|-------------|-------------|
| `schema` | ✅ Full | BigQuery table metadata | None |
| `columns` | ✅ Full | BigQuery schema | None |
| `info` | ⚠️ Partial | BigQuery table metadata | Missing: file path, tags, unique_id |
| `config` | ⚠️ Partial | BigQuery partitioning/clustering | Missing: dbt-specific configs (unique_key, incremental_strategy, etc.) |
| `path` | ⚠️ Conditional | Filesystem search | May find wrong file if multiple matches |
| `deps` | ❌ None | Requires manifest | Cannot infer dbt dependencies from BigQuery |
| `sql` | ❌ None | Requires manifest | Use `path` to find source file instead |
| `parents` | ❌ None | Requires manifest | Lineage stored only in manifest.json |
| `children` | ❌ None | Requires manifest | Lineage stored only in manifest.json |

### Configuration

```bash
# Enable fallback (default)
export DBT_FALLBACK_BIGQUERY="true"

# Disable fallback (faster, requires complete manifest)
export DBT_FALLBACK_BIGQUERY="false"
```

**Recognized values:**
- Enable: `true`, `True`, `TRUE`, `1`, `yes`, `Yes`, `YES`
- Disable: `false`, `False`, `FALSE`, `0`, `no`, `No`, `NO`, empty string

### Example Usage

**Model NOT in manifest - fallback to BigQuery:**

```bash
# Get schema for model not in manifest
meta schema core_client__new_model
# ⚠️  Model 'core_client__new_model' not in manifest, using BigQuery table: core_client.new_model
# Output: {"database": "admirals-bi-dwh", "schema": "core_client", "table": "new_model", ...}

# Get columns from BigQuery directly
meta columns -j core_client__new_model
# ⚠️  Model 'core_client__new_model' not in manifest, fetching columns from BigQuery
# Output: [{"name": "id", "data_type": "int64"}, {"name": "created_at", "data_type": "timestamp"}, ...]

# Get partial config (partition/cluster info)
meta config -j core_client__partitioned_table
# ⚠️  Model 'core_client__partitioned_table' not in manifest, using BigQuery config
# ⚠️  Partial config available (dbt-specific settings unavailable)
# Output: {"materialized": "table", "partition_by": "created_at", "cluster_by": ["user_id"], ...}

# Get info (basic metadata)
meta info core_client__new_model
# ⚠️  Model 'core_client__new_model' not in manifest, using BigQuery metadata
# ⚠️  Partial metadata available (missing: file path, tags, unique_id)
# Output: {"name": "core_client__new_model", "database": "admirals-bi-dwh", ...}
```

**Commands without fallback - improved error messages:**

```bash
# Try to get dependencies
meta deps core_client__new_model
# ❌ Dependencies not available for 'core_client__new_model': model not in manifest
#    Dependencies are dbt-specific (refs, sources, macros) and cannot be inferred from BigQuery.
#    Hint: Run 'defer run --select core_client__new_model' to add model to manifest

# Try to get SQL code
meta sql core_client__new_model
# ❌ SQL code not available for 'core_client__new_model': model not in manifest
#    Hint: Use 'meta path core_client__new_model' to locate source file
```

### When Fallback is Disabled

```bash
export DBT_FALLBACK_BIGQUERY="false"

# All commands return None immediately if model not in manifest
meta schema core_client__new_model
# Output: (empty, returns None)

meta columns core_client__new_model
# Output: (empty, returns None)
```

**Use case**: Faster execution when you know all required models are in manifest, or when BigQuery access is not available.

## 💡 Examples

### Before SQL Query Workflow

```bash
# Get table name and validate columns exist
MODEL="core_client__client_profiles_events"

# Extract production table name
BQ_TABLE=$(meta schema -j "$MODEL" | jq -r '"\(.schema).\(.table)"')

# Get column list
columns=$(meta columns -j "$MODEL")

# Verify column exists
echo "$columns" | jq -e '.[] | select(.name == "client_id")' > /dev/null || {
  echo "Error: client_id column not found"
  exit 1
}

# Build and execute query
bq query --format=json --batch --quiet "
SELECT
  client_id,
  event_timestamp,
  event_name
FROM $BQ_TABLE
WHERE DATE(event_timestamp) = CURRENT_DATE()
LIMIT 1000
"
```

### Incremental Model Analysis

```bash
# Check partition and clustering config
meta config -j core_client__events | jq '{
  partition_by,
  cluster_by,
  unique_key,
  incremental_strategy
}'

# Output:
# {
#   "partition_by": "DATE(event_timestamp)",
#   "cluster_by": ["client_id", "event_name"],
#   "unique_key": "event_id",
#   "incremental_strategy": "merge"
# }
```

### Lineage Tracing

```bash
# Find all upstream models (ancestors)
meta parents -j --all dim_client_info | jq -r '.[]'

# Find all downstream models (descendants)
meta children -j --all dim_client_info | jq -r '.[]'

# Visualize full lineage
echo "Upstream models:"
meta parents --all dim_client_info

echo "\nDownstream models:"
meta children --all dim_client_info
```

### Finding Models

```bash
# List all models in core_client schema
meta list -j core_client | jq -r '.[]'

# Search for models containing "revenue"
meta search revenue --json | jq -r '.[] | "\(.name): \(.description)"'

# Find models with specific tag
meta list | while read model; do
  meta info -j "$model" | jq -r 'select(.tags | contains(["finance"])) | .name'
done
```

### Editing Model Files

```bash
# Open model in VS Code
code $(meta path core_client__events)

# View compiled SQL with syntax highlighting
meta sql core_client__events | bat -l sql

# Compare raw vs compiled SQL
diff \
  <(meta sql core_client__events --jinja) \
  <(meta sql core_client__events)
```

### Multi-Environment Workflows

```bash
# Development: Query your personal schema
export DBT_DEV_SCHEMA_PREFIX="dev"
export DBT_USER="alice"

defer build --select core_client__events
DEV_TABLE=$(meta schema-dev -j core_client__events | jq -r .full_name)
bq query "SELECT COUNT(*) FROM $DEV_TABLE"

# Production: Compare with production data
PROD_TABLE=$(meta schema -j core_client__events | jq -r .full_name)
bq query "SELECT COUNT(*) FROM $PROD_TABLE"
```

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

### Performance

**dbt-meta** uses several optimization techniques:

- **LRU Caching**: ManifestParser cached with `@lru_cache(maxsize=1)`
- **orjson**: Fast JSON parsing (2-3x faster than standard json)
- **Lazy loading**: Manifest parsed only when needed
- **Single-pass algorithms**: Optimized list/search operations

Typical performance:
- First command: ~30-60ms (manifest parsing)
- Subsequent commands: ~5-10ms (cached parser)
- 865+ models parsed in ~35ms (median)

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
