# dbt-meta

> ⚡ AI-first CLI for dbt metadata extraction

[![Version](https://img.shields.io/badge/version-0.3.0-blue.svg)](https://github.com/Filianin/dbt-meta/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Built for AI](https://img.shields.io/badge/Built_for-AI_Agents-blueviolet.svg)](#-built-for-ai-workflows)
[![Anthropic Recommended](https://img.shields.io/badge/Anthropic-CLI_Tools-success.svg)](https://docs.anthropic.com/en/docs/build-with-claude/tool-use)

**dbt-meta** is a lightning-fast command-line tool that extracts metadata from dbt's `manifest.json`, eliminating the need to parse `.sql` files or query your data warehouse for schema information.

## ✨ Features

- **🎯 Works out-of-box** - Simple Mode: just run `dbt compile` and start using (v0.3.1+)
- **⚡ Lightning fast** - Optimized Python with LRU caching and orjson parser
- **🔄 Production Mode** - Full defer workflow support for multi-project setups
- **📊 AI-friendly JSON** - Machine-readable structured output (`-j` flag)
- **🔍 Rich metadata** - Schema, columns, dependencies, config, compiled SQL
- **🌳 Dependency navigation** - Trace upstream/downstream models
- **🔎 Smart search** - Find models by name or description
- **🎨 Beautiful UI** - Rich terminal formatting with helpful examples
- **⚙️ Flexible naming** - Configurable schema and table naming for any project
- **⚡ Combined flags** - Use `-dj`, `-ajd`, `-jm` for faster typing

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
meta schema customers           # → analytics.customers
meta columns -j orders          # → JSON array of columns
meta deps customers             # → Dependencies list
meta search "customer"          # → Find models

# Get comprehensive help with examples
meta --help
```

### Production Mode (with defer workflow)

```bash
# One-time setup: Set production manifest path
echo 'export DBT_PROD_MANIFEST_PATH=~/dbt-state/manifest.json' >> ~/.zshrc
source ~/.zshrc

# Copy production manifest (one-time or via cron)
cp ~/Projects/my-dbt-project/.dbt-state/manifest.json ~/dbt-state/

# Now works from any directory!
cd /tmp && meta schema customers  # → Uses production manifest

# For dev models (after defer run):
defer run --select customers
meta schema --dev customers      # → personal_USERNAME.customers
meta columns -dj customers       # → Dev columns with JSON output
```

### Combined Flags (faster typing)

```bash
meta schema -dj customers        # → Dev + JSON
meta parents -ajd model          # → All ancestors + JSON + Dev
meta columns -jm ~/path.json m   # → JSON + Custom manifest
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
✅ **Type-safe** - Mypy strict mode, comprehensive test coverage (91%+)

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
| `schema <model>` | BigQuery table name (`--dev` for dev schema) | `meta schema customers`<br>`meta schema --dev customers` |
| `path <model>` | Relative file path to .sql file | `meta path customers` |
| `columns <model>` | Column names and types (`--dev` supported) | `meta columns -j customers`<br>`meta columns -dj customers` |
| `sql <model>` | Compiled SQL (or raw with `--jinja`) | `meta sql customers`<br>`meta sql --jinja customers` |
| `docs <model>` | Column names, types, and descriptions | `meta docs customers` |
| `deps <model>` | Dependencies by type (refs, sources, macros) | `meta deps -j customers` |
| `parents <model>` | Upstream dependencies (`-a` for all ancestors) | `meta parents customers`<br>`meta parents -a customers` |
| `children <model>` | Downstream dependencies (`-a` for all descendants) | `meta children customers`<br>`meta children -a customers` |
| `config <model>` | Full dbt config (29 fields: partition_by, cluster_by, etc.) | `meta config -j customers` |

### Utilities

| Command | Description | Example |
|---------|-------------|---------|
| `list [pattern]` | List all models (optionally filter by pattern) | `meta list`<br>`meta list staging` |
| `search <query>` | Search models by name or description | `meta search "customer"`<br>`meta search "dim_" -j` |
| `refresh` | Refresh manifest (runs `dbt parse`) | `meta refresh` |

### Global Flags

| Flag | Description |
|------|-------------|
| `-j, --json` | Output as JSON (AI-friendly structured data) |
| `-d, --dev` | Use dev manifest and schema |
| `-m, --manifest PATH` | Explicit path to manifest.json |
| `-a, --all` | Recursive mode (parents/children only) |
| `-h, --help` | Show help with examples |
| `-v, --version` | Show version |

**Combined flags**: `-dj`, `-ajd`, `-jm PATH` (order-independent)

## 💡 Common Use Cases

### Querying BigQuery with Correct Table Names

```bash
# Get production table name (eliminates AI hallucinations)
TABLE=$(meta schema -j customers | jq -r '.full_name')
bq query "SELECT * FROM $TABLE LIMIT 10"
# → SELECT * FROM analytics.dim_customers LIMIT 10
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
TABLE=$(meta schema --dev -j customers | jq -r '.full_name')
bq query "SELECT * FROM $TABLE LIMIT 10"
# → SELECT * FROM personal_USERNAME.customers LIMIT 10
```

### Search and Discovery

```bash
# Find all staging models
meta list staging

# Search models by description
meta search "customer dimension" -j | jq -r '.[] | .name'

# Get file path to edit model
vim $(meta path customers)
```

## ⚙️ Configuration

### Simple Mode (Recommended for Single Project)

**No configuration needed!** Just run `dbt compile` and start using:

```bash
cd ~/my-dbt-project
dbt compile
meta schema customers  # ✓ Works immediately with ./target/manifest.json
```

### Production Mode (Multi-Project / Defer Workflow)

Set production manifest path to work from any directory:

```bash
# Add to ~/.bashrc or ~/.zshrc
export DBT_PROD_MANIFEST_PATH=~/dbt-state/manifest.json

# Auto-update manifest via cron (recommended)
# Every hour: copy production manifest to central location
0 * * * * cp ~/Projects/my-dbt-project/.dbt-state/manifest.json ~/dbt-state/
```

Now `meta` commands work from anywhere and always use production manifest.

### Advanced: Naming Configuration

**dbt-meta** provides flexible naming configuration for complex projects.

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
meta schema --dev customers
# → my_custom_schema.customers
# Ignores all other variables

# Priority 2: Template with {username} placeholder
export DBT_DEV_SCHEMA_TEMPLATE="dev_{username}"
meta schema --dev customers
# → dev_alice.customers

# Template examples:
export DBT_DEV_SCHEMA_TEMPLATE="{username}_sandbox"
# → alice_sandbox.customers

export DBT_DEV_SCHEMA_TEMPLATE="analytics_{username}_v2"
# → analytics_alice_v2.customers

export DBT_DEV_SCHEMA_TEMPLATE="{username}"
# → alice.customers (no prefix)

# Priority 3: Simple prefix
export DBT_DEV_SCHEMA_PREFIX="sandbox"
meta schema --dev customers
# → sandbox_alice.customers

# Empty prefix = no prefix
export DBT_DEV_SCHEMA_PREFIX=""
meta schema --dev customers
# → alice.customers

# Priority 4: Default (LOWEST)
# Uses "personal_{username}" if nothing else is set
meta schema --dev customers
# → personal_alice.customers
```

#### Username Configuration

```bash
# Default: Uses system $USER
meta schema --dev customers
# username = $USER (from system)

# Override username
export DBT_USER="custom_user"
meta schema --dev customers
# username = custom_user

# Dots are automatically replaced with underscores (BigQuery compatibility)
export DBT_USER="john.doe"
meta schema --dev customers
# username = john_doe (dots → underscores)
```

#### Dev Table Naming

Control how dev table names are generated:

```bash
# Default: Uses model filename (NOT alias)
meta schema --dev customers
# → personal_USERNAME.customers (from customers.sql)

# Use alias instead of filename
export DBT_DEV_TABLE_PATTERN="alias"
meta schema --dev customers
# → personal_USERNAME.dim_customers (from config.alias)

# Custom template with placeholders
export DBT_DEV_TABLE_PATTERN="{folder}_{name}"
meta schema --dev staging__customers
# → personal_USERNAME.staging_customers
```

#### Advanced Environment Variables

All environment variables for manifest discovery and naming:

```bash
# Manifest paths
export DBT_PROD_MANIFEST_PATH=~/dbt-state/manifest.json  # Production manifest
export DBT_DEV_MANIFEST_PATH=./target/manifest.json      # Dev manifest (default)

# Dev schema naming (4-level priority)
export DBT_DEV_SCHEMA="custom_schema"                    # Full override (priority 1)
export DBT_DEV_SCHEMA_TEMPLATE="dev_{username}"          # Template (priority 2)
export DBT_DEV_SCHEMA_PREFIX="personal"                  # Prefix (priority 3, default)
export DBT_USER="custom_username"                        # Username override

# Dev table naming
export DBT_DEV_TABLE_PATTERN="alias"                     # alias | name | {template}

# Production naming (advanced)
export DBT_PROD_TABLE_NAME="alias_or_name"              # alias_or_name | name | alias
export DBT_PROD_SCHEMA_SOURCE="config_or_model"         # config_or_model | model | config

# Validation (optional)
export DBT_VALIDATE_BIGQUERY="true"                      # Enable BigQuery name validation
```

# Usage
meta schema --dev jaffle_shop__orders
# → alice.orders
```

**Example 3: Analytics team template**
```bash
# Setup
export DBT_DEV_SCHEMA_TEMPLATE="analytics_{username}_sandbox"
export DBT_PROD_TABLE_NAME="name"

# Usage
meta schema --dev core_client__events
# → analytics_alice_sandbox.events

meta schema core_client__events
# → admirals-bi-dwh.core_client.core_client__events
```

**Example 4: Full schema override for CI/CD**
```bash
# Setup (in CI environment)
export DBT_DEV_SCHEMA="ci_pr_123"

# Usage
meta schema --dev jaffle_shop__customers
# → ci_pr_123.customers (ignores username)
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
