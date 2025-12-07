# dbt-meta

> ⚡ AI-first CLI for dbt metadata extraction

**dbt-meta** is a lightning-fast command-line tool that extracts metadata from dbt's artifacts for DEs and AI agents, eliminating the need to parse `.sql` files or query your data warehouse for schema information. This is especially useful for fast and accurate agent operation, for example, Claude Code.

## ✨ Features

- **🎯 Works out-of-box** - Simple Mode: just run `dbt compile` and start using
- **⚙️ TOML configuration** - Modern config files with XDG compliance (optional)
- **⚡ Lightning fast** - Optimized Python with LRU caching and orjson parser
- **🔄 Production Mode** - Full `defer` workflow support for multi-project setups and development environment
- **📊 AI-friendly JSON** - Machine-readable structured output (`-j` flag)
- **🔍 Rich metadata** - Schema, columns, dependencies, config, compiled SQL
- **🌳 Dependency navigation** - Trace upstream/downstream models
- **🔎 Advanced filtering** - Filter models by tags, config, path with OR/AND logic
- **🔀 Git-aware** - Find modified models and dependencies needing `--full-refresh`
- **📋 Smart search** - Find models by name or description
- **🎨 Beautiful UI** - Rich terminal formatting with helpful examples
- **⚡ Combined flags** - Use `-dj`, `-ajd`, `-mf` for faster typing


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
| `meta info` | ~335ms | Manifest only |
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
meta deps customers             # → Dependencies list
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

### Core Commands

| Command | Description | Example |
|---------|-------------|---------|
| `info <model>` | Model summary (name, schema, table, materialization, tags) | `meta info -j customers` |
| `schema <model>` | Full table name (`database.schema.table`) | `meta schema customers` |
| `path <model>` | Relative file path to .sql file | `meta path customers` |
| `columns <model>` | Column names and types (`--dev` supported) | `meta columns -dj customers` |
| `sql <model>` | Compiled SQL (or raw with `--jinja`) | `meta sql --jinja customers` |
| `docs <model>` | Column names, types, and descriptions | `meta docs customers` |
| `deps <model>` | Dependencies by type (refs, sources, macros) | `meta deps -j customers` |
| `parents <model>` | Upstream dependencies (`-a` for all ancestors) | `meta parents -aj customers` |
| `children <model>` | Downstream dependencies (`-a` for all descendants) | `meta children -a customers` |
| `config <model>` | Full dbt config (29 fields: partition_by, cluster_by, etc.) | `meta config -j customers` |

### Settings & Utilities

| Command | Description | Example |
|---------|-------------|---------|
| `settings init` | Create config file from template | `meta settings init` |
| `settings show` | Display current configuration | `meta settings show -j` |
| `settings validate` | Validate config file | `meta settings validate` |
| `settings path` | Show path to active config file | `meta settings path` |
| `list [selectors]` | Advanced model filtering with selectors (`tag:`, `config.`, `path:`) | `meta list tag:daily --modified` |
| `models [pattern]` | Simple substring search in model names | `meta models staging` |
| `search <query>` | Search models by name or description | `meta search "customer" -j` |
| `refresh` | Refresh manifest (runs `dbt parse`) | `meta refresh` |

**List command flags:**
- `--and` - AND logic for multiple selectors (default: OR)
- `--group` - Group results by tag combinations
- `-m, --modified` - Show only git-modified models
- `-f, --full-refresh` - Show models needing `--full-refresh` (modified + descendants)

### Global Flags

| Flag | Description |
|------|-------------|
| `-j, --json` | Output as JSON (AI-friendly structured data) |
| `-d, --dev` | Use dev manifest and schema |
| `--manifest PATH` | Explicit path to manifest.json |
| `-a, --all` | Recursive mode (parents/children only) |
| `-h, --help` | Show help with examples |
| `-v, --version` | Show version |

**Combined flags**: `-dj`, `-ajd`, `-mf` (order-independent)

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

### Search and Discovery

```bash
# Simple substring search (old command)
meta models staging

# Advanced filtering with selectors
meta list tag:daily                           # Models with 'daily' tag
meta list config.materialized:incremental     # Incremental models
meta list path:models/core/                   # Models in specific folder

# Multiple selectors with OR logic (default)
meta list tag:daily tag:core
# → Returns models with 'daily' OR 'core' tag

# Multiple selectors with AND logic
meta list tag:daily tag:core --and
# → Returns models with both 'daily' AND 'core' tags

# Git-aware filtering
meta list -m                                  # Show modified models
meta list -f                                  # Models needing --full-refresh

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

All TOML settings can be set via environment variables with `DBT_` prefix:

| TOML key | Environment variable |
|----------|---------------------|
| `prod_manifest_path` | `DBT_PROD_MANIFEST_PATH` |
| `dev_manifest_path` | `DBT_DEV_MANIFEST_PATH` |
| `dev_schema` | `DBT_DEV_SCHEMA` |
| `fallback_dev_enabled` | `DBT_FALLBACK_TARGET` |
| `fallback_bigquery_enabled` | `DBT_FALLBACK_BIGQUERY` |

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
