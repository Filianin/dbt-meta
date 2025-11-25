# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.0
