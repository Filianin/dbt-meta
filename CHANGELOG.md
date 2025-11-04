# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2025-02-04

### Added
- **BigQuery fallback for models not in manifest** - Continue working even when models are missing from manifest.json
  - `schema()` - Full fallback to BigQuery table metadata
  - `columns()` - Full fallback to BigQuery schema
  - `info()` - Partial fallback (missing: file path, tags, unique_id)
  - `config()` - Partial fallback (extracts partition_by, cluster_by from BigQuery)
  - `path()` - Conditional filesystem search fallback
- **Improved error messages** for commands without fallback:
  - `deps()` - Explains dependencies are dbt-specific, suggests `defer run`
  - `sql()` - Suggests using `meta path` to locate source file
  - `parents()` - Explains lineage is stored only in manifest
  - `children()` - Explains lineage is stored only in manifest
- **New helper functions**:
  - `_infer_table_parts()` - Extract dataset and table from dbt model name
  - `_fetch_table_metadata_from_bigquery()` - Fetch metadata from BigQuery with 10s timeout
  - `_fetch_columns_from_bigquery_direct()` - Fetch columns without requiring model in manifest
- **Environment variable**: `DBT_FALLBACK_BIGQUERY` (default: `true`)
  - Controls BigQuery fallback behavior
  - Recognized values: `true`/`1`/`yes` (enable), `false`/`0`/`no` (disable)

### Changed
- `columns()` now fallback to BigQuery even when model not in manifest (previously only when columns empty)
- All BigQuery operations use 10-second timeout
- Warnings printed to stderr with `⚠️` prefix when using fallback

### Documentation
- Added "BigQuery Fallback" section in README with:
  - Supported commands table (Full, Partial, None fallback)
  - Configuration examples
  - Usage examples with expected output
  - When to disable fallback

## [0.1.0] - 2025-01-31

### Added
- Initial release of dbt-meta Python CLI
- Core commands for metadata extraction:
  - `info` - Model summary (name, schema, table, materialization, tags)
  - `schema` - Production table name (database.schema.table)
  - `schema-dev` - Dev table name (personal_USERNAME.filename)
  - `columns` - Column names and types with BigQuery fallback
  - `docs` - Column names, types, and descriptions
  - `node` - Full node details by unique_id or model name
  - `refresh` - Refresh manifest (runs dbt parse)
- Advanced metadata commands:
  - `config` - Full dbt config (29 fields: partition_by, cluster_by, etc.)
  - `deps` - Dependencies by type (refs, sources, macros)
  - `sql` - Compiled SQL (default) or raw SQL with `--jinja` flag
  - `path` - Relative file path to .sql file
- Dependency navigation:
  - `parents` - Upstream dependencies (direct or all ancestors with `--all`)
  - `children` - Downstream dependencies (direct or all descendants with `--all`)
- Search and discovery:
  - `list` - List models (optionally filter by pattern)
  - `search` - Search by name or description
- Flexible naming configuration:
  - Production table naming: `DBT_PROD_TABLE_NAME` (alias_or_name, name, alias)
  - Production schema/database: `DBT_PROD_SCHEMA_SOURCE` (config_or_model, model, config)
  - Dev schema naming: 4-level priority system
    - `DBT_DEV_SCHEMA` - Full override (highest priority)
    - `DBT_DEV_SCHEMA_TEMPLATE` - Template with {username} placeholder
    - `DBT_DEV_SCHEMA_PREFIX` - Simple prefix (default: "personal")
    - Fallback to "personal_{username}"
  - Username configuration: `DBT_USER` (default: $USER)
- BigQuery validation (opt-in):
  - `DBT_VALIDATE_BIGQUERY` - Validate and sanitize schema names for BigQuery
  - Replaces invalid characters, ensures proper starting char, length limits
  - Prints warnings when sanitization occurs
- Manifest priority system:
  - Automatically prioritizes production manifest (`.dbt-state/manifest.json`)
  - Configurable via `DBT_PROD_STATE_PATH` (default: `.dbt-state`)
  - 7-level manifest search order
- Configuration:
  - `DBT_PROJECT_PATH` - Path to dbt project root
  - `DBT_MANIFEST_PATH` - Override manifest.json location (highest priority)
- Output modes:
  - `--json, -j` - JSON output for all commands
  - Rich formatted output with colors and tables (default)
- Installation:
  - `pip install -e .` for development
  - Python 3.9+ required
- Testing:
  - 110 comprehensive tests
  - 95%+ code coverage
  - Edge case testing (empty strings, null values, special characters, priority logic)
  - BigQuery validation tests
- Performance:
  - LRU caching for manifest parsing
  - Fast metadata extraction
- Documentation:
  - Comprehensive README with examples
  - Environment variables summary
  - Naming configuration guide
  - Apache 2.0 license

[Unreleased]: https://github.com/Filianin/dbt-meta/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Filianin/dbt-meta/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.0
