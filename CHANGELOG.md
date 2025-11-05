# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2025-11-05

### Added

#### Universal `--dev` Flag Support
- **All 10 model commands now support `--dev` flag**:
  - **Metadata**: `schema`, `columns`, `info`, `config`, `docs`
  - **Code**: `sql`, `path`
  - **Lineage**: `deps`, `parents`, `children`
  - Prioritizes dev manifest (`target/`) over production (`.dbt-state/`)
  - Returns dev schema names and uses model filename (not alias)
  - Short form: `-d`

#### Simplified Dev Naming Configuration
- **`DBT_DEV_DATASET`** - Simple constant for dev dataset name
  - Example: `export DBT_DEV_DATASET="personal_pavel_filianin"`
  - Replaces complex 4-level priority system

- **`DBT_DEV_TABLE_PATTERN`** - Flexible table naming (default: `name`)
  - **Predefined**: `name` (filename), `alias` (config alias with fallback)
  - **Custom with 6 placeholders**: `{name}`, `{alias}`, `{username}`, `{folder}`, `{model_name}`, `{date}`
  - Examples: `tmp_{name}`, `{username}_{name}`, `{name}_{date}`
  - Invalid placeholders → warning + fallback to `name`

#### Three-Level Fallback System
- **LEVEL 1**: Production manifest (`.dbt-state/manifest.json`)
- **LEVEL 2**: Dev manifest (`target/manifest.json`) - for defer-built models
- **LEVEL 3**: BigQuery metadata (`bq show`)
- Environment variables: `DBT_FALLBACK_TARGET`, `DBT_FALLBACK_BIGQUERY` (default: `true`)

#### Intelligent Warning System
- **Automatic git change detection** - Commands automatically check if model is modified
  - Warns when querying production but model is modified in git → suggest `--dev`
  - Warns when using `--dev` flag but model is not modified → suggest removing flag
  - Warns when using `--dev` but dev manifest not found → suggest `defer run`
  - **Machine-readable JSON warnings** - All stderr output is JSON when using `-j` flag
    - Git warnings: `{"warnings": [{"type": "git_mismatch", "severity": "warning", ...}]}`
    - Fallback warnings: `{"type": "dev_manifest_fallback", "severity": "warning", ...}`
    - BigQuery fallback: `{"type": "bigquery_fallback", "severity": "warning", ...}`
  - Color-coded text warnings without `-j` flag (yellow ⚠️) for humans

### Changed

#### Breaking Changes
- **Removed `is-modified` CLI command** - Now internal helper function
  - Migration: Remove scripts using `meta is-modified`
  - Alternative: Use `git diff` directly or check stderr warnings from other commands
  - Git detection now automatic via warning system
- **Removed `schema-dev` command** → use `schema --dev` instead

#### Internal Improvements
- Function signatures: added `use_dev: bool = False` to all 10 model commands
- New helper functions:
  - `_calculate_dev_schema()` - simplified dev schema calculation
  - `_validate_dev_dataset()` - BigQuery validation
  - `_build_dev_table_name()` - supports 6 placeholders with error handling
  - `_build_dev_schema_result()` - builds dev schema result
  - `is_modified()` - internal helper for git change detection (no longer public API)
  - `_check_manifest_git_mismatch()` - warns when git status doesn't match command
  - `_find_dev_manifest()` - locates target/manifest.json
- Manifest parser: LRU cache increased from 1 to 2 entries

### Deprecated
- `DBT_DEV_SCHEMA`, `DBT_DEV_SCHEMA_TEMPLATE`, `DBT_DEV_SCHEMA_PREFIX` → use `DBT_DEV_DATASET`
- These still work with deprecation warnings

### Testing
- **145 tests passing** (was 110 in v0.1.0, was 151 before refactor)
  - Removed 6 tests for `is-modified` CLI command (no longer public API)
  - Git detection now tested indirectly through warning system
- Added 35 new tests in v0.2.1:
  - 10 tests for --dev flag behavior
  - 13 tests for dev table pattern placeholders
  - 12 tests for three-level fallback system

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

[Unreleased]: https://github.com/Filianin/dbt-meta/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/Filianin/dbt-meta/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Filianin/dbt-meta/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.0
