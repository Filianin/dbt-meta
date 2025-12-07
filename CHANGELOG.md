# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[0.2.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.2.0
[0.1.6]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.6
[0.1.5]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.5
[0.1.4]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.4
[0.1.3]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.3
[0.1.2]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.2
[0.1.1]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.1
[0.1.0]: https://github.com/Filianin/dbt-meta/releases/tag/v0.1.0
