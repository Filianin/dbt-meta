# CLAUDE.md

## Project Overview

**dbt-meta** is an AI-first CLI tool for extracting metadata from dbt's `manifest.json`.

**Key Design Principles:**
- Performance-first: LRU caching, orjson parser, lazy loading
- AI-optimized: JSON output mode, deterministic responses
- Production-first: Automatically prioritizes production manifest
- Fallback-enabled: BigQuery fallback when models missing from manifest

## Development Setup

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests (95%+ coverage required)
pytest --cov=dbt_meta

# Type checking + linting
mypy src/dbt_meta && ruff check src/dbt_meta
```

## Architecture

### Module Structure

```
src/dbt_meta/
├── cli.py                # Typer CLI + Rich formatting
├── commands.py           # Command implementations + BigQuery fallback
├── errors.py             # Exception hierarchy (v0.3.0+)
├── config.py             # Configuration management (v0.3.0+)
├── fallback.py           # 3-level fallback strategy (v0.3.0+)
├── utils/                # Utility modules (v0.3.0+)
│   ├── __init__.py       # Parser caching, warnings
│   └── git.py            # Git operations
└── manifest/
    ├── parser.py         # Fast manifest parsing (orjson + caching)
    └── finder.py         # 4-level manifest discovery
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

#### 4. Dev Schema Resolution (4-level priority)

```python
1. DBT_DEV_SCHEMA - Full override
2. DBT_DEV_SCHEMA_TEMPLATE - Template with {username} placeholder
3. DBT_DEV_SCHEMA_PREFIX - Simple prefix (default: "personal")
4. Default: "personal_{username}"
```

Location: `commands.py:979-998`

#### 5. Exception Hierarchy

**Consistent error handling with typed exceptions** (Added in v0.3.0):

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

**Centralized configuration with validation** (Added in v0.3.0):

```python
# src/dbt_meta/config.py

from dbt_meta.config import Config

# Load configuration from environment variables
config = Config.from_env()

# Access configuration
config.prod_manifest_path       # ~/dbt-state/manifest.json
config.dev_manifest_path        # ./target/manifest.json
config.fallback_dev_enabled     # True/False
config.fallback_bigquery_enabled # True/False
config.dev_dataset              # personal_username
config.prod_table_name_strategy # alias_or_name | name | alias
config.prod_schema_source       # config_or_model | model | config

# Validate configuration
warnings = config.validate()
for warning in warnings:
    print(f"Warning: {warning}")
```

**Key features:**
- Single source of truth for all environment variables
- Automatic path expansion (~ to home directory)
- Boolean parsing with sensible defaults
- Validation with helpful warnings
- Type-safe dataclass with full type hints

**Dev schema resolution** (simplified to 2-level):
```python
# Priority 1: Direct schema name
DBT_DEV_DATASET = "my_custom_dev_schema"

# Priority 2: Default with username (fallback)
# personal_{username} (from USER env var)
```

Location: `config.py:24-139`

#### 7. Fallback Strategy

**3-level fallback system with clean interface** (Added in v0.3.0):

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

**Coverage requirement:** 95%+ (pyproject.toml:79)

**Test markers:**
- `@pytest.mark.unit` - Fast unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.performance` - Performance benchmarks

**Test structure:**
- `test_commands.py` - Command implementations
- `test_infrastructure.py` - Manifest discovery + warnings
- `test_errors.py` - Exception hierarchy (v0.3.0+)
- `test_config.py` - Configuration management (v0.3.0+)
- `test_fallback.py` - Fallback strategy (v0.3.0+)
- `conftest.py` - Shared fixtures (uses dynamic `test_model` fixture)

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
| Dev schema resolution | `commands.py:934-1042` (deprecated, use `config.py`) |
| Prod table naming | `commands.py:452-493` |
| Lineage traversal | `commands.py:773-805` |
| Help formatting | `cli.py:43-157` |

## Environment Variables

**Preferred access:** Use `Config.from_env()` (v0.3.0+) for centralized configuration management with validation.

**Manifest:**
- `DBT_PROD_MANIFEST_PATH` - Production manifest path (default: `~/.dbt-state/manifest.json`)
- `DBT_DEV_MANIFEST_PATH` - Dev manifest path (default: `./target/manifest.json`)

**Fallback control:**
- `DBT_FALLBACK_TARGET` - Enable dev manifest fallback (default: `true`)
- `DBT_FALLBACK_BIGQUERY` - Enable BigQuery fallback (default: `true`)

**Naming:**
- `DBT_PROD_TABLE_NAME` - `alias_or_name` (default), `name`, `alias`
- `DBT_PROD_SCHEMA_SOURCE` - `config_or_model` (default), `model`, `config`
- `DBT_DEV_DATASET` - Direct dev schema name (overrides default `personal_{username}`)
- `DBT_USER` - Override username for dev schema (default: `$USER`)

**Deprecated (v0.3.0+):**
- `DBT_DEV_SCHEMA` - Use `DBT_DEV_DATASET` instead
- `DBT_DEV_SCHEMA_TEMPLATE` - Use `DBT_DEV_DATASET` instead
- `DBT_DEV_SCHEMA_PREFIX` - Use `DBT_DEV_DATASET` instead

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
