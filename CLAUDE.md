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
- `conftest.py` - Shared fixtures (uses dynamic `test_model` fixture)

**Excluded from coverage:**
- `cli.py` - UI layer (tested manually)
- `manifest/finder.py` - Auto-discovery logic

## Important Code Locations

| Feature | Location |
|---------|----------|
| Manifest discovery | `manifest/finder.py:26-89` |
| Parser caching | `commands.py:20-34`, `manifest/parser.py:28-58` |
| BigQuery fallback | `commands.py:399-446` |
| Dev schema resolution | `commands.py:934-1042` |
| Prod table naming | `commands.py:452-493` |
| Lineage traversal | `commands.py:773-805` |
| Help formatting | `cli.py:43-157` |

## Environment Variables

**Manifest:**
- `DBT_PROD_MANIFEST_PATH` - Production manifest path (default: `~/dbt-state/manifest.json`)
- `DBT_DEV_MANIFEST_PATH` - Dev manifest path (default: `./target/manifest.json`)

**Naming:**
- `DBT_PROD_TABLE_NAME` - `alias_or_name` (default), `name`, `alias`
- `DBT_PROD_SCHEMA_SOURCE` - `config_or_model` (default), `model`, `config`
- `DBT_DEV_SCHEMA` - Full dev schema override
- `DBT_DEV_SCHEMA_TEMPLATE` - Template with `{username}`
- `DBT_DEV_SCHEMA_PREFIX` - Prefix (default: `personal`)
- `DBT_USER` - Override username

**BigQuery:**
- `DBT_FALLBACK_BIGQUERY` - Enable fallback (default: `true`)

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
