# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**dbt-meta** is an AI-first CLI tool for extracting metadata from dbt's `manifest.json`. It eliminates the need to parse SQL files or query data warehouses for schema information.

**Key Design Principles:**
- Performance-first: LRU caching, orjson parser, lazy loading
- AI-optimized: JSON output mode, deterministic responses, structured metadata
- Production-first: Automatically prioritizes production manifest over dev
- Fallback-enabled: BigQuery fallback when models missing from manifest

## Development Commands

### Setup
```bash
# Install in development mode
pip install -e .

# Install with dev dependencies (required for testing/linting)
pip install -e ".[dev]"

# Verify installation (both aliases work)
meta --version
dbt-meta --version
```

### Testing
```bash
# Run all tests
pytest

# Run with coverage (95%+ required)
pytest --cov=dbt_meta --cov-report=html

# Run specific test categories
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests
pytest -m performance       # Performance benchmarks

# Run tests in parallel
pytest -n auto

# Run specific test file
pytest tests/test_commands.py
```

### Code Quality
```bash
# Type checking (strict mode enabled)
mypy src/dbt_meta

# Linting
ruff check src/dbt_meta

# Formatting (auto-fix)
ruff format src/dbt_meta

# Run all quality checks before committing
mypy src/dbt_meta && ruff check src/dbt_meta && pytest
```

## Architecture

### Module Structure

```
src/dbt_meta/
├── __init__.py           # Package version
├── cli.py                # Typer CLI interface + Rich formatting
├── commands.py           # Command implementations + BigQuery fallback logic
└── manifest/
    ├── __init__.py
    ├── parser.py         # Fast manifest parsing (orjson + caching)
    └── finder.py         # 8-level manifest discovery system
```

### Key Architectural Patterns

#### 1. Lazy Loading + Caching Strategy

**Three-level caching:**
1. **Parser instance caching** (commands.py:20-34):
   ```python
   @lru_cache(maxsize=1)
   def _get_cached_parser(manifest_path: str) -> ManifestParser
   ```
   - Avoids re-instantiating parser for same manifest
   - Cache size=1 since we typically work with one manifest per session

2. **Manifest lazy loading** (manifest/parser.py:28-58):
   ```python
   @cached_property
   def manifest(self) -> Dict[str, Any]
   ```
   - Manifest loaded only on first access
   - Subsequent accesses return cached dict

3. **orjson for fast parsing** (manifest/parser.py:52):
   - 6-20x faster than stdlib json
   - Binary input for optimal performance

**Why this matters:** Enables sub-10ms response times after first command (cached parser).

#### 2. Production-First Manifest Discovery

**Manifest search priority** (manifest/finder.py:26-89):
1. `DBT_MANIFEST_PATH` (explicit override)
2. `./{DBT_PROD_STATE_PATH}/manifest.json` ← **PRODUCTION (preferred)**
3. `./target/manifest.json` (dev)
4. `$DBT_PROJECT_PATH/{DBT_PROD_STATE_PATH}/manifest.json` ← **PRODUCTION**
5. Upward search for production manifest
6. Fallback to dev manifest

**Critical distinction:**
- Production manifest (`.dbt-state/`) uses `config.alias` for table names
- Dev manifest (`target/`) uses SQL filename for table names
- Always use `schema-dev` command for dev tables to get correct names

#### 3. BigQuery Fallback System

**Fallback hierarchy** (commands.py:129-254, 399-446, 496-543, 546-612):

Models not in manifest can still work via BigQuery queries:

```python
# Pattern: Try manifest first, then BigQuery
model = parser.get_model(model_name)
if not model:
    if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):
        # Extract dataset.table from model name
        dataset, table = _infer_table_parts(model_name)
        bq_metadata = _fetch_table_metadata_from_bigquery(dataset, table)
        # Return partial metadata with warning to stderr
```

**Supported commands:**
- `schema`, `columns` - Full fallback (all data available)
- `info`, `config` - Partial fallback (missing dbt-specific fields)
- `path` - Conditional (filesystem search)
- `deps`, `sql`, `parents`, `children` - No fallback (dbt-specific)

**Implementation notes:**
- BigQuery calls timeout after 10 seconds
- Warnings printed to stderr with `⚠️` prefix
- Controlled by `DBT_FALLBACK_BIGQUERY` env var (default: enabled)

#### 4. Environment-Driven Configuration

**Naming strategies are environment-driven** (commands.py:452-493):

```python
# Production table name resolution
table_name_strategy = os.environ.get('DBT_PROD_TABLE_NAME', 'alias_or_name')
if table_name_strategy == 'name':
    table_name = name or alias
elif table_name_strategy == 'alias':
    table_name = alias or name
else:  # 'alias_or_name' (default)
    table_name = alias or name
```

**Dev schema resolution uses 4-level priority** (commands.py:979-998):
1. `DBT_DEV_SCHEMA` - Full override
2. `DBT_DEV_SCHEMA_TEMPLATE` - Template with `{username}` placeholder
3. `DBT_DEV_SCHEMA_PREFIX` - Simple prefix
4. Default: `"personal_{username}"`

**Why this matters:** Different projects have different naming conventions. Environment variables allow per-project configuration without code changes.

## Testing Strategy

### Test Organization (pyproject.toml:69-86)

```python
markers = [
    "unit: Unit tests (fast)",
    "integration: Integration tests (medium)",
    "performance: Performance benchmarks (slow)",
]
```

### Key Test Files

- `test_commands.py` - Command function tests with fixture data
- `test_manifest_parser.py` - Parser logic and caching behavior
- `test_manifest_finder.py` - Manifest discovery priority
- `test_edge_cases.py` - Null values, empty strings, special characters
- `conftest.py` - Shared fixtures (manifest paths, mock data)

### Coverage Requirements

**95%+ coverage required** (pyproject.toml:79)

**Excluded from coverage** (pyproject.toml:89-92):
- `cli.py` - UI layer, tested manually
- `manifest/finder.py` - Utility, auto-discovery logic

### Writing New Tests

When adding new commands or features:

1. Add unit tests in `test_commands.py`:
   ```python
   def test_new_command_success(manifest_prod):
       result = commands.new_command(manifest_prod, "model_name")
       assert result is not None
       assert result['expected_field'] == 'expected_value'
   ```

2. Add edge case tests in `test_edge_cases.py`:
   ```python
   def test_new_command_edge_cases(manifest_prod):
       # Empty string
       assert commands.new_command(manifest_prod, "") is None
       # Non-existent model
       assert commands.new_command(manifest_prod, "nonexistent") is None
   ```

3. Add fixture data if needed in `tests/fixtures/`:
   - Expected outputs in `expected_outputs/`
   - Test manifests in `manifests/`

## Common Development Tasks

### Adding a New Command

1. **Add command function** in `commands.py`:
   ```python
   def new_command(manifest_path: str, model_name: str) -> Optional[Dict]:
       """Extract some metadata"""
       parser = _get_cached_parser(manifest_path)  # Use cached parser
       model = parser.get_model(model_name)

       if not model:
           # Consider BigQuery fallback if applicable
           return None

       return {
           'field': model.get('field', '')
       }
   ```

2. **Add CLI command** in `cli.py`:
   ```python
   @app.command()
   def new_command(
       model_name: str = typer.Argument(..., help="Model name"),
       json_output: bool = typer.Option(False, "-j", "--json"),
       manifest: Optional[str] = typer.Option(None, "--manifest"),
   ):
       """Command description"""
       manifest_path = get_manifest_path(manifest)
       result = commands.new_command(manifest_path, model_name)
       handle_command_output(result, json_output)
   ```

3. **Add tests** in `test_commands.py`:
   ```python
   def test_new_command(manifest_prod):
       result = commands.new_command(manifest_prod, "test_model")
       assert result is not None
   ```

4. **Update help text** in `cli.py` if needed (see `_build_commands_panel()`)

### Working with BigQuery Fallback

**When to add fallback:**
- Command extracts metadata that can be inferred from BigQuery
- Examples: table schema, column types, partitioning config

**When NOT to add fallback:**
- Command requires dbt-specific data (refs, sources, macros, SQL code)
- Lineage information (parents, children)

**Fallback pattern:**
```python
model = parser.get_model(model_name)

if not model:
    # Check if fallback enabled
    if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):
        dataset, table = _infer_table_parts(model_name)
        if dataset:
            bq_metadata = _fetch_table_metadata_from_bigquery(dataset, table)
            if bq_metadata:
                # Print warning to stderr
                print(f"⚠️  Model '{model_name}' not in manifest, using BigQuery",
                      file=sys.stderr)
                # Return partial/full metadata
                return {...}
    return None
```

### Performance Optimization

**Current benchmarks:**
- First command: 30-60ms (manifest parsing + caching)
- Subsequent commands: 5-10ms (cached parser)
- 865+ models parsed in ~35ms median

**Optimization checklist:**
1. Use `_get_cached_parser()` - never instantiate ManifestParser directly
2. Avoid repeated manifest access - cache results in local variables
3. Use generator expressions for filtering instead of list comprehensions when possible
4. Consider `@lru_cache` for expensive helper functions called multiple times

## Important Code Locations

- **Manifest discovery logic**: manifest/finder.py:26-89
- **Caching strategy**: commands.py:20-34, manifest/parser.py:28-58
- **BigQuery fallback pattern**: commands.py:399-446 (schema command example)
- **Dev schema resolution**: commands.py:934-1042
- **Environment variable handling**: commands.py:452-493 (prod naming), 979-998 (dev naming)
- **Recursive lineage traversal**: commands.py:773-805
- **Help text formatting**: cli.py:43-142

## Environment Variables Reference

**Manifest Discovery:**
- `DBT_MANIFEST_PATH` - Override manifest location (highest priority)
- `DBT_PROJECT_PATH` - dbt project root directory
- `DBT_PROD_STATE_PATH` - Production manifest dir (default: `.dbt-state`)

**Naming Configuration:**
- `DBT_PROD_TABLE_NAME` - Production table strategy: `alias_or_name` (default), `name`, `alias`
- `DBT_PROD_SCHEMA_SOURCE` - Schema/database strategy: `config_or_model` (default), `model`, `config`
- `DBT_DEV_SCHEMA` - Full dev schema override (priority 1)
- `DBT_DEV_SCHEMA_TEMPLATE` - Template with `{username}` placeholder (priority 2)
- `DBT_DEV_SCHEMA_PREFIX` - Schema prefix (priority 3, default: `personal`)
- `DBT_USER` - Override username (default: `$USER`)

**BigQuery:**
- `DBT_FALLBACK_BIGQUERY` - Enable BigQuery fallback (default: `true`)
- `DBT_VALIDATE_BIGQUERY` - Validate/sanitize BigQuery names (opt-in, default: disabled)

## Type Checking

**Strict mode enabled** (pyproject.toml:64-66):
- All functions must have type hints
- Use `Optional[T]` for nullable returns
- Use `Dict[str, Any]` for manifest data (dynamic structure)

**Common patterns:**
```python
from typing import Dict, List, Optional, Any

def command(manifest_path: str, model_name: str) -> Optional[Dict[str, Any]]:
    """Returns None if model not found"""
    ...

def search(manifest_path: str, query: str) -> List[Dict[str, str]]:
    """Always returns list (empty if no results)"""
    ...
```

## Git Workflow

- **Main branch**: Direct commits (no PRs for small project)
- **Version tags**: Follow semver (v0.1.0, v0.2.0, etc.)
- **Changelog**: Update CHANGELOG.md before version bumps

## Publishing Checklist

Before publishing a new version:

1. Run full test suite: `pytest`
2. Check types: `mypy src/dbt_meta`
3. Check linting: `ruff check src/dbt_meta`
4. Update version in `pyproject.toml`
5. Update `CHANGELOG.md` with new version and date
6. Update version in `src/dbt_meta/__init__.py`
7. Test installation: `pip install -e .`
8. Verify commands work: `meta --version`, `meta --help`
9. Tag release: `git tag v0.x.0`

## Troubleshooting

**Tests failing with "Model not found":**
- Check if test manifest has the model
- Verify model name matches unique_id pattern: `model.project.name`

**Type errors from mypy:**
- Ensure all function signatures have type hints
- Use `Any` for manifest data (dynamic structure)
- Check for `None` returns - use `Optional[T]`

**Coverage below 95%:**
- Check which lines are uncovered: `pytest --cov=dbt_meta --cov-report=term-missing`
- Add tests for uncovered branches
- Consider if code should be in excluded files (cli.py, finder.py)

**Performance regression:**
- Verify `_get_cached_parser()` is used (not direct ManifestParser instantiation)
- Check for repeated manifest parsing
- Run performance benchmarks: `pytest -m performance`
