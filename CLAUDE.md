# CLAUDE.md

## Project Overview

**dbt-meta** — AI-first CLI для извлечения метаданных из dbt `manifest.json`. Performance-first (LRU + orjson + lazy), production-first (prod manifest по умолчанию), fallback-enabled (prod → dev → BigQuery).

## Quick Reference

- Полный список команд и флагов: `README.md` (Commands Reference) или `meta --help` / `meta <cmd> -h`.
- История изменений и breaking changes: `CHANGELOG.md`.
- Decision-tree источника данных (5 сценариев): `.qa/decision_tree_visual.txt`, `.qa/data_source_logic.md`.

## Hard Invariants

Эти правила load-bearing — нарушение ломает корректность или производительность.

### 1. Parser caching

**Всегда** через `_get_cached_parser(manifest_path)` (`utils/__init__.py`). Прямой `ManifestParser(path)` обходит LRU и роняет производительность с 5-10ms до 30-60ms на каждую команду.

### 2. Manifest discovery — приоритет

```
1. --manifest PATH      # explicit, highest
2. DBT_DEV_MANIFEST_PATH    # if --dev
3. DBT_PROD_MANIFEST_PATH   # default
```

`--manifest` и `--dev` взаимоисключающие — при совместном использовании `--dev` игнорируется с warning'ом.

**Prod vs dev manifest:** prod использует `config.alias` для имени таблицы, dev — имя SQL-файла. Это критично для `meta schema` и BigQuery fallback.

### 3. Fallback ordering (`fallback.py`)

```
PROD_MANIFEST → DEV_MANIFEST → BIGQUERY
```

Управляется `DBT_FALLBACK_TARGET`, `DBT_FALLBACK_BIGQUERY`. BigQuery fallback поддерживается для: `schema`, `columns`, `info`, `config`. **Не** поддерживается для: `deps`, `sql`, `parents`, `children` (dbt-specific).

### 4. Schema resolution rule (BigQuery fallback)

Когда модель найдена в одном manifest, но нужен BigQuery fallback (пустые колонки и т.п.) — **всегда** использовать schema из **найденной** модели:

- Найдена в dev → dev schema (`personal_user`)
- Найдена в prod → prod schema
- **Никогда** не делать повторный поиск в prod после нахождения в dev.

Реализация: `utils/state_detector.py` (`ModelStateDetector`), `command_impl/column_source.py` (`ColumnSourceFactory`).

### 5. Catalog staleness — by file mtime, not generated_at

`meta columns` использует `catalog.json` как быстрая альтернатива BigQuery (~10ms vs ~3s).

- File mtime > 24h → fallback в BigQuery (CI/CD сломан).
- Internal `generated_at` > 7 дней → info-сообщение, но без fallback (catalog синхронизируется при merge to master; если schema не менялась — старый `generated_at` ОК).

Реализация: `catalog/parser.py`, `command_impl/columns.py`.

### 6. Compiled SQL — 3-level lookup

`utils/compiled_sql.py:get_compiled_sql()`:

```
1. manifest['compiled_code']
2. {project_root}/target/compiled/{package}/{original_file_path}
3. dbt compile --select <model> --target dev   # only with --dev, 180s timeout
```

Project root найден walk-up'ом от manifest по `dbt_project.yml`. Без `--dev` шаг 3 не запускается (предлагается `meta validate --dev <model>`).

**ВАЖНО:** `validate --dev` и `scan --dev` используют **dev** manifest path для определения project root в `get_compiled_sql`. Это критично: без этого disk lookup (`target/compiled/`) и auto-compile работают в prod project root, а не dev.

### 7. Dev schema resolution

```
1. DBT_DEV_SCHEMA           # explicit
2. personal_{username}      # default, username sanitized via re.sub(r'[^a-zA-Z0-9_]', '_', ...)
```

Username из `DBT_USER` или `$USER`. Sanitize обязателен — BigQuery dataset name допускает только `[a-zA-Z0-9_]`.

### 8. File management

- **Никаких** временных файлов в `/tmp/`. Тесты, debug-скрипты, артефакты — в корень проекта (видны в `git status`, легко ревьюить и удалять).

### 9. `context` — стабильная полная схема, `list` — разреженная

`meta context` всегда отдаёт **полный** набор ключей; пустое значение — это сигнал (`partition_by: null` = «не партиционирована», `description: ""` = «не задокументировано», но колонка существует). Не прятать пустые ключи — агент не должен ветвиться по их наличию. `meta list` наоборот: `meta` выводится только когда непустой (отсутствие = «нет model-level meta» для сотен моделей, `{}` был бы шумом).

## Architecture

```
src/dbt_meta/
├── cli.py                 # Typer CLI + Rich (help panels, wiring)
├── errors.py              # DbtMetaError hierarchy
├── config.py              # TOML + env, XDG, Power BI section
├── fallback.py            # 3-level fallback strategy
├── command_impl/
│   ├── base.py            # Fallback orchestration (get_model_with_fallback)
│   ├── column_source.py   # ColumnSource strategies: BigQuery/Catalog + ColumnSourceFactory
│   ├── schema.py, columns.py, config.py, sql.py, path.py
│   ├── context.py         # queryable-shape bundle (facade over columns/docs/config + catalog stats), 1+ models
│   ├── docs.py            # column descriptions — no CLI command, used internally by context.py
│   ├── ls.py, refresh.py, search.py
│   ├── parents.py, children.py, lineage_utils.py
│   ├── validate.py, scan.py
│   ├── analyze.py, hotspots.py, branch.py
│   ├── lineage.py         # column-level lineage queries
│   └── powerbi.py
├── lineage/               # column-level lineage core
│   ├── builder.py         # SQLGlot all-columns mode (v30.7+)
│   ├── graph.py           # rustworkx wrapper, model.column ids
│   ├── artifact.py        # JSON save/load + staleness
│   └── finder.py
├── usage/                 # column-usage analysis (WHERE/JOIN/GROUP BY)
│   ├── extractor.py       # ColumnUsageExtractor (SQLGlot AST)
│   ├── _common.py         # downstream traversal, alias resolution, select_star_from
│   ├── advisor_cluster.py
│   ├── advisor_partition.py
│   └── advisor_refresh.py
├── manifest/{parser,finder}.py
├── catalog/parser.py
├── utils/                 # bigquery, compiled_sql, monitoring, powerbi, git, dev, model_state
│   └── state_detector.py  # ModelStateDetector: manifest + git → DetectedState
└── templates/dbt-meta.toml
```

## Optimization Advisors — Algorithms

Чистый AST-walk + tunable heuristics. Без `INFORMATION_SCHEMA.JOBS_BY_PROJECT` и без LLM.

### Common pipeline

```
load manifest.json + catalog.json
→ resolve target → physical table aliases
→ collect downstream unique_ids (manifest.child_map BFS)
→ for each downstream: ColumnUsageExtractor(compiled SQL)
    → SQLGlot AST: WHERE / JOIN / GROUP BY / ORDER BY / QUALIFY /
       Window PARTITION BY → UsageEvent list
→ advisor-specific scoring
```

**`ColumnUsageExtractor` qualify pass:** перед обходом запускает `sqlglot.optimizer.qualify_tables` + `qualify_columns(infer_schema=True)`. Без этого bare-name references (`WHERE event_type = 1` без `events.` префикса) дают `col.table = ''` и фильтр алиасов их отбрасывает. dbt-compiled SQL часто использует bare refs — без qualify advisor не работает на проде.

### `optimize cluster` heuristic

Per-column score = Σ event_weight × `log2(downstream_count + 1)`:

| Event | Weight |
|---|---|
| WHERE eq | 3.0 |
| WHERE in | 2.5 |
| WHERE between | 2.0 |
| WHERE gt/ge/lt/le | 2.0 |
| WHERE (function-wrapped) | 0.5 |
| JOIN | 2.0 |
| GROUP BY | 1.0 |

Исключения: own partition column; типы `STRUCT` / `ARRAY` / `GEOGRAPHY` / `JSON`. Cap = 4 (BigQuery limit).

### `optimize partition` heuristic

Per-column score = Σ event_weight × type_bonus × `log2(downstream_count + 1)`:

| Operator | Weight |
|---|---|
| between | 3.0 |
| gt/ge/lt/le | 2.5 |
| eq | 2.0 |
| in | 1.5 |
| function-wrapped | 0.0 |

Type bonus: TIMESTAMP/DATE × 1.5, DATETIME × 1.3, INT64 × 1.0. Granularity: TIME → `DAY`, INT64 → `RANGE_BUCKET`. Output: 1 primary + до 4 alternatives.

### `optimize refresh` — chain-aware propagation

Walks transitive downstream в **топологическом порядке** (Kahn на `depends_on.nodes`) и пропагирует affectedness через цепочку. 3-уровневый descendant, который напрямую не упоминает changed model, всё равно классифицируется правильно — affectedness течёт через промежуточные модели.

**State:** `affected_cols: dict[unique_id, set[str] | None]`. `None` = "whole row" (SELECT * propagation или unknown change).

**Init:** для каждой changed model `affected_cols[changed] = <cols из --cols>` или `None`.

**Propagation step** (downstream `M`, affected parent `U`):

1. `SELECT * FROM <U>` (AST: `exp.Select` со `exp.Star` в projections + matching `from_`) → `M.affected = None`.
2. `U.affected = None` → любая ссылка на `U` (через aliases или `references_target`) делает `M.affected = None`.
3. `U.affected = {c1, c2}` → собрать `ColumnUsageExtractor` events на SQL `M`, где `event.column ∈ U.affected`. Имена upstream-колонок аккумулируются в `M.affected` (proxy для output columns — точно для pass-through и same-name renames, over-counts на aliased renames, что безопаснее).

**Per-model classification:**

| Условие | Bucket |
|---|---|
| `M.affected == None` | full_refresh |
| `M.affected ∩ M.unique_key ≠ ∅` | full_refresh (merge сломает историю) |
| `M.affected ∩ M.partition_by ≠ ∅` | full_refresh (partition pruning промахнётся) |
| `M.materialized != "incremental"` | full_refresh |
| иначе | incremental |

**`SELECT *` detection** — AST-level через `select_star_from()` в `usage/_common.py`: проверяет `exp.Select` с `exp.Star` в projections AND matching `from_`. Заменил regex, который ловил false positives в комментариях/CTE.

**Compiled-SQL fallback** (когда `node['compiled_code']` пустой, например local manifest от `dbt parse`):

1. Manifest `compiled_code`.
2. Disk: `<project_root>/target/compiled/<package>/<original_file_path>.sql`.
3. **Bulk auto-compile** — один раз за `plan()`: `dbt compile --select <downstream models>`. Триггер: найден project root, `dbt` в PATH, >50% sampled 20-model slice без SQL на (1)+(2). Отключается через `--no-compile`.

### Advisor caveats

- Cardinality-aware scoring не подключён (план: `dbt_bigquery_monitoring`).
- UDF внутренности opaque — вызов функции скрывает column-level usage внутри тела.
- Без `--cols` propagation намеренно пессимистичный: любая ссылка на whole-model-changed upstream → downstream affected целиком.
- Для моделей без `compiled_code` (после всех 3 fallback'ов) advisor классифицирует как `full_refresh` (cannot prove safety).
- SQLGlot optimizer может бросить на необычном SQL — extractor глотает `SqlglotError` / `RecursionError` / `AttributeError` и трактует модель как zero-event (advisor consequently conservative).

## Hotspots Scoring (calibrated in cents, €0.01 = 1pt)

| Criterion | Points | Threshold |
|---|---|---|
| query_cost | cost_7d × 100 | direct spend, €0.01/week = 1pt (PRIMARY) |
| high_scan | base × log2(freq), max 100 | >10GB/q=20, >1GB=10, >100MB=3 |
| high_slot | base × log2(freq), max 75 | >10min/q=15, >2min=8, >30sec=3 |
| no_partition | base × log2(freq), max 75 | >100GB=15, >10GB=8, >1GB=3 |
| no_clustering | base × log2(freq), max 50 | >100GB=10, >10GB=5, >1GB=2 |
| low_cache | wasted_cost × 100, max 200 | cache_hit < 30% |
| unused | storage_cost × 100, max 200 | unused >30 days |

Returns: Top by Score + Top Storage Savings.

## Column-Level Lineage — Caveats

- SQLGlot требует **compiled SQL** (не jinja). Модели без `compiled_code` skip с warning'ом.
- `SELECT * EXCEPT(...)` требует `catalog.json` для expansion. Без catalog колонки достижимые через `*` теряются.
- UDF internals opaque (leaf = "derived from input cols", тело UDF не парсится).
- Build CPU-heavy: ~5-10 min для 800+ моделей без `sqlglot[c]`. Рекомендация: CI build prod-артефакта, дистрибуция через `~/dbt-state/` sync.

## Adding a New Command

1. **`commands.py`** — функция через `_get_cached_parser(manifest_path)`, возвращает `Optional[Dict]`.
2. **`cli.py`** — Typer command с `-j/--json`, делегирует в `commands`/`command_impl`.
3. **`tests/test_commands.py`** — тест с fixtures `prod_manifest`, `test_model`.
4. **Help panel** — обновить `_build_commands_panel()` в `cli.py` при необходимости.

Type hints обязательны (strict mypy).

## Testing

- Coverage requirement: 90%+ (см. `pyproject.toml`).
- Markers: `unit`, `integration`, `performance`, `critical`.
- Запуск: `pytest --cov=dbt_meta`.
- Excluded from coverage: `cli.py` (UI), `manifest/finder.py` (auto-discovery).

## Environment Variables

**Manifest / catalog / lineage paths:**
- `DBT_PROD_MANIFEST_PATH` (default `~/dbt-state/manifest.json`)
- `DBT_DEV_MANIFEST_PATH` (default `./target/manifest.json`)
- `DBT_PROD_CATALOG_PATH`, `DBT_DEV_CATALOG_PATH`
- `DBT_PROD_LINEAGE_PATH`, `DBT_DEV_LINEAGE_PATH`

**Fallback toggles:**
- `DBT_FALLBACK_TARGET` (dev manifest, default `true`)
- `DBT_FALLBACK_BIGQUERY` (BQ metadata, default `true`)
- `DBT_FALLBACK_CATALOG` (catalog.json for columns, default `true`)

**Naming:**
- `DBT_PROD_TABLE_NAME` — `alias_or_name` (default) | `name` | `alias`
- `DBT_PROD_SCHEMA_SOURCE` — `config_or_model` (default) | `model` | `config`
- `DBT_DEV_SCHEMA` — explicit (overrides `personal_{username}`)
- `DBT_USER` — username override

**Power BI:** `POWERBI_ENABLED`, `POWERBI_TENANT_ID`, `POWERBI_CLIENT_ID`, `POWERBI_CLIENT_SECRET`, `POWERBI_WORKSPACES`.

**Deprecated** (warning при использовании): `DBT_DEV_DATASET`, `DBT_DEV_SCHEMA_TEMPLATE`, `DBT_DEV_SCHEMA_PREFIX` → use `DBT_DEV_SCHEMA`.

## Publishing

```bash
pytest && mypy src/dbt_meta && ruff check src/dbt_meta
# bump version: pyproject.toml + src/dbt_meta/__init__.py
# update CHANGELOG.md
pip install -e . && meta --version
git tag v0.x.0
```

## Performance Targets

- First command: 30-60ms (manifest parse + cache populate).
- Subsequent: 5-10ms (cached parser).
- 865+ models parsed in ~35ms median.

Правила: всегда `_get_cached_parser()`, кэшировать в локальных переменных, generator expressions > list comprehensions для больших коллекций, `@lru_cache` для дорогих helper'ов.
