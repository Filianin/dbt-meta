"""Tests for Task G commands: resolve, sources, name: selector, batch validate."""

from __future__ import annotations

from dbt_meta.commands import ls, resolve, sources


def test_resolve_finds_close_match(prod_manifest, test_model):
    # Drop a character from the middle to simulate a typo.
    if len(test_model) < 4:
        return
    typo = test_model[: len(test_model) // 2] + test_model[len(test_model) // 2 + 1 :]
    matches = resolve(str(prod_manifest), typo, limit=10, cutoff=0.5)
    assert any(m['name'] == test_model for m in matches), (
        f"Expected '{test_model}' in matches for typo '{typo}', got {[m['name'] for m in matches]}"
    )
    for m in matches:
        assert 0.0 <= m['score'] <= 1.0
        assert m['unique_id'].startswith('model.')


def test_resolve_returns_empty_on_nonsense(prod_manifest):
    matches = resolve(str(prod_manifest), 'zzzzzzzzzzzzzzz_no_such_model', cutoff=0.95)
    assert matches == []


def test_resolve_respects_limit(prod_manifest, test_model):
    matches = resolve(str(prod_manifest), test_model[:3], limit=2, cutoff=0.1)
    assert len(matches) <= 2


def test_sources_returns_list(prod_manifest):
    result = sources(str(prod_manifest))
    assert isinstance(result, list)
    for s in result:
        assert {'name', 'unique_id', 'schema', 'identifier'} <= set(s.keys())
        assert s['unique_id'].startswith('source.')


def test_sources_freshness_only_filters(prod_manifest):
    all_sources = sources(str(prod_manifest))
    fresh = sources(str(prod_manifest), freshness_only=True)
    assert len(fresh) <= len(all_sources)
    for s in fresh:
        assert s['freshness'] is not None


def test_sources_name_filter(prod_manifest):
    all_sources = sources(str(prod_manifest))
    if not all_sources:
        return
    # Pick a substring from the first source's unique_id.
    needle = all_sources[0]['unique_id'].split('.')[-1][:3]
    filtered = sources(str(prod_manifest), name_filter=needle)
    assert filtered  # at least the seed source matches
    for s in filtered:
        assert needle.lower() in s['unique_id'].lower()


def test_list_name_selector(prod_manifest, test_model):
    # Use a 3-char fragment of test_model as a substring selector.
    needle = test_model[:3]
    result = ls(str(prod_manifest), selectors=[f'name:{needle}'])
    assert isinstance(result, str)
    # Result is a space-separated string of model names.
    names = result.split()
    assert all(needle.lower() in n.lower() for n in names), (
        f"All names should contain '{needle}': {names[:5]}"
    )
    assert test_model in names


def test_list_name_selector_empty(prod_manifest):
    result = ls(str(prod_manifest), selectors=['name:zzzzz_no_match_zzzzz'])
    # Empty result is rendered as empty string.
    assert result == '' or result == [] or (isinstance(result, dict) and not result.get('models'))
