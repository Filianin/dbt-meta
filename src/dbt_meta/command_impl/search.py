"""Search command for dbt-meta."""

from __future__ import annotations

from dbt_meta.utils import get_cached_parser as _get_cached_parser


class SearchCommand:
    """Search models by name or description.

    Args:
        manifest_path: Path to manifest.json
        query: Search query (case-insensitive substring match)

    Returns:
        Sorted list of dicts with:
          - name: model name
          - description: model description
    """

    def __init__(self, manifest_path: str, query: str):
        self.manifest_path = manifest_path
        self.query = query

    def execute(self) -> list[dict[str, str]]:
        parser = _get_cached_parser(self.manifest_path)
        results = parser.search_models(self.query)

        output = [
            {
                'name': model['unique_id'].split('.')[-1],
                'description': model.get('description', ''),
            }
            for model in results
        ]
        return sorted(output, key=lambda x: x['name'])
