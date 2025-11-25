"""
ManifestParser - Fast manifest.json parsing with orjson

Uses orjson (6-20x faster than stdlib json) and @cached_property
for lazy loading and optimal performance.
"""

from functools import cached_property
from pathlib import Path
from typing import Any, Optional

import orjson

from dbt_meta.errors import ManifestNotFoundError, ManifestParseError


class ManifestParser:
    """Parse dbt manifest.json with lazy loading and fast orjson"""

    def __init__(self, manifest_path: str):
        """
        Initialize parser with manifest path

        Args:
            manifest_path: Absolute path to manifest.json

        Note: Manifest is not loaded until accessed (lazy loading)
        """
        self.manifest_path = manifest_path

    @cached_property
    def manifest(self) -> dict[str, Any]:
        """
        Load and parse manifest.json using orjson

        Uses @cached_property for lazy loading:
        - First access: loads and parses manifest
        - Subsequent access: returns cached value

        Returns:
            Parsed manifest dictionary

        Raises:
            ManifestNotFoundError: If manifest doesn't exist
            ManifestParseError: If manifest contains invalid JSON
        """
        manifest_file = Path(self.manifest_path)

        if not manifest_file.exists():
            raise ManifestNotFoundError(searched_paths=[self.manifest_path])

        try:
            with open(manifest_file, 'rb') as f:
                return orjson.loads(f.read())
        except orjson.JSONDecodeError as e:
            raise ManifestParseError(
                path=self.manifest_path,
                parse_error=str(e)
            ) from e

    def get_model(self, model_name: str) -> Optional[dict[str, Any]]:
        """
        Get model by name (searches unique_id)

        Args:
            model_name: Model name (e.g., "core_client__client_profiles_events")

        Returns:
            Model dictionary if found, None otherwise
        """
        nodes = self.manifest.get('nodes', {})

        # Search for model by exact match in unique_id
        # unique_id format: model.project_name.model_name
        for unique_id, node in nodes.items():
            if not unique_id.startswith('model.'):
                continue

            # Extract model name from unique_id (last part after last dot)
            uid_model_name = unique_id.split('.')[-1]

            # Exact match required
            if uid_model_name == model_name:
                return node

        return None

    def get_all_models(self) -> dict[str, dict[str, Any]]:
        """
        Get all models from manifest

        Returns:
            Dictionary of {unique_id: model_data} for all models
        """
        nodes = self.manifest.get('nodes', {})

        # Filter to include only models
        return {
            unique_id: node
            for unique_id, node in nodes.items()
            if unique_id.startswith('model.')
        }

    def search_models(self, pattern: str) -> list[dict[str, Any]]:
        """
        Search models by name pattern (case-insensitive)

        Args:
            pattern: Search pattern (substring match)

        Returns:
            List of matching models
        """
        models = self.get_all_models()
        pattern_lower = pattern.lower()

        return [
            model
            for unique_id, model in models.items()
            if pattern_lower in unique_id.lower()
        ]

    def get_dependencies(self, model_name: str) -> dict[str, list[str]]:
        """
        Get model dependencies (refs and sources)

        Args:
            model_name: Model name

        Returns:
            Dictionary with 'refs' and 'sources' lists
        """
        model = self.get_model(model_name)

        if not model:
            return {'refs': [], 'sources': []}

        # Extract refs from depends_on
        depends_on = model.get('depends_on', {})
        refs = [
            node for node in depends_on.get('nodes', [])
            if node.startswith('model.')
        ]

        sources = [
            node for node in depends_on.get('nodes', [])
            if node.startswith('source.')
        ]

        return {
            'refs': refs,
            'sources': sources
        }
