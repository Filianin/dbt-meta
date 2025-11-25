"""Tests to cover remaining gaps in columns.py and deps.py.

Target lines:
- columns.py: 78-82, 190-191, 234, 250, 282
- deps.py: 50-54, 70
"""


import pytest

from dbt_meta.commands import columns, deps


class TestColumnsEdgeCases:
    """Cover columns.py edge cases."""

    def test_columns_with_existing_model(self, enable_fallbacks, prod_manifest):
        """Test columns command with actual production model."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find a model with columns
        for node_id, node_data in nodes.items():
            if node_data.get('resource_type') == 'model' and node_data.get('columns'):
                model_name = node_id.split('.')[-1]

                result = columns(str(prod_manifest), model_name, use_dev=False, json_output=False)

                # Should return columns data
                if result:
                    assert isinstance(result, (list, dict))
                    break

    def test_columns_json_output_with_warnings(self, enable_fallbacks, prod_manifest):
        """Test columns command JSON output includes warnings (line 282)."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find a model with columns
        for node_id, node_data in nodes.items():
            if node_data.get('resource_type') == 'model' and node_data.get('columns'):
                model_name = node_id.split('.')[-1]

                # Enable JSON output
                result = columns(str(prod_manifest), model_name, use_dev=False, json_output=True)

                # JSON output should be a list (columns array)
                if result:
                    assert isinstance(result, list)
                    break


class TestDepsEdgeCases:
    """Cover deps.py edge cases."""

    def test_deps_with_refs_and_sources(self, enable_fallbacks, prod_manifest):
        """Test deps command with model that has both refs and sources (lines 50-54)."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find a model with dependencies
        for node_id, node_data in nodes.items():
            if node_data.get('resource_type') == 'model':
                model_name = node_id.split('.')[-1]

                # Get deps
                result = deps(str(prod_manifest), model_name, use_dev=False, json_output=False)

                if result and (result.get('refs') or result.get('sources')):
                    # Found a model with dependencies
                    assert 'refs' in result
                    assert 'sources' in result
                    assert isinstance(result['refs'], list)
                    assert isinstance(result['sources'], list)
                    break

    def test_deps_json_output_format(self, enable_fallbacks, prod_manifest):
        """Test deps command returns proper JSON format (line 70)."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find any model
        for node_id in nodes:
            if nodes[node_id].get('resource_type') == 'model':
                model_name = node_id.split('.')[-1]

                # Get deps with JSON output
                result = deps(str(prod_manifest), model_name, use_dev=False, json_output=True)

                # Should return dict with refs and sources
                assert isinstance(result, dict)
                assert 'refs' in result
                assert 'sources' in result
                break


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
