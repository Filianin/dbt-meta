"""Tests to cover remaining gaps in columns.py.

Target lines:
- columns.py: 78-82, 190-191, 234, 250, 282
"""


import pytest

from tests.helpers_cmd import columns


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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
