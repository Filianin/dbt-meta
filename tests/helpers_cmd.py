"""Compatibility wrappers for tests — thin adapters over command_impl classes.

Preserves the old commands.py functional API so test call sites don't change.
"""
from dbt_meta.command_impl.analyze import AnalyzeCommand
from dbt_meta.command_impl.branch import BranchCommand
from dbt_meta.command_impl.children import ChildrenCommand
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.command_impl.config import ConfigCommand
from dbt_meta.command_impl.docs import DocsCommand
from dbt_meta.command_impl.hotspots import HotspotsCommand
from dbt_meta.command_impl.ls import ListModelsCommand, LsCommand
from dbt_meta.command_impl.parents import ParentsCommand
from dbt_meta.command_impl.path import PathCommand
from dbt_meta.command_impl.refresh import RefreshCommand
from dbt_meta.command_impl.scan import ScanCommand
from dbt_meta.command_impl.schema import SchemaCommand
from dbt_meta.command_impl.search import SearchCommand
from dbt_meta.command_impl.sql import SqlCommand
from dbt_meta.command_impl.validate import ValidateCommand
from dbt_meta.config import Config
from dbt_meta.utils import print_warnings as _print_warnings
from dbt_meta.utils.git import check_manifest_git_mismatch as _check_manifest_git_mismatch


def _cfg() -> Config:
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning)
        return Config.from_env()


def schema(manifest_path, model_name, use_dev=False, json_output=False):
    return SchemaCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def columns(manifest_path, model_name, use_dev=False, json_output=False):
    return ColumnsCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def config(manifest_path, model_name, use_dev=False, json_output=False):
    return ConfigCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def sql(manifest_path, model_name, use_dev=False, json_output=False, raw=False):
    return SqlCommand(_cfg(), manifest_path, model_name, use_dev, json_output, raw=raw).execute()


def path(manifest_path, model_name, use_dev=False, json_output=False):
    return PathCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def parents(manifest_path, model_name, use_dev=False, json_output=False, recursive=False):
    return ParentsCommand(_cfg(), manifest_path, model_name, use_dev, json_output, recursive=recursive).execute()


def children(manifest_path, model_name, use_dev=False, json_output=False, recursive=False):
    return ChildrenCommand(_cfg(), manifest_path, model_name, use_dev, json_output, recursive=recursive).execute()


def docs(manifest_path, model_name, use_dev=False, json_output=False):
    return DocsCommand(manifest_path, model_name, use_dev, json_output).execute()


def search(manifest_path, query):
    return SearchCommand(manifest_path, query).execute()


def refresh(use_dev=False):
    return RefreshCommand(use_dev).execute()


def ls(manifest_path, selectors=None, modified=False, and_logic=False, group=False, use_dev=False, json_output=False):
    return LsCommand(manifest_path, selectors, modified, and_logic, group, use_dev, json_output).execute()


def list_models(manifest_path, pattern=None):
    return ListModelsCommand(manifest_path, pattern).execute()


def scan(manifest_path, model_name, use_dev=False, json_output=False):
    return ScanCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def validate(manifest_path, model_name, use_dev=False, json_output=False):
    return ValidateCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def analyze(manifest_path, model_name, use_dev=False, json_output=False):
    return AnalyzeCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()


def hotspots(manifest_path, limit=20, min_gb=1.0, json_output=False):
    return HotspotsCommand(_cfg(), manifest_path, limit, min_gb, json_output).execute()


def branch(manifest_path, model_name, use_dev=False, json_output=False):
    return BranchCommand(_cfg(), manifest_path, model_name, use_dev, json_output).execute()
