"""
CLI - Modern command-line interface using Typer

Provides dbt-meta CLI with:
- Type-hint based argument parsing
- Rich formatted output
- JSON output mode
- Auto-discovery of manifest.json
"""

import json
from pathlib import Path
from typing import Any, Callable, NoReturn, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from dbt_meta.command_impl.analyze import AnalyzeCommand
from dbt_meta.command_impl.branch import BranchCommand
from dbt_meta.command_impl.children import ChildrenCommand
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.command_impl.config import ConfigCommand
from dbt_meta.command_impl.context import ContextCommand
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
from dbt_meta.errors import DbtMetaError
from dbt_meta.manifest.finder import ManifestFinder

# Create Typer app
app = typer.Typer(
    name="dbt-meta",
    help="AI-first CLI for dbt metadata extraction",
    add_completion=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)

# Create settings management subcommand group
settings_app = typer.Typer(
    help="CLI settings management",
    context_settings={"help_option_names": ["-h", "--help"]},
)
app.add_typer(settings_app, name="settings")

# Column-level lineage subcommand group
lineage_app = typer.Typer(
    help="Column-level lineage queries (build/column/downstream/stats)",
    context_settings={"help_option_names": ["-h", "--help"]},
)
app.add_typer(lineage_app, name="lineage")

# Column-usage-aware optimization advisors
optimize_app = typer.Typer(
    help="Optimization advisors based on downstream column usage (refresh/cluster/partition)",
    context_settings={"help_option_names": ["-h", "--help"]},
)
app.add_typer(optimize_app, name="optimize")

# Rich console for formatted output
console = Console()

# Rich styles - reusable constants
STYLE_COMMAND = "cyan"
STYLE_DESCRIPTION = "white"
STYLE_HEADER = "bold green"
STYLE_ERROR = "red"
STYLE_DIM = "dim"
STYLE_GREEN = "green"


def handle_error(error: DbtMetaError, json_output: bool = False) -> NoReturn:
    """Display formatted error message with suggestion and exit.

    When json_output=True, emits {"error": "..."} to stdout for machine consumption.
    Otherwise, emits Rich-formatted text to stderr.
    """
    if json_output:
        print(json.dumps({"error": error.message}))
        raise typer.Exit(code=1)

    error_console = Console(stderr=True)
    error_console.print(f"\n[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {error.message}")
    if error.suggestion:
        error_console.print(f"[yellow]Suggestion:[/yellow] {error.suggestion}")
    error_console.print()
    raise typer.Exit(code=1)


def _not_found_error(model_name: str, json_output: bool) -> NoReturn:
    """Emit 'model not found' error and exit, respecting json_output mode."""
    if json_output:
        print(json.dumps({"error": f"Model '{model_name}' not found"}))
    else:
        Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
    raise typer.Exit(code=1)


def _build_tree_recursive(parent_tree: Tree, nodes: list[dict[str, Any]]) -> None:
    """
    Recursively build Rich Tree from hierarchical node structure

    Args:
        parent_tree: Rich Tree node to add children to
        nodes: List of node dicts with 'children' key
    """
    for node in nodes:
        node_type = node.get('type', '')
        node_name = node.get('name', '')

        # Format node label with color based on type
        if node_type == 'source':
            label = f"[yellow]{node_name}[/yellow] [dim]({node_type})[/dim]"
        elif node_type == 'model':
            label = f"[cyan]{node_name}[/cyan] [dim]({node_type})[/dim]"
        else:
            label = f"[white]{node_name}[/white] [dim]({node_type})[/dim]"

        # Add node to tree
        child_tree = parent_tree.add(label)

        # Recursively add children
        children = node.get('children', [])
        if children:
            _build_tree_recursive(child_tree, children)


def _build_commands_panel() -> Panel:
    """Build Commands panel with categorized commands"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=28)
    table.add_column(style=STYLE_DESCRIPTION)

    # Core commands (green)
    table.add_row("[bold green]Core:[/bold green]", "")
    table.add_row("  [green]schema[/green]", "BigQuery table name (--dev for dev schema)")
    table.add_row("  [green]path[/green]", "Relative file path to .sql file")
    table.add_row("  [green]columns[/green]", "Column names and types (--dev for dev schema)")
    table.add_row("  [green]sql[/green]", "Compiled SQL (default) or raw SQL with --jinja")
    table.add_row("  [green]context[/green]", "Full queryable-shape bundle (1+ models) before a BigQuery query")
    table.add_row("  [green]parents[/green]", "Upstream dependencies (direct or -a/--all ancestors)")
    table.add_row("  [green]children[/green]", "Downstream dependencies (direct or -a/--all descendants)")
    table.add_row("  [green]config[/green]", "Full dbt config (29 fields: partition_by, cluster_by, etc.)")
    table.add_row("", "")

    # Utilities (cyan)
    table.add_row("[bold cyan]Utilities:[/bold cyan]", "")
    table.add_row("  [cyan]list[/cyan]", "Filter models by selectors (tag:, config., path:)")
    table.add_row("  [cyan]models[/cyan]", "Simple substring search in model names")
    table.add_row("  [cyan]search[/cyan]", "Search by name or description")
    table.add_row("  [cyan]refresh[/cyan]", "Sync prod artifacts (or parse local with --dev)")
    table.add_row("  [cyan]validate[/cyan]", "Validate SQL syntax (BigQuery dry run)")
    table.add_row("  [cyan]scan[/cyan]", "Estimate query scan size (MB/GB)")
    table.add_row("", "")

    # Optimization & Analysis (yellow)
    table.add_row("[bold yellow]Optimization:[/bold yellow]", "")
    table.add_row("  [yellow]hotspots[/yellow]", "Find optimization opportunities (-n LIMIT, --min-gb GB)")
    table.add_row("  [yellow]analyze[/yellow]", "Deep analysis of single model")
    table.add_row("  [yellow]branch[/yellow]", "Branch-level optimization impact")
    table.add_row("  [yellow]optimize cluster[/yellow]", "Recommend cluster keys from downstream WHERE/JOIN")
    table.add_row("  [yellow]optimize partition[/yellow]", "Recommend partition column from downstream filters")
    table.add_row("  [yellow]optimize refresh[/yellow]", "Column-aware --full-refresh planner")
    table.add_row("", "")

    # Column-level lineage (magenta)
    table.add_row("[bold magenta]Lineage:[/bold magenta]", "")
    table.add_row("  [magenta]lineage build[/magenta]", "Build column-level lineage artifact (SQLGlot + rustworkx)")
    table.add_row("  [magenta]lineage column <m.col>[/magenta]", "Upstream lineage for a column")
    table.add_row("  [magenta]lineage downstream <m.col>[/magenta]", "Downstream impact for a column")
    table.add_row("  [magenta]lineage stats[/magenta]", "Artifact summary (nodes, edges, generated_at)")
    table.add_row("", "")

    # Integration — Power BI (blue)
    table.add_row("[bold blue]Power BI:[/bold blue]", "")
    table.add_row("  [blue]powerbi artifacts[/blue]", "Scan workspaces + build compact index → ~/dbt-state/")
    table.add_row("  [blue]powerbi list[/blue]", "List all reports (workspace | report | dataset)")
    table.add_row("  [blue]powerbi find <q>[/blue]", "Find reports / metrics behind a dashboard")
    table.add_row("  [blue]powerbi show <report>[/blue]", "Report breakdown: tables + SQL analysis")
    table.add_row("  [blue]powerbi reports <model>[/blue]", "Reverse lookup: dbt model → PBI reports")
    table.add_row("  [blue]powerbi cost <report>[/blue]", "Per-table query cost metrics (7-day, live BQ)")
    table.add_row("  [blue]powerbi lineage <report>[/blue]", "Column-level upstream lineage for report SQL filters")
    table.add_row("  [blue]powerbi measures <report>[/blue]", "DAX measures + expressions (from raw)")
    table.add_row("  [blue]powerbi source <report>[/blue]", "Power Query M-expressions (from raw)")
    table.add_row("  [blue]powerbi owners <report>[/blue]", "Report owners + last modified (from raw)")
    table.add_row("", "")

    # Settings management (magenta)
    table.add_row("[bold magenta]Settings:[/bold magenta]", "")
    table.add_row("  [magenta]settings init[/magenta]", "Create config file from template (-f to overwrite)")
    table.add_row("  [magenta]settings show[/magenta]", "Display current configuration")
    table.add_row("  [magenta]settings validate[/magenta]", "Validate config file")
    table.add_row("  [magenta]settings path[/magenta]", "Show path to active config file")
    table.add_row("", "")

    # Help on demand (dim)
    table.add_row("[bold]Help:[/bold]", "")
    table.add_row("  examples", "Show usage examples for all commands")
    table.add_row("  config-help", "Show env vars and TOML configuration reference")

    return Panel(table, title="[bold white]🚀 Commands[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def _build_flags_panel() -> Panel:
    """Build Flags panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=20)
    table.add_column(style=STYLE_DESCRIPTION)

    table.add_row("[bold cyan]Global flags:[/bold cyan]", "")
    table.add_row("-h, --help", "Show this help message")
    table.add_row("-v, --version", "Show version and exit")
    table.add_row("--manifest PATH", "Explicit path to manifest.json")
    table.add_row("-d, --dev", "Use dev manifest and schema")
    table.add_row("", "")
    table.add_row("[bold cyan]Output flags:[/bold cyan]", "")
    table.add_row("[green]-j, --json[/green]", "Output as JSON (AI-friendly structured data)")
    table.add_row("", "")
    table.add_row("[bold cyan]Lineage flags:[/bold cyan]", "")
    table.add_row("-a, --all", "Recursive mode (parents, children)")
    table.add_row("", "")
    table.add_row("[bold cyan]SQL flags:[/bold cyan]", "")
    table.add_row("--jinja", "Show raw SQL with Jinja (sql command)")
    table.add_row("", "")
    table.add_row("[bold cyan]list flags:[/bold cyan]", "")
    table.add_row("--and", "Require ALL selectors (default: OR)")
    table.add_row("--group", "Group by tag combinations")
    table.add_row("-m, --modified", "Show only git-modified/new models")
    table.add_row("", "")
    table.add_row("[bold cyan]hotspots flags:[/bold cyan]", "")
    table.add_row("-n, --limit N", "Number of hotspots to show (default: 10)")
    table.add_row("--min-gb GB", "Minimum table size in GB (default: 1.0)")
    table.add_row("", "")
    table.add_row("[bold cyan]settings init flags:[/bold cyan]", "")
    table.add_row("-f, --force", "Overwrite existing config file")

    return Panel(table, title="[bold white]🚩 Flags[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def _build_examples_panel() -> Panel:
    """Build Examples panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=45)
    table.add_column(style=STYLE_DESCRIPTION)

    table.add_row("[bold]Basic metadata:[/bold]", "")
    table.add_row("  meta schema customers", "my_project.analytics.customers")
    table.add_row("  meta path customers", "models/analytics/customers.sql")
    table.add_row("  meta columns -j orders", "Get columns as JSON")
    table.add_row("  meta config -j customers", "Full dbt config")
    table.add_row("  meta context -j customers", "Full queryable-shape bundle")
    table.add_row("  meta sql customers", "View compiled SQL")
    table.add_row("  meta sql --jinja customers", "Raw SQL with Jinja")
    table.add_row('  meta search "customer"', "Search by name/description")
    table.add_row("", "")
    table.add_row("[bold]Lineage:[/bold]", "")
    table.add_row("  meta parents customers", "Direct upstream dependencies")
    table.add_row("  meta parents -a customers", "All ancestors (tree view)")
    table.add_row("  meta children -a -j customers", "All descendants as nested JSON")
    table.add_row("", "")
    table.add_row("[bold]Dev workflow (with defer):[/bold]", "")
    table.add_row("  defer run --select customers", "Build dev table first")
    table.add_row("  meta schema --dev customers", "personal_USERNAME.customers")
    table.add_row("  meta columns --dev -j customers", "Get dev table columns")
    table.add_row("  meta refresh", "Sync prod artifacts from remote storage")
    table.add_row("  meta refresh --dev", "Parse local project (dbt parse)")
    table.add_row("", "")
    table.add_row("[bold]Filtering (list):[/bold]", "")
    table.add_row("  meta list tag:daily", "Models with daily tag")
    table.add_row("  meta list tag:a tag:b --and", "Models with BOTH tags")
    table.add_row("  meta list path:models/core/", "Models under path")
    table.add_row("  meta list config.materialized:incremental", "Incremental models")
    table.add_row("  meta list -m", "Git-modified models")
    table.add_row("  meta list tag:daily --group", "Group by tag combinations")
    table.add_row("", "")
    table.add_row("[bold]SQL validation:[/bold]", "")
    table.add_row("  meta validate customers", "Check SQL syntax (BigQuery dry run)")
    table.add_row("  meta scan customers", "Estimate scan size (MB/GB)")
    table.add_row("  meta scan --dev -j customers", "Dev SQL scan size as JSON")
    table.add_row("", "")
    table.add_row("[bold]Optimization:[/bold]", "")
    table.add_row("  meta hotspots", "Top 10 optimization candidates")
    table.add_row("  meta hotspots -n 20 --min-gb 10", "Top 20, tables >10 GB")
    table.add_row("  meta analyze customers", "Deep analysis of one model")
    table.add_row("  meta branch customers", "Upstream/downstream alignment")
    table.add_row("", "")
    table.add_row("[bold]Column-level lineage:[/bold]", "")
    table.add_row("  meta lineage build", "Build prod artifact from manifest+catalog")
    table.add_row("  meta lineage column model.col", "Upstream lineage (where does column come from)")
    table.add_row("  meta lineage downstream model.col", "Impact analysis (what breaks if column changes)")
    table.add_row("  meta lineage stats -j", "Artifact stats as JSON")
    table.add_row("", "")
    table.add_row("[bold]Power BI integration:[/bold]", "")
    table.add_row("  meta powerbi artifacts", "Scan + build agent index → ~/dbt-state/")
    table.add_row("  meta powerbi find leads", "Reports/metrics matching 'leads'")
    table.add_row("  meta powerbi show 'Organic Leads'", "Full report breakdown")
    table.add_row("", "")
    table.add_row("[bold]Combined flags:[/bold]", "")
    table.add_row("  meta schema -dj customers", "Dev + JSON output")
    table.add_row("  meta parents -adj customers", "All + JSON + Dev")
    table.add_row("", "")
    table.add_row("[bold]Configuration:[/bold]", "")
    table.add_row("  meta settings init", "Create config file")
    table.add_row("  meta settings init --force", "Overwrite existing config")
    table.add_row("  meta settings show -j", "View current settings as JSON")
    table.add_row("  meta settings validate", "Check config file")
    table.add_row("  meta --manifest ~/custom.json list", "Use custom manifest")

    return Panel(table, title="[bold white]💡 Examples[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def _build_configuration_panel() -> Panel:
    """Build Configuration panel with TOML-based setup"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style="white", no_wrap=False)

    # Quick Start
    table.add_row("[bold cyan]Quick Start (zero config):[/bold cyan]")
    table.add_row("Just run [cyan]dbt compile[/cyan] and start using meta commands")
    table.add_row("Works out of the box with sensible defaults")
    table.add_row("")

    # TOML Configuration
    table.add_row("[bold cyan]Configuration File (recommended):[/bold cyan]")
    table.add_row("1. Create config:    [cyan]meta settings init[/cyan]")
    table.add_row("2. Edit config:      [cyan]~/.config/dbt-meta/config.toml[/cyan]")
    table.add_row("3. Validate:         [cyan]meta settings validate[/cyan]")
    table.add_row("4. View settings:    [cyan]meta settings show[/cyan]")
    table.add_row("")

    # Config Locations
    table.add_row("[bold cyan]Config File Locations (priority order):[/bold cyan]")
    table.add_row("  1. [cyan]./.dbt-meta.toml[/cyan]              → Project-local config")
    table.add_row("  2. [cyan]~/.config/dbt-meta/config.toml[/cyan] → User config (XDG)")
    table.add_row("  3. [cyan]~/.dbt-meta.toml[/cyan]               → Fallback")
    table.add_row("")

    # What to Configure
    table.add_row("[bold cyan]Common Settings:[/bold cyan]")
    table.add_row("  • Manifest paths (prod/dev)")
    table.add_row("  • Catalog paths (prod/dev)")
    table.add_row("  • Fallback behavior")
    table.add_row("  • BigQuery settings")
    table.add_row("  • Output formatting")
    table.add_row("")

    # Environment Variables (alternative)
    table.add_row("[bold cyan]Environment Variables (alternative to TOML):[/bold cyan]")
    table.add_row("  [cyan]DBT_PROD_MANIFEST_PATH[/cyan]  → Production manifest path")
    table.add_row("  [cyan]DBT_DEV_MANIFEST_PATH[/cyan]   → Dev manifest path")
    table.add_row("  [cyan]DBT_PROD_CATALOG_PATH[/cyan]   → Production catalog.json path")
    table.add_row("  [cyan]DBT_DEV_CATALOG_PATH[/cyan]    → Dev catalog.json path")
    table.add_row("  [cyan]DBT_DEV_SCHEMA[/cyan]          → Dev schema override")
    table.add_row("  [cyan]DBT_FALLBACK_TARGET[/cyan]     → Enable dev manifest fallback")
    table.add_row("  [cyan]DBT_FALLBACK_BIGQUERY[/cyan]   → Enable BigQuery fallback")
    table.add_row("  [cyan]DBT_FALLBACK_CATALOG[/cyan]    → Enable catalog fallback (columns)")
    table.add_row("")

    # Power BI (optional)
    table.add_row("[bold cyan]Power BI (optional, for `meta powerbi`):[/bold cyan]")
    table.add_row("  [cyan]POWERBI_ENABLED[/cyan]         → Enable integration (default: false)")
    table.add_row("  [cyan]POWERBI_TENANT_ID[/cyan]       → Azure AD tenant ID")
    table.add_row("  [cyan]POWERBI_CLIENT_ID[/cyan]       → App registration client ID")
    table.add_row("  [cyan]POWERBI_CLIENT_SECRET[/cyan]   → App registration client secret")
    table.add_row("  [cyan]POWERBI_WORKSPACES[/cyan]      → Comma-separated workspace IDs")
    table.add_row("")

    # Priority System
    table.add_row("[bold cyan]Priority:[/bold cyan] CLI flags > TOML config > Env vars > Defaults")

    return Panel(table, title="[bold white]⚙️ Configuration[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def show_help_with_examples(ctx: typer.Context) -> None:
    """Show help with additional examples and usage info"""
    # Empty line before help
    print()

    # Description
    rprint("AI-first CLI for dbt metadata extraction")
    rprint()

    # Usage block (like glab style)
    console.print("[bold white]USAGE[/bold white]")
    console.print()
    usage_table = Table(show_header=False, box=None, padding=(0, 2), border_style="dim")
    usage_table.add_column(style="white")
    usage_table.add_row("meta COMMAND MODEL_NAME [FLAGS]")
    usage_table.add_row("meta COMMAND [FLAGS]                   [dim](for list, search, refresh)[/dim]")
    console.print(Panel(usage_table, border_style="white", padding=(0, 0)))
    rprint()

    # Print Commands and Flags panels only
    console.print(_build_commands_panel())
    console.print(_build_flags_panel())

    # Footer with links
    console.print()
    console.print("─" * 80)
    console.print("  Run [cyan]meta examples[/cyan] for usage examples")
    console.print("  Run [cyan]meta config-help[/cyan] for env vars and TOML configuration")
    console.print()
    console.print("  📚 Docs:   https://github.com/Filianin/dbt-meta")
    console.print("  🐛 Issues: https://github.com/Filianin/dbt-meta/issues")
    console.print()


def version_callback(value: bool) -> None:
    """Show version and exit"""
    if value:
        from dbt_meta import __version__
        rprint(f"[{STYLE_HEADER}]dbt-meta[/{STYLE_HEADER}] v{__version__}")
        rprint("Copyright (c) 2025 Pavel Filianin")
        rprint("Licensed under Apache License 2.0")
        rprint("https://github.com/Filianin/dbt-meta")
        raise typer.Exit()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
        help="Show version and exit",
        callback=version_callback,
        is_eager=True,
    ),
    help_flag: bool = typer.Option(
        None,
        "--help",
        "-h",
        help="Show this message and exit",
        is_eager=True,
    ),
) -> None:
    """
    AI-first CLI for dbt metadata extraction

    Run 'meta --help' for usage examples and available commands.
    """
    # Handle help flag manually for main command only
    if help_flag and ctx.invoked_subcommand is None:
        show_help_with_examples(ctx)
        raise typer.Exit()

    if ctx.invoked_subcommand is None and not version and not help_flag:
        # Show help with examples when no command specified
        show_help_with_examples(ctx)


# ============================================================================
# On-demand help commands
# ============================================================================

@app.command("examples")
def examples_cmd() -> None:
    """Show usage examples for all commands."""
    console.print(_build_examples_panel())


@app.command("config-help")
def config_help_cmd() -> None:
    """Show env vars and TOML configuration reference."""
    console.print(_build_configuration_panel())


# ============================================================================
# Settings Management Commands
# ============================================================================

@settings_app.command("init")
def settings_init(
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing config file"),
) -> None:
    """
    Initialize config file from template

    Creates ~/.config/dbt-meta/config.toml with documented defaults.
    Use --force to overwrite existing config.

    Examples:
        meta settings init              # Create config file
        meta settings init --force      # Overwrite existing
    """
    import shutil
    from pathlib import Path

    # Target location (XDG standard)
    target_dir = Path.home() / ".config" / "dbt-meta"
    target_file = target_dir / "config.toml"

    # Check if already exists
    if target_file.exists() and not force:
        console.print(f"[yellow]Config file already exists:[/yellow] {target_file}")
        console.print("Use --force to overwrite")
        raise typer.Exit(code=1)

    # Find template file (should be in package)
    try:
        import dbt_meta
        package_dir = Path(dbt_meta.__file__).parent
        template_file = package_dir / "templates" / "dbt-meta.toml"

        if not template_file.exists():
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Template file not found: {template_file}")
            console.print("Please reinstall dbt-meta package")
            raise typer.Exit(code=1)

        # Create target directory
        target_dir.mkdir(parents=True, exist_ok=True)

        # Copy template
        shutil.copy(template_file, target_file)

        console.print(f"[green]✅ Config file created:[/green] {target_file}")
        console.print()
        console.print("Next steps:")
        console.print("  1. Edit config file: ~/.config/dbt-meta/config.toml")
        console.print("  2. Validate config: meta settings validate")
        console.print("  3. View merged config: meta settings show")

    except Exception as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Failed to create config file: {e!s}")
        raise typer.Exit(code=1) from None


@settings_app.command("show")
def settings_show(
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """
    Display current merged configuration

    Shows configuration from TOML file with environment variable overrides.

    Examples:
        meta settings show              # Human-readable table
        meta settings show --json       # JSON output
    """
    try:
        config = Config.from_config_or_env()
        config_dict = config.to_dict()

        if json_output:
            print(json.dumps(config_dict, indent=2))
        else:
            print()
            table = Table(title="[bold green not italic]Current Configuration[/bold green not italic]", header_style="bold green")
            table.add_column("Section", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Key", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Value", style="white")

            # Group by section (based on field prefixes)
            sections = {
                "Manifest": ["prod_manifest_path", "dev_manifest_path"],
                "Catalog": ["prod_catalog_path", "dev_catalog_path"],
                "Fallback": ["fallback_dev_enabled", "fallback_bigquery_enabled", "fallback_catalog_enabled"],
                "Dev": ["dev_dataset", "dev_user"],
                "Production": ["prod_table_name_strategy", "prod_schema_source"],
                "BigQuery": ["bigquery_project_id", "bigquery_timeout", "bigquery_retries", "bigquery_location"],
                "Database": ["database_type", "database_host", "database_port", "database_name", "database_username", "database_password"],
                "Output": ["output_default_format", "output_json_pretty", "output_color", "output_show_source"],
                "Defer": ["defer_auto_sync", "defer_sync_threshold", "defer_sync_command", "defer_target"],
            }

            for section_name, fields in sections.items():
                first_row = True
                for field in fields:
                    if field in config_dict:
                        value = config_dict[field]
                        # Mask password
                        if field == "database_password" and value:
                            value = "***"

                        section_display = section_name if first_row else ""
                        table.add_row(section_display, field, str(value))
                        first_row = False

            console.print(table)

            # Show config file location
            config_file = Config.find_config_file()
            print()
            if config_file:
                console.print(f"[dim]Config file:[/dim] {config_file}")
            else:
                console.print("[dim]Config file: Not found (using defaults)[/dim]")

    except Exception as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Failed to load config: {e!s}")
        raise typer.Exit(code=1) from None


@settings_app.command("validate")
def settings_validate() -> None:
    """
    Validate configuration file

    Checks TOML syntax and validates configuration values.

    Examples:
        meta settings validate
    """
    try:
        # Try to load config
        config_file = Config.find_config_file()

        if not config_file:
            console.print("[yellow]No config file found[/yellow]")
            console.print("Run 'meta settings init' to create one")
            raise typer.Exit(code=0)

        console.print(f"[dim]Validating:[/dim] {config_file}")
        print()

        # Load and validate
        config = Config.from_toml(config_file)
        warnings_list = config.validate()

        if warnings_list:
            console.print("[yellow]Validation warnings:[/yellow]")
            for warning in warnings_list:
                console.print(f"  • {warning}")
            print()
            console.print("[yellow]⚠ Configuration has warnings[/yellow]")
        else:
            console.print("[green]✅ Configuration is valid[/green]")

    except FileNotFoundError as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {e!s}")
        raise typer.Exit(code=1) from None
    except ValueError as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {e!s}")
        raise typer.Exit(code=1) from None
    except Exception as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Unexpected error: {e!s}")
        raise typer.Exit(code=1) from None


@settings_app.command("path")
def settings_path() -> None:
    """
    Show path to active config file

    Displays the path to the config file being used (if any).

    Examples:
        meta settings path
    """
    config_file = Config.find_config_file()

    if config_file:
        print(str(config_file))
    else:
        error_console = Console(stderr=True)
        error_console.print("[yellow]No config file found[/yellow]")
        error_console.print("Using defaults")
        error_console.print()
        error_console.print("Search locations:")
        error_console.print("  1. ./.dbt-meta.toml")
        error_console.print("  2. ~/.config/dbt-meta/config.toml")
        error_console.print("  3. ~/.dbt-meta.toml")
        raise typer.Exit(code=1)


# ============================================================================
# Model Metadata Commands
# ============================================================================

def get_manifest_path(manifest_path: Optional[str] = None, use_dev: bool = False) -> tuple[str, bool]:
    """
    Get manifest path from explicit parameter or auto-discover

    Args:
        manifest_path: Optional explicit path from --manifest flag
        use_dev: If True, use dev manifest (ignored if manifest_path provided)

    Returns:
        Tuple of (manifest_path, effective_use_dev)
        - manifest_path: Absolute path to manifest.json
        - effective_use_dev: Actual use_dev value (False if manifest_path was provided)

    Raises:
        typer.Exit: If manifest not found
    """
    # Warning if both --manifest and --dev are used
    effective_use_dev = use_dev
    if manifest_path and use_dev:
        import sys as _sys
        _sys.stderr.write("⚠️  Warning: --dev flag ignored because --manifest was provided\n")
        # When explicit manifest is provided, ignore use_dev flag
        effective_use_dev = False

    try:
        path = ManifestFinder.find(explicit_path=manifest_path, use_dev=effective_use_dev)
        return path, effective_use_dev
    except FileNotFoundError as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {e!s}")
        raise typer.Exit(code=1) from None


def handle_command_output(
    result: Any,
    json_output: bool,
    formatter_func: Optional[Callable[[Any], None]] = None
) -> None:
    """
    Handle command output in JSON or human-readable format

    Args:
        result: Command result data
        json_output: If True, output as JSON
        formatter_func: Optional function to format human-readable output
                       Function signature: formatter_func(result) -> None

    Raises:
        typer.Exit: If result is None
    """
    if result is None:
        # Error already printed by command
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    elif formatter_func:
        formatter_func(result)
    else:
        # Default: just print the result
        print(result)


@app.command()
def schema(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Production table name (database.schema.table) or dev with --dev flag

    Examples:
        meta schema jaffle_shop__orders            # Production
        meta schema --dev jaffle_shop__orders      # Dev (personal_USERNAME)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = SchemaCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output).execute()

        if not result or not result.get('full_name'):
            _not_found_error(model_name, json_output)

        if json_output:
            output = {
                "model_name": model_name,
                "full_name": result['full_name']
            }
            print(json.dumps(output, indent=2))
        else:
            print(result['full_name'])

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def columns(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema"),
) -> None:
    """
    Column names and types

    Examples:
        meta columns -j customers                # Production
        meta columns --dev -j customers          # Dev
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = ColumnsCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output).execute()

        if not result:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()
            table = Table(title=f"[bold green not italic]Columns: {model_name}[/bold green not italic]", header_style="bold green")
            table.add_column("Name", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Type", style="white")

            for col in result:
                table.add_row(col['name'], col['data_type'])

            console.print(table)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def config(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Full dbt config (29 fields: partition_by, cluster_by, etc.)

    Examples:
        meta config -j model_name              # Production
        meta config --dev -j model_name        # Dev (personal_USERNAME)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = ConfigCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()
            table = Table(title=f"[bold green not italic]Config: {model_name}[/bold green not italic]", header_style="bold green")
            table.add_column("Key", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Value", style="white")

            for key, value in result.items():
                table.add_row(key, str(value))

            console.print(table)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def sql(
    model_name: str = typer.Argument(..., help="Model name"),
    jinja: bool = typer.Option(False, "--jinja", help="Show raw SQL with Jinja"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """
    Compiled SQL (default) or raw SQL with --jinja

    Examples:
        meta sql model_name                  # Production compiled SQL
        meta sql --dev model_name            # Dev (personal_USERNAME)
        meta sql --jinja model_name          # Raw SQL with Jinja
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        # Compiled SQL is the whole point of this command — make sure the
        # manifest has it. ``jinja`` mode is exempt (it reads raw SQL).
        if not jinja:
            _preflight_compiled_sql_by_path(manifest_path, manifest, no_compile, json_output)
        result = SqlCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output, raw=jinja).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        # Empty string is valid result (e.g., compiled_code missing from manifest)
        # SqlCommand will print informational messages to stderr if needed
        if json_output:
            output = {
                "model_name": model_name,
                "sql": result,
                "type": "raw" if jinja else "compiled"
            }
            print(json.dumps(output, indent=2))
        else:
            print(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def validate(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev manifest SQL"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """
    Validate SQL syntax using BigQuery dry run

    Examples:
        meta validate model_name          # Validate production SQL
        meta validate --dev model_name    # Validate dev SQL
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        _preflight_compiled_sql_by_path(manifest_path, manifest, no_compile, json_output)
        result = ValidateCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        elif result['valid']:
            console.print("[green]✅ Valid[/green]")
        else:
            console.print(f"[{STYLE_ERROR}]❌ Error:[/{STYLE_ERROR}] {result['error']}")
            raise typer.Exit(code=1)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def scan(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev manifest SQL"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """
    Estimate query scan size using BigQuery dry run

    Examples:
        meta scan model_name              # Show scan size for production SQL
        meta scan --dev model_name        # Show scan size for dev SQL
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        _preflight_compiled_sql_by_path(manifest_path, manifest, no_compile, json_output)
        result = ScanCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        elif result.get('error'):
            console.print(f"[{STYLE_ERROR}]❌ Error:[/{STYLE_ERROR}] {result['error']}")
            raise typer.Exit(code=1)
        else:
            # Color by size: <1GB green, <10GB yellow, >=10GB red
            bytes_val = result.get('bytes', 0) or 0
            gb = bytes_val / (1024 ** 3)
            if gb < 1:
                color = "green"
            elif gb < 10:
                color = "yellow"
            else:
                color = "red"
            console.print(f"Scan size: [bold {color}]{result['formatted']}[/bold {color}]")

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def path(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Relative file path to .sql file

    Examples:
        meta path model_name              # Production
        meta path --dev model_name        # Dev (personal_USERNAME)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = PathCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            output = {
                "model_name": model_name,
                "path": result
            }
            print(json.dumps(output, indent=2))
        else:
            print(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command("models")
def models_cmd(
    pattern: Optional[str] = typer.Argument(None, help="Filter pattern"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
) -> None:
    """
    List all models (optionally filter by pattern - simple substring search)

    Example: meta models jaffle_shop
    """
    try:
        manifest_path, _ = get_manifest_path(manifest)
        result = ListModelsCommand(manifest_path, pattern).execute()

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()
            title = f"Models ({len(result)})"
            if pattern:
                title = f"Models matching '{pattern}' ({len(result)})"

            table = Table(title=f"[bold green not italic]{title}[/bold green not italic]", header_style="bold green")
            table.add_column("Model", style=STYLE_COMMAND)

            for model in result:
                table.add_row(model)

            console.print(table)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
) -> None:
    """
    Search by name or description

    Example: meta search "customers" --json
    """
    try:
        manifest_path, _ = get_manifest_path(manifest)
        result = SearchCommand(manifest_path, query).execute()

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()
            table = Table(title=f"[bold green not italic]Search results for '{query}' ({len(result)})[/bold green not italic]", header_style="bold green")
            table.add_column("Model", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Description", style="white")

            for model in result:
                desc = model['description'] or ""
                table.add_row(model['name'], desc)

            console.print(table)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command("list")
def list_cmd(
    selectors: Optional[list[str]] = typer.Argument(None, help="Selectors: tag:name config.key:val path:dir package:name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    modified: bool = typer.Option(False, "-m", "--modified", help="Show only modified/new models (git-aware)"),
    and_logic: bool = typer.Option(False, "--and", help="Require ALL tags (default: OR - at least one)"),
    group: bool = typer.Option(False, "--group", help="Group by tag combinations"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema"),
) -> None:
    """Filter and list dbt models (replaces dbt ls)

    \b
    SELECTORS:
      tag:name               - Filter by tag (OR logic by default)
      config.key:value       - Filter by config value
      path:dir/              - Filter by file path
      package:name           - Filter by package

    \b
    FLAGS:
      --and                  - Use AND logic for tags (default: OR)
      --group                - Group results by tag combinations
      -m, --modified         - Show only git-modified/new models
      --dev / -d             - Use dev manifest (personal schema)
      --json / -j            - Output as JSON

    \b
    EXAMPLES:
      meta list tag:verified                      # Filter by tag
      meta list tag:verified tag:active           # At least ONE tag (OR)
      meta list tag:verified tag:active --and     # BOTH tags (AND)
      meta list tag:verified tag:active --group   # Grouped by tags
      meta list config.materialized:incremental   # Incremental models
      meta list path:models/staging/              # Staging models
      meta list -m                                # Git-modified models
      meta list tag:verified -j                   # JSON output

    \b
    OUTPUT FORMATS:
      Default    - Space-separated model names (for copy-paste)
      --group    - Grouped by tag combinations with headers
      --json     - Structured metadata [{"model": "...", "tags": [...]}]

    For full-refresh / incremental / skip planning of changed models, use
    `meta optimize refresh` — it does proper column-aware chain analysis
    instead of the naïve "everything downstream" listing this command used
    to do via the now-removed --full-refresh flag.
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        selector_list = list(selectors) if selectors else None

        result = LsCommand(
            manifest_path,
            selectors=selector_list,
            modified=modified,
            and_logic=and_logic,
            group=group,
            use_dev=effective_use_dev,
            json_output=json_output,
        ).execute()

        # Check for empty results (handles both list and dict formats)
        is_empty = (
            not result or
            (isinstance(result, list) and len(result) == 0) or
            (isinstance(result, dict) and len(result.get('models', [])) == 0)
        )

        if is_empty:
            if json_output:
                # Return empty dict format for consistency
                print(json.dumps({"models": [], "tables": []}))
            else:
                # Show header even for empty results (only in TTY)
                import sys
                if sys.stdout.isatty():
                    console.print()
                    console.print("[bold green]Models:[/]")
            return

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Add header and empty line ONLY if output is to TTY (not piped)
            import sys
            if sys.stdout.isatty():
                console.print()
                console.print("[bold green]Models:[/]")
            print(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def parents(
    model_name: str = typer.Argument(..., help="Model name"),
    all_ancestors: bool = typer.Option(False, "-a", "--all", help="Get all ancestors (recursive)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Upstream dependencies (direct or all ancestors)

    Examples:
        meta parents -j model_name                    # Direct parents (old format)
        meta parents -a model_name                    # Tree view
        meta parents -a -j model_name                 # Nested JSON (<=20) or flat array (>20)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = ParentsCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output, recursive=all_ancestors).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            print()
            if all_ancestors and result and isinstance(result[0], dict) and 'children' in result[0]:
                # Hierarchical tree output
                tree = Tree(f"[bold green]👴 All parents: {model_name}[/bold green]")
                _build_tree_recursive(tree, result)
                console.print(tree)
            else:
                # Tree-style output (no truncation)
                mode = "All parents" if all_ancestors else "Direct parents"
                console.print(f"[bold green]⬆️ {mode}: {model_name} ({len(result)})[/bold green]")
                for i, parent in enumerate(result):
                    is_last = (i == len(result) - 1)
                    branch = "└── " if is_last else "├── "
                    cont = "    " if is_last else "│   "
                    ptype = parent.get('type', '')
                    type_badge = f" [{STYLE_DIM}]{ptype}[/{STYLE_DIM}]" if ptype else ""
                    console.print(f"[{STYLE_DIM}]{branch}[/{STYLE_DIM}][{STYLE_COMMAND}]{parent['path']}[/{STYLE_COMMAND}]{type_badge}")
                    console.print(f"[{STYLE_DIM}]{cont}[/{STYLE_DIM}]{parent['table']}")

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def children(
    model_name: str = typer.Argument(..., help="Model name"),
    all_descendants: bool = typer.Option(False, "-a", "--all", help="Get all descendants (recursive)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Downstream dependencies (direct or all descendants)

    Examples:
        meta children -j model_name                 # Direct children (old format)
        meta children -a model_name                 # Tree view
        meta children -a -j model_name              # Nested JSON (<=20) or flat array (>20)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = ChildrenCommand(Config.from_config_or_env(), manifest_path, model_name, effective_use_dev, json_output, recursive=all_descendants).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            print()
            if all_descendants and result and isinstance(result[0], dict) and 'children' in result[0]:
                # Hierarchical tree output
                tree = Tree(f"[bold green]👶 All children: {model_name}[/bold green]")
                _build_tree_recursive(tree, result)
                console.print(tree)
            else:
                # Tree-style output (no truncation)
                mode = "All children" if all_descendants else "Direct children"
                console.print(f"[bold green]⬇️ {mode}: {model_name} ({len(result)})[/bold green]")
                for i, child in enumerate(result):
                    is_last = (i == len(result) - 1)
                    branch = "└── " if is_last else "├── "
                    cont = "    " if is_last else "│   "
                    ctype = child.get('type', '')
                    type_badge = f" [{STYLE_DIM}]{ctype}[/{STYLE_DIM}]" if ctype else ""
                    console.print(f"[{STYLE_DIM}]{branch}[/{STYLE_DIM}][{STYLE_COMMAND}]{child['path']}[/{STYLE_COMMAND}]{type_badge}")
                    console.print(f"[{STYLE_DIM}]{cont}[/{STYLE_DIM}]{child['table']}")

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def refresh(
    dev: bool = typer.Option(False, "--dev", "-d", help="Parse local project instead of syncing from remote"),
) -> None:
    """
    Refresh dbt artifacts (manifest.json + catalog.json)

    Production mode (default):
      Downloads latest artifacts to ~/dbt-state/
      - manifest.json (metadata for all models)
      - catalog.json (column types from database)
      Always runs with --force (immediate sync)

    Dev mode (--dev):
      Parses local dbt project to ./target/manifest.json
      Runs: dbt parse --target dev
      Use after: Editing models, schema.yml, dbt_project.yml

    Examples:
      meta refresh              # Sync production artifacts from remote storage
      meta refresh --dev        # Parse local project (dev mode)
    """
    try:
        RefreshCommand(use_dev=dev).execute()
        console.print("[green]✅ Artifacts refreshed successfully[/green]")
    except DbtMetaError as e:
        handle_error(e)
    except Exception as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Failed to refresh artifacts: {e!s}")
        raise typer.Exit(code=1) from None


def _format_size(num_bytes: int) -> str:
    """Human-readable byte size (KB/MB/GB/TB), binary units. JSON keeps raw bytes."""
    size = float(num_bytes)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(size) < 1024.0 or unit == 'TB':
            return f"{size:.0f} {unit}" if unit == 'B' else f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"


@app.command()
def context(
    model_names: list[str] = typer.Argument(..., help="One or more model names"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Full queryable-shape bundle for one or more models (one call before BigQuery)

    Bundles FQN, materialization, partition/cluster/unique_key, row_count/bytes,
    table description, and columns (type + description) so you can write a precise
    query without exploratory SELECT */DISTINCT/COUNT probes. Output is always a
    JSON object keyed by model name; not-found models become null + a warning.

    Examples:
        meta context -j core_client__events                 # single, keyed
        meta context -j model_a model_b model_c             # batch in one JSON
        meta context --dev -j customers                     # dev schema
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)

        # Dedup preserving order.
        ordered_names = list(dict.fromkeys(model_names))

        results: dict[str, Optional[dict[str, Any]]] = {}
        for name in ordered_names:
            bundle = ContextCommand(
                Config.from_config_or_env(), manifest_path, name, effective_use_dev, json_output
            ).execute()
            results[name] = bundle
            if bundle is None and not json_output:
                console.print(f"[{STYLE_ERROR}]Model not found:[/{STYLE_ERROR}] {name}")

        if json_output:
            print(json.dumps(results, indent=2))
        else:
            for bundle in results.values():
                if bundle is None:
                    continue
                _render_context_bundle(bundle)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _render_context_bundle(bundle: dict[str, Any]) -> None:
    """Render a single context bundle as Rich tables (header + columns)."""
    print()
    header = Table(
        title=f"[bold green not italic]Context: {bundle['name']}[/bold green not italic]",
        show_header=False,
    )
    header.add_column("Field", style=STYLE_COMMAND, no_wrap=True)
    header.add_column("Value", style="white")

    header.add_row("Full Name:", bundle['full_name'])
    header.add_row("Materialized:", bundle['materialized'])
    if bundle.get('description'):
        header.add_row("Description:", bundle['description'])
    header.add_row("Partition By:", str(bundle.get('partition_by') or '(none)'))
    cluster_by = bundle.get('cluster_by')
    header.add_row("Cluster By:", ', '.join(cluster_by) if cluster_by else '(none)')
    unique_key = bundle.get('unique_key')
    if isinstance(unique_key, list):
        unique_key = ', '.join(unique_key)
    header.add_row("Unique Key:", str(unique_key or '(none)'))
    row_count = bundle.get('row_count')
    header.add_row("Rows:", f"{row_count:,}" if isinstance(row_count, int) else '(unknown)')
    byte_size = bundle.get('bytes')
    header.add_row("Size:", _format_size(byte_size) if isinstance(byte_size, int) else '(unknown)')
    header.add_row("Tags:", ', '.join(bundle['tags']) if bundle.get('tags') else '(none)')
    console.print(header)

    cols = Table(show_header=True, header_style="bold green")
    cols.add_column("Name", style=STYLE_COMMAND, no_wrap=True)
    cols.add_column("Type", style="white")
    cols.add_column("Description", style=STYLE_DESCRIPTION)
    for col in bundle.get('columns', []):
        cols.add_row(col['name'], col.get('data_type', ''), col.get('description', '') or "(no description)")
    console.print(cols)


# =============================================================================
# Optimization Commands
# =============================================================================


@app.command()
def analyze(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
) -> None:
    """
    Analyze model partitioning/clustering effectiveness

    Combines manifest metadata with BigQuery monitoring data to identify
    optimization opportunities.

    Examples:
        meta analyze core_client__events          # Full analysis
        meta analyze -j core_client__events       # JSON output
    """
    try:
        manifest_path, _ = get_manifest_path(manifest, False)
        result = AnalyzeCommand(Config.from_config_or_env(), manifest_path, model_name, False, json_output).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            _print_analyze_result(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_analyze_result(result: dict[str, Any]) -> None:
    """Pretty print analyze command result."""
    print()
    console.print(f"[bold green]Model:[/bold green] {result['model']}")
    console.print(f"[bold green]Table:[/bold green] {result['table']}")
    print()

    # Config section
    config = result.get('config', {})
    console.print("[bold green]Config:[/bold green]")
    partition = config.get('partition_by')
    partition_type = config.get('partition_type', '')
    if partition:
        console.print(f"  partition_by: {partition} ({partition_type})")
    else:
        console.print(f"  partition_by: [{STYLE_DIM}]not set[/{STYLE_DIM}]")

    cluster = config.get('cluster_by', [])
    if cluster:
        console.print(f"  cluster_by: [{', '.join(cluster)}]")
    else:
        console.print(f"  cluster_by: [{STYLE_DIM}]not set[/{STYLE_DIM}]")

    console.print(f"  materialized: {config.get('materialized', 'unknown')}")

    exp_days = config.get('partition_expiration_days')
    if exp_days:
        console.print(f"  partition_expiration: {exp_days} days")
    print()

    # Storage section
    storage = result.get('storage')
    if storage:
        console.print("[bold green]Storage:[/bold green]")
        console.print(f"  Total: {storage.get('total_gb', 0):.1f} GB | Active: {storage.get('active_gb', 0):.1f} GB")
        console.print(f"  Partitions: {storage.get('partition_count', 0)} | Rows: {storage.get('row_count', 0):,}")
        console.print(f"  Cost: ${storage.get('cost_monthly_usd', 0):.2f}/month")
        print()

    # Usage section
    usage = result.get('usage')
    if usage:
        console.print("[bold green]Usage (30d):[/bold green]")
        console.print(f"  Queries: {usage.get('query_count', 0):,}")
        print()

    # Recommendations section
    recommendations = result.get('recommendations', [])
    if recommendations:
        console.print("[bold green]Recommendations:[/bold green]")
        for i, rec in enumerate(recommendations, 1):
            priority = rec.get('priority', 'MEDIUM')
            if priority == 'HIGH':
                color = 'red'
            elif priority == 'MEDIUM':
                color = 'yellow'
            else:
                color = 'dim'
            console.print(f"  {i}. [{color}][{priority}][/{color}] {rec.get('message', '')}")
            if rec.get('impact'):
                console.print(f"     [{STYLE_DIM}]{rec['impact']}[/{STYLE_DIM}]")
    else:
        console.print(f"[{STYLE_DIM}]No optimization recommendations[/{STYLE_DIM}]")


@app.command()
def hotspots(
    limit: int = typer.Option(10, "-n", "--limit", help="Number of hotspots to show"),
    min_gb: float = typer.Option(1.0, "--min-gb", help="Minimum table size in GB"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
) -> None:
    """
    Find models with highest optimization potential

    Analyzes all tables and scores them based on partitioning,
    clustering, and query patterns.

    Examples:
        meta hotspots                     # Top 20 optimization candidates
        meta hotspots -n 10               # Top 10
        meta hotspots --min-gb 10         # Only tables > 10 GB
        meta hotspots -j                  # JSON output
    """
    try:
        manifest_path, _ = get_manifest_path(manifest, False)
        result = HotspotsCommand(Config.from_config_or_env(), manifest_path, limit, min_gb, json_output).execute()

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            _print_hotspots_result(result, limit)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_hotspots_result(result: dict[str, Any], limit: int) -> None:
    """Pretty print hotspots command result."""
    print()
    summary = result.get('summary', {})
    console.print(f"[bold green]Optimization Hotspots[/bold green] (top {limit})")
    console.print(
        f"Analyzed: {summary.get('total_tables_analyzed', 0)} tables | "
        f"Issues: {summary.get('tables_with_issues', 0)} | "
        f"Size: {summary.get('total_size_gb', 0):.1f} GB"
    )
    # Total BigQuery costs (all usage)
    bq_cost = summary.get('bq_total_cost_7d', 0)
    bq_slots = summary.get('bq_total_slot_hours_7d', 0)
    bq_queries = summary.get('bq_total_queries_7d', 0)
    bq_slots_str = f"{bq_slots:.0f}h" if bq_slots >= 1 else f"{bq_slots * 60:.0f}m"
    console.print(
        f"BigQuery (7d): €{bq_cost:.2f} cost | {bq_slots_str} compute | {bq_queries:,} queries"
    )
    # DBT runs only
    dbt_cost = summary.get('dbt_query_cost_7d', 0)
    dbt_slots = summary.get('dbt_slot_hours_7d', 0)
    dbt_slots_str = f"{dbt_slots:.0f}h" if dbt_slots >= 1 else f"{dbt_slots * 60:.0f}m"
    console.print(
        f"DBT runs (7d): €{dbt_cost:.2f} cost | {dbt_slots_str} compute | "
        f"Est. billing savings: €{summary.get('total_billing_savings_eur', 0):.2f}/mo"
    )
    print()

    hotspots = result.get('hotspots', [])
    if not hotspots:
        console.print(f"[{STYLE_DIM}]No optimization opportunities found[/{STYLE_DIM}]")
        return

    # Create table for hotspots (use wide console to prevent truncation)
    wide_console = Console(width=200)
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("#", style="bold", width=2)
    table.add_column("Model", style="cyan")
    table.add_column("Cost", justify="right")
    table.add_column("Scan", justify="right")
    table.add_column("Compute", justify="right")
    table.add_column("Queries", justify="right")
    table.add_column("Storage", justify="right")
    table.add_column("Incr", justify="center")
    table.add_column("Part", justify="center")
    table.add_column("Clust", justify="center")

    for i, hs in enumerate(hotspots, 1):
        # Get triggered criteria for highlighting
        scoring_details = hs.get('scoring_details', [])
        triggered = {d.get('criterion') for d in scoring_details}

        # Model name with color based on score
        score = hs.get('score', 0)
        model = hs.get('model') or hs.get('table', '')
        if score >= 500:
            model_fmt = f"[red]{model}[/red]"
        elif score >= 200:
            model_fmt = f"[yellow]{model}[/yellow]"
        else:
            model_fmt = model

        # Metrics with bold red if triggered
        query_cost = hs.get('query_cost_7d', 0)
        cost_val = f"€{query_cost:.2f}/w"
        cost_fmt = f"[bold red]{cost_val}[/bold red]" if 'query_cost' in triggered else cost_val

        gb_per_query = hs.get('gb_per_query', 0)
        scan_val = f"{gb_per_query:.1f} GB" if gb_per_query > 0.1 else "-"
        scan_bold = 'ineffective_partition' in triggered or 'high_scan' in triggered
        scan_fmt = f"[bold red]{scan_val}[/bold red]" if scan_bold else scan_val

        query_count = hs.get('query_count_7d', 0)
        slot_hours = hs.get('slot_hours_7d', 0)
        mins_per_query = (slot_hours * 60) / query_count if slot_hours > 0 and query_count > 0 else 0
        compute_val = f"{mins_per_query:.1f} min" if mins_per_query > 0 else "-"
        compute_fmt = f"[bold red]{compute_val}[/bold red]" if 'high_slot' in triggered else compute_val

        queries_val = f"{query_count}/w" if query_count > 0 else "-"

        total_gb = hs.get('total_gb', 0)
        storage_val = f"{total_gb:.1f} GB"
        storage_fmt = f"[bold red]{storage_val}[/bold red]" if 'unused' in triggered else storage_val

        # Config flags
        incr_fmt = "[bold green]yes[/bold green]" if hs.get('is_incremental') else "[bold red]no[/bold red]"
        part_fmt = "[bold green]yes[/bold green]" if hs.get('is_partitioned') else "[bold red]no[/bold red]"
        clust_fmt = "[bold green]yes[/bold green]" if hs.get('is_clustered') else "[bold red]no[/bold red]"

        table.add_row(
            str(i), model_fmt, cost_fmt, scan_fmt, compute_fmt,
            queries_val, storage_fmt, incr_fmt, part_fmt, clust_fmt
        )

    wide_console.print(table)
    print()

    # Dataset billing recommendations block
    billing_recs = result.get('dataset_billing_recommendations', [])
    if billing_recs:
        console.print(f"[bold cyan]Dataset Billing Recommendations[/bold cyan] (top {len(billing_recs)})")
        print()

        for i, rec in enumerate(billing_recs, 1):
            dataset = rec.get('dataset', '')
            tables_physical = rec.get('tables_recommend_physical', 0)
            total_tables = rec.get('total_tables', 0)
            net_savings = rec.get('net_savings_eur', 0)
            recommended = rec.get('recommended_billing', 'PHYSICAL')
            console.print(f"[bold]{i}.[/bold] {dataset} ({tables_physical}/{total_tables} tables → {recommended})")
            console.print(f"   [green]€{net_savings:.2f}/mo net savings[/green]")
            print()

        console.print(f"[{STYLE_DIM}]⚠ Run: ALTER SCHEMA `dataset` SET OPTIONS(storage_billing_model='..')[/{STYLE_DIM}]")


@app.command()
def branch(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """
    Analyze optimization across model branch

    Examines upstream and downstream models to identify alignment
    issues between partitioning/clustering configurations.

    Examples:
        meta branch core_client__events          # Branch analysis
        meta branch -j core_client__events       # JSON output
    """
    try:
        manifest_path, _ = get_manifest_path(manifest, False)
        # Branch reads root's compiled_code to extract filter patterns
        # for upstream/downstream alignment — without it the output is
        # heavily degraded (no filter analysis), so apply the same
        # pre-flight as the optimize advisors.
        _preflight_compiled_sql_by_path(manifest_path, manifest, no_compile, json_output)
        result = BranchCommand(Config.from_config_or_env(), manifest_path, model_name, False, json_output).execute()

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            _print_branch_result(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_branch_result(result: dict[str, Any]) -> None:
    """Pretty print branch command result."""
    print()
    console.print(f"[bold green]Branch Analysis:[/bold green] {result['root']}")

    root_config = result.get('root_config', {})
    partition = root_config.get('partition_by') or 'not set'
    cluster = root_config.get('cluster_by', [])
    cluster_str = ', '.join(cluster) if cluster else 'not set'
    console.print(f"  partition_by: {partition} | cluster_by: [{cluster_str}]")
    print()

    # Upstream
    upstream = result.get('upstream', [])
    if upstream:
        console.print(f"[bold green]Upstream ({len(upstream)} models):[/bold green]")
        for i, u in enumerate(upstream):
            is_last = (i == len(upstream) - 1)
            branch_char = "└── " if is_last else "├── "
            cont_char = "    " if is_last else "│   "

            impact = u.get('impact', 'N/A')
            if impact == 'HIGH':
                impact_color = 'red'
            elif impact == 'MEDIUM':
                impact_color = 'yellow'
            else:
                impact_color = 'green'

            console.print(f"[{STYLE_DIM}]{branch_char}[/{STYLE_DIM}][{STYLE_COMMAND}]{u.get('model', '')}[/{STYLE_COMMAND}]")

            details = u.get('details', [])
            for detail in details:
                if 'aligned' in detail.lower():
                    console.print(f"[{STYLE_DIM}]{cont_char}[/{STYLE_DIM}][green]✓ {detail}[/green]")
                else:
                    console.print(f"[{STYLE_DIM}]{cont_char}[/{STYLE_DIM}][{impact_color}]⚠️ {detail}[/{impact_color}]")
        print()

    # Downstream
    downstream = result.get('downstream', [])
    if downstream:
        console.print(f"[bold green]Downstream ({len(downstream)} models):[/bold green]")
        for i, d in enumerate(downstream):
            is_last = (i == len(downstream) - 1)
            branch_char = "└── " if is_last else "├── "
            cont_char = "    " if is_last else "│   "

            alignment = d.get('alignment', 'N/A')
            if alignment == 'GOOD':
                align_color = 'green'
            elif alignment == 'SUBOPTIMAL':
                align_color = 'yellow'
            else:
                align_color = 'red'

            console.print(f"[{STYLE_DIM}]{branch_char}[/{STYLE_DIM}][{STYLE_COMMAND}]{d.get('model', '')}[/{STYLE_COMMAND}]")

            details = d.get('details', [])
            for detail in details:
                if 'partition' in detail.lower() or 'cluster' in detail.lower():
                    if 'not in' in detail.lower():
                        console.print(f"[{STYLE_DIM}]{cont_char}[/{STYLE_DIM}][{align_color}]⚠️ {detail}[/{align_color}]")
                    else:
                        console.print(f"[{STYLE_DIM}]{cont_char}[/{STYLE_DIM}][green]✓ {detail}[/green]")
                else:
                    console.print(f"[{STYLE_DIM}]{cont_char}[/{STYLE_DIM}][{STYLE_DIM}]{detail}[/{STYLE_DIM}]")
        print()

    # Recommendations
    recommendations = result.get('recommendations', [])
    if recommendations:
        console.print("[bold green]Recommendations:[/bold green]")
        for i, rec in enumerate(recommendations, 1):
            priority = rec.get('priority', 'MEDIUM')
            if priority == 'HIGH':
                color = 'red'
            elif priority == 'MEDIUM':
                color = 'yellow'
            else:
                color = 'dim'

            model = rec.get('model', '')
            action = rec.get('action', '')
            impact = rec.get('impact', '')

            console.print(f"  {i}. [{color}][{priority}][/{color}] {model}: {action}")
            if impact:
                console.print(f"     [{STYLE_DIM}]{impact}[/{STYLE_DIM}]")
    else:
        console.print(f"[{STYLE_DIM}]No branch optimization recommendations[/{STYLE_DIM}]")


# ============================================================================
# Power BI Commands
# ============================================================================

powerbi_app = typer.Typer(
    help="Power BI dashboard metadata — build artifacts, list/find/show reports.",
    no_args_is_help=True,
)
app.add_typer(powerbi_app, name="powerbi")


@powerbi_app.command("artifacts")
def powerbi_artifacts(
    raw: Optional[str] = typer.Option(
        None, "--raw",
        help="Raw scanResult output path (default: <prod-manifest-dir>/powerbi_raw.json)",
    ),
    output: Optional[str] = typer.Option(
        None, "-o", "--output",
        help="Compact index output path (default: <prod-manifest-dir>/powerbi_index.json)",
    ),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    no_layouts: bool = typer.Option(
        False, "--no-layouts",
        help="Skip the Fabric getDefinition pass (no per-page visual layout)",
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output summary as JSON"),
) -> None:
    """Scan workspaces and build the compact agent index in one shot.

    Writes powerbi_raw.json (raw scanResult) and powerbi_index.json (compact
    index) — both default to the prod-manifest directory (~/dbt-state/).
    Running this manually overwrites the files managed by the cron job.

    By default a second pass calls Fabric getDefinition per report to attach
    per-page visual layout (needs a Fabric-scoped SP that is a member of the
    workspaces). Use --no-layouts to skip it.
    """
    import os

    from dbt_meta.command_impl import powerbi as pbi

    try:
        manifest_path, _ = get_manifest_path(manifest, False)
        manifest_dir = os.path.dirname(manifest_path)
        raw_path = raw or os.path.join(manifest_dir, "powerbi_raw.json")
        index_path = output or os.path.join(manifest_dir, "powerbi_index.json")
        result = pbi.artifacts_cmd(
            Config.from_config_or_env(), manifest_path, raw_path, index_path,
            with_layouts=not no_layouts,
        )
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        layouts = result.get("layouts") or {}
        layout_line = ""
        if layouts:
            layout_line = (
                f"  layouts → {layouts['with_layout']}/{layouts['total']} reports\n"
            )
        console.print(
            f"[green]Scanned & indexed[/green] {result['workspaces']} workspaces, "
            f"{result['reports']} reports, {result['metrics']} metrics\n"
            f"{layout_line}"
            f"  raw   → {result['raw_path']}\n"
            f"  index → {result['index_path']}"
        )


@powerbi_app.command("find")
def powerbi_find(
    query: str = typer.Argument(..., help="Report / dataset / table / metric substring"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", help="Explicit powerbi_index.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Find reports / metrics behind a dashboard name or metric."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_artifact

    try:
        path = find_powerbi_artifact(explicit_path=artifact)
        result = pbi.find_in_index(path, query)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_find(result)


@powerbi_app.command("list")
def powerbi_list(
    artifact: Optional[str] = typer.Option(
        None, "--artifact", help="Explicit powerbi_index.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """List all reports (workspace | report | dataset | tables) for discovery."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_artifact

    try:
        path = find_powerbi_artifact(explicit_path=artifact)
        result = pbi.list_cmd(path)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_list(result)


def _print_powerbi_list(result: dict[str, Any]) -> None:
    """Render `meta powerbi list` results."""
    from rich.table import Table

    reports = result.get("reports", [])
    if not reports:
        console.print(f"[{STYLE_DIM}]No reports in index[/{STYLE_DIM}]")
        return
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Workspace", style="cyan")
    table.add_column("Report", style="white")
    table.add_column("Dataset", style="white")
    table.add_column("Tables", justify="right", style="green")
    for r in reports:
        dataset = "" if r["dataset"] == r["report"] else r["dataset"]
        table.add_row(r["workspace"], r["report"], dataset, str(len(r["tables"])))
    console.print(table)
    console.print(f"[{STYLE_DIM}]{result.get('count', len(reports))} reports[/{STYLE_DIM}]")


@powerbi_app.command("show")
def powerbi_show(
    report: str = typer.Argument(..., help="Report name (exact or substring)"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", help="Explicit powerbi_index.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Show one report's full breakdown — tables, SQL analysis, dbt mapping."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_artifact

    try:
        path = find_powerbi_artifact(explicit_path=artifact)
        result = pbi.show_report(path, report)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_show(result)


def _print_powerbi_find(result: dict[str, Any]) -> None:
    """Render `meta powerbi find` results."""
    from rich.table import Table

    reports = result.get("reports", [])
    metrics = result.get("metrics", {})
    if not reports and not metrics:
        console.print(
            f"[{STYLE_DIM}]No matches for {result.get('query')!r}[/{STYLE_DIM}]"
        )
        return
    if reports:
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Workspace", style="cyan")
        table.add_column("Report", style="white")
        table.add_column("Dataset", style="white")
        table.add_column("Tables", justify="right", style="green")
        for r in reports:
            table.add_row(
                r["workspace"], r["report"], r["dataset"], str(len(r["tables"]))
            )
        console.print(table)
    if metrics:
        console.print()
        console.print("[bold cyan]Metrics[/bold cyan]")
        for name, tables in metrics.items():
            joined = ", ".join(tables) if tables else "[dim]?[/dim]"
            console.print(f"  [white]{name}[/white] → {joined}")


def _print_powerbi_show(result: dict[str, Any]) -> None:
    """Render `meta powerbi show` results."""
    from rich.table import Table

    console.print(
        f"[bold green]{result['report']}[/bold green] "
        f"([cyan]{result['workspace']}[/cyan] / dataset "
        f"[white]{result['dataset']}[/white])"
    )
    tables = result.get("tables", [])
    if tables:
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("BigQuery Table", style="white", width=55)
        table.add_column("Status", style="cyan")
        table.add_column("dbt Model", style="green")
        for t in tables:
            status = t["status"]
            color = {"model": "green", "source": "cyan", "external": "yellow"}.get(
                status, "white"
            )
            table.add_row(
                t["bq"], f"[{color}]{status}[/{color}]", t.get("dbt_model") or ""
            )
        console.print(table)
    sql = result.get("sql_analysis", [])
    if sql:
        console.print()
        console.print("[bold cyan]SQL analysis (logic outside dbt)[/bold cyan]")
        for s in sql:
            console.print(
                f"  [white]{s['query']}[/white] "
                f"[{STYLE_DIM}]({s['parse_status']})[/{STYLE_DIM}]"
            )
            if s.get("filters"):
                console.print(f"    filters: {', '.join(s['filters'])}")
            if s.get("joins"):
                console.print(f"    joins:   {', '.join(s['joins'])}")
            if s.get("group_by"):
                console.print(f"    group:   {', '.join(s['group_by'])}")


@powerbi_app.command("reports")
def powerbi_reports(
    model: str = typer.Argument(..., help="dbt model name or substring"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", help="Explicit powerbi_index.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Reverse lookup: find all Power BI reports that use a given dbt model."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_artifact

    try:
        path = find_powerbi_artifact(explicit_path=artifact)
        result = pbi.reports_for_model_cmd(path, model)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_reports(result)


@powerbi_app.command("measures")
def powerbi_measures(
    report: str = typer.Argument(..., help="Report name (exact or substring)"),
    raw: Optional[str] = typer.Option(
        None, "--raw", help="Explicit powerbi_raw.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Show all DAX measures (+ expressions) for the dataset behind a report."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_raw

    try:
        raw_path = find_powerbi_raw(explicit_path=raw)
        result = pbi.measures_cmd(raw_path, report)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_measures(result)


@powerbi_app.command("source")
def powerbi_source(
    report: str = typer.Argument(..., help="Report name (exact or substring)"),
    raw: Optional[str] = typer.Option(
        None, "--raw", help="Explicit powerbi_raw.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Show Power Query M-expressions for each table in a report's dataset."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_raw

    try:
        raw_path = find_powerbi_raw(explicit_path=raw)
        result = pbi.source_cmd(raw_path, report)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_source(result)


@powerbi_app.command("owners")
def powerbi_owners(
    report: str = typer.Argument(..., help="Report name (exact or substring)"),
    raw: Optional[str] = typer.Option(
        None, "--raw", help="Explicit powerbi_raw.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Show Owner-level users and last-modified info for a report."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_raw

    try:
        raw_path = find_powerbi_raw(explicit_path=raw)
        result = pbi.owners_cmd(raw_path, report)
    except FileNotFoundError as e:
        handle_error(DbtMetaError(str(e)), json_output)
        return
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_owners(result)


@powerbi_app.command("cost")
def powerbi_cost(
    report: str = typer.Argument(..., help="Report name (exact or substring)"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", help="Explicit powerbi_index.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Show per-table query cost metrics (7-day) for the tables behind a report."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.powerbi.artifact import find_powerbi_artifact

    try:
        artifact_path = find_powerbi_artifact(explicit_path=artifact)
        result = pbi.cost_cmd(artifact_path, report)
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_cost(result)


def _print_powerbi_reports(result: dict[str, Any]) -> None:
    """Render `meta powerbi reports` results."""
    from rich.table import Table

    model = result.get("model", "")
    reports = result.get("reports", [])
    if not reports:
        console.print(
            f"[{STYLE_DIM}]No Power BI reports use model '{model}'[/{STYLE_DIM}]"
        )
        return
    console.print(f"[bold cyan]Power BI reports using[/bold cyan] [green]{model}[/green]")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Workspace", style="cyan")
    table.add_column("Report", style="white")
    table.add_column("Dataset", style="white")
    table.add_column("Matched Tables", style="green")
    for r in reports:
        table.add_row(
            r["workspace"],
            r["report"],
            r["dataset"],
            "\n".join(r.get("matched_tables", [])),
        )
    console.print(table)


def _print_powerbi_measures(result: dict[str, Any]) -> None:
    """Render `meta powerbi measures` results."""
    from rich.table import Table

    measures = result.get("measures", [])
    console.print(
        f"[bold green]{result['report']}[/bold green] — "
        f"[cyan]{len(measures)} measures[/cyan]"
    )
    if not measures:
        return
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Table", style="cyan", width=20)
    table.add_column("Measure", style="white", width=25)
    table.add_column("Hidden", justify="center", width=7)
    table.add_column("Expression", style="dim")
    for m in measures:
        hidden_marker = "✗" if m.get("hidden") else ""
        table.add_row(m["table"], m["name"], hidden_marker, m["expression"])
    console.print(table)


def _print_powerbi_source(result: dict[str, Any]) -> None:
    """Render `meta powerbi source` results."""
    sources = result.get("sources", [])
    console.print(
        f"[bold green]{result['report']}[/bold green] — "
        f"[cyan]{len(sources)} tables with source[/cyan]"
    )
    for s in sources:
        console.print(f"\n[bold cyan]{s['table']}[/bold cyan]")
        console.print(s["expression"])


def _print_powerbi_owners(result: dict[str, Any]) -> None:
    """Render `meta powerbi owners` results."""
    console.print(f"[bold green]{result['report']}[/bold green]")
    owners = result.get("owners", [])
    if owners:
        console.print(f"  [cyan]Owners:[/cyan]  {', '.join(owners)}")
    modified_by = result.get("modified_by")
    modified_at = result.get("modified_at")
    if modified_by:
        console.print(f"  [cyan]Modified by:[/cyan] {modified_by}")
    if modified_at:
        console.print(f"  [cyan]Modified at:[/cyan] {modified_at}")


@powerbi_app.command("lineage")
def powerbi_lineage(
    report: str = typer.Argument(..., help="Report name (exact or substring)"),
    artifact: Optional[str] = typer.Option(
        None, "--artifact", help="Explicit powerbi_index.json path"
    ),
    lineage: Optional[str] = typer.Option(
        None, "--lineage", help="Explicit lineage.json path"
    ),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
) -> None:
    """Show column-level upstream lineage for filter/join columns in a report's SQL."""
    from dbt_meta.command_impl import powerbi as pbi
    from dbt_meta.lineage.finder import find_lineage_artifact
    from dbt_meta.powerbi.artifact import find_powerbi_artifact

    try:
        artifact_path = find_powerbi_artifact(explicit_path=artifact)
        lineage_path = lineage or find_lineage_artifact()
        result = pbi.lineage_cmd(artifact_path, lineage_path, report)
    except DbtMetaError as e:
        handle_error(e, json_output)
        return
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        _print_powerbi_lineage(result)


def _print_powerbi_lineage(result: dict[str, Any]) -> None:
    """Render `meta powerbi lineage` results."""
    cols = result.get("columns", [])
    console.print(
        f"[bold cyan]Column lineage:[/bold cyan] [green]{result['report']}[/green]"
    )
    if not cols:
        console.print(f"  [{STYLE_DIM}]No lineage found for filter/join columns[/{STYLE_DIM}]")
        return
    for col in cols:
        console.print(
            f"\n  [cyan]{col['dbt_model']}[/cyan].[white]{col['bq_column']}[/white]"
        )
        for ancestor in col["ancestors"]:
            console.print(f"    ← {ancestor}")


def _print_powerbi_cost(result: dict[str, Any]) -> None:
    """Render `meta powerbi cost` results."""
    from rich.table import Table

    console.print(f"[bold cyan]Query cost (7d):[/bold cyan] [green]{result['report']}[/green]")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("BQ Table", style="white")
    table.add_column("Status", style="cyan")
    table.add_column("Cost USD", style="yellow", justify="right")
    table.add_column("Queries", style="white", justify="right")
    table.add_column("Cache Hit", style="green", justify="right")
    for t in result.get("tables", []):
        cost = f"${t['query_cost_usd']:.4f}" if t["query_cost_usd"] is not None else "—"
        queries = str(t["query_count"]) if t["query_count"] is not None else "—"
        cache = f"{t['cache_hit_ratio']:.0%}" if t["cache_hit_ratio"] is not None else "—"
        table.add_row(t["bq"], t["status"], cost, queries, cache)
    console.print(table)


# ============================================================================
# Column-Level Lineage Commands
# ============================================================================

def _lineage_artifact_path(explicit: Optional[str]) -> str:
    """Locate lineage.json or exit with a clear error."""
    from dbt_meta.command_impl import lineage as lineage_impl

    try:
        return lineage_impl.find_artifact(explicit=explicit)
    except FileNotFoundError as e:
        Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {e!s}")
        raise typer.Exit(code=1) from None


def _validate_column_ref(column_ref: str, json_output: bool) -> None:
    """Reject inputs that can't possibly identify a model.column pair.

    Caught here rather than letting the lookup fail with the generic
    "not found in lineage graph" message — that message is correct for
    ``unknown_model.col`` but actively misleading when the user typed
    ``some_model`` (no separator) or left the column empty.
    """
    has_separator = "." in column_ref or ":" in column_ref
    parts_ok = False
    if has_separator:
        if ":" in column_ref:
            model_part, _, col_part = column_ref.rpartition(":")
        else:
            model_part, _, col_part = column_ref.rpartition(".")
        parts_ok = bool(model_part) and bool(col_part)
    if has_separator and parts_ok:
        return
    msg = (
        f"Invalid column reference {column_ref!r}: expected 'model.column' "
        "or 'model:column'"
    )
    if json_output:
        print(json.dumps({"error": msg}))
    else:
        Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {msg}")
    raise typer.Exit(code=1)


@lineage_app.command("build")
def lineage_build(
    output: Optional[str] = typer.Option(None, "-o", "--output", help="Custom output path"),
    manifest_path: Optional[str] = typer.Option(None, "--manifest", help="Explicit manifest.json path"),
    catalog_path: Optional[str] = typer.Option(None, "--catalog", help="Explicit catalog.json path"),
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output (build summary)"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Print per-model progress"),
    timeout: int = typer.Option(30, "--timeout", help="Per-model timeout in seconds (0 disables)"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """Build column-level lineage artifact from manifest + catalog.

    Reads compiled SQL from manifest.json, parses with SQLGlot, resolves
    column-to-column dependencies and writes lineage.json.

    Lineage is a prod-only concept (column-level lineage of the deployed
    state). The artifact is written next to the production manifest
    (default ~/dbt-state/lineage.json), mirroring its location.

    Examples:
        meta lineage build              # build prod artifact
        meta lineage build -o my.json   # custom output
    """
    import os
    import time

    import orjson

    from dbt_meta.lineage import LineageBuilder, save_artifact

    # Resolve manifest path (reuse existing finder)
    manifest_file, _ = get_manifest_path(manifest_path, use_dev=False)

    # Resolve catalog path
    if catalog_path:
        catalog_file: Optional[str] = catalog_path
    else:
        config = Config.from_config_or_env()
        catalog_file = config.prod_catalog_path

    # Load manifest + catalog
    try:
        manifest = orjson.loads(Path(manifest_file).read_bytes())
    except Exception as e:
        Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] failed to read manifest: {e!s}")
        raise typer.Exit(code=1) from None

    # Mandatory compiled-SQL pre-flight. SQLGlot's column lineage extractor
    # needs every model's compiled_code; building the graph on a `dbt parse`
    # manifest produces an artifact that's mostly "skipped: no compiled_code"
    # — useless. Force a full ``dbt compile`` when the manifest is local
    # and most models are uncompiled; hard-fail otherwise.
    manifest = _ensure_manifest_compiled(
        manifest=manifest,
        manifest_file=manifest_file,
        explicit_manifest_path=manifest_path,
        no_compile=no_compile,
        json_output=json_output,
    )

    catalog: dict[str, Any] = {}
    if catalog_file and os.path.exists(catalog_file):
        try:
            catalog = orjson.loads(Path(catalog_file).read_bytes())
        except Exception as e:
            Console(stderr=True).print(f"[yellow]Warning:[/yellow] failed to read catalog ({e!s}); continuing without column types")

    # Resolve output path
    if output:
        out_path = output
    else:
        # Mirror manifest location: same dir as the production manifest
        out_path = os.path.join(os.path.dirname(manifest_file), "lineage.json")

    if not json_output:
        console.print(f"[dim]Manifest:[/dim] {manifest_file}")
        console.print(f"[dim]Catalog:[/dim]  {catalog_file or '(none)'}")
        console.print(f"[dim]Output:[/dim]   {out_path}")
        console.print(f"[dim]Per-model timeout:[/dim] {timeout}s")
        console.print("[cyan]Building lineage graph...[/cyan]")

    progress_callback = None
    if verbose and not json_output:
        def progress_callback(idx: int, total: int, name: str, model_elapsed: float) -> None:
            tag = "[yellow]slow[/yellow]" if model_elapsed >= 3.0 else "[dim]ok[/dim]"
            console.print(f"  [{idx}/{total}] {name} ({model_elapsed:.2f}s) {tag}")

    builder = LineageBuilder(
        manifest,
        catalog,
        per_model_timeout=timeout,
        progress_callback=progress_callback,
    )
    t0 = time.time()
    graph, stats = builder.build()
    elapsed = time.time() - t0

    artifact = save_artifact(graph, out_path, warnings=stats.warnings)

    if json_output:
        print(json.dumps({
            "artifact": artifact,
            "elapsed_seconds": round(elapsed, 2),
            "models_total": stats.models_total,
            "models_parsed": stats.models_parsed,
            "models_skipped_no_sql": stats.models_skipped_no_sql,
            "models_skipped_parse_error": stats.models_skipped_parse_error,
            "models_skipped_timeout": stats.models_skipped_timeout,
            "nodes": graph.node_count,
            "edges": graph.edge_count,
            "warnings_count": len(stats.warnings),
            "slow_models": [{"model": m, "elapsed": round(e, 2)} for m, e in stats.slow_models],
        }, indent=2))
        return

    console.print(f"[green]✅ Built in {elapsed:.1f}s[/green]")
    console.print(f"   Parsed: {stats.models_parsed}/{stats.models_total} models")
    if stats.models_skipped_no_sql:
        console.print(f"   [yellow]Skipped (no compiled_code):[/yellow] {stats.models_skipped_no_sql}")
    if stats.models_skipped_parse_error:
        console.print(f"   [yellow]Skipped (parse error):[/yellow] {stats.models_skipped_parse_error}")
    if stats.models_skipped_timeout:
        console.print(f"   [yellow]Skipped (timeout >{timeout}s):[/yellow] {stats.models_skipped_timeout}")
    console.print(f"   Graph: {graph.node_count} columns, {graph.edge_count} edges")
    console.print(f"   Artifact: {artifact}")
    if stats.slow_models:
        console.print("   [yellow]Slowest models (top 5):[/yellow]")
        for name, sec in sorted(stats.slow_models, key=lambda x: -x[1])[:5]:
            console.print(f"     · {name}: {sec:.1f}s")


@lineage_app.command("column")
def lineage_column(
    column_ref: str = typer.Argument(..., help="Column reference: 'model.column' or 'model:column'"),
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output"),
    artifact: Optional[str] = typer.Option(None, "--artifact", help="Explicit lineage.json path"),
) -> None:
    """Show upstream lineage for a column ('where does this column come from').

    Examples:
        meta lineage column core_clients.client_id
        meta lineage column -j core_clients:client_id
    """
    from dbt_meta.command_impl import lineage as lineage_impl

    _validate_column_ref(column_ref, json_output)
    artifact_path = _lineage_artifact_path(artifact)
    result = lineage_impl.column_lineage(artifact_path, column_ref, direction="upstream")

    if result is None:
        if json_output:
            print(json.dumps({"error": f"Column '{column_ref}' not found in lineage graph"}))
        else:
            Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Column '{column_ref}' not found in lineage graph")
            Console(stderr=True).print("[yellow]Hint:[/yellow] try `meta lineage build` to refresh the artifact")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
        return

    target = result["target"]
    console.print(f"[bold green]{target['model']}.{target['column']}[/bold green] [dim]({target['data_type'] or '?'})[/dim]")
    console.print(f"[dim]Direct upstream ({result['stats']['direct_count']}):[/dim]")
    for n in result["direct"]:
        console.print(f"  ← {n['model']}.{n['column']}")
    if result["stats"]["total_count"] > result["stats"]["direct_count"]:
        console.print(f"[dim]All ancestors ({result['stats']['total_count']}):[/dim]")
        for n in result["all"]:
            console.print(f"  · {n['model']}.{n['column']}")


@lineage_app.command("downstream")
def lineage_downstream(
    column_ref: str = typer.Argument(..., help="Column reference: 'model.column' or 'model:column'"),
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output"),
    artifact: Optional[str] = typer.Option(None, "--artifact", help="Explicit lineage.json path"),
) -> None:
    """Show downstream impact for a column ('what breaks if this changes').

    Examples:
        meta lineage downstream raw_clients.id
        meta lineage downstream -j staging_clients.client_id
    """
    from dbt_meta.command_impl import lineage as lineage_impl

    _validate_column_ref(column_ref, json_output)
    artifact_path = _lineage_artifact_path(artifact)
    result = lineage_impl.column_lineage(artifact_path, column_ref, direction="downstream")

    if result is None:
        if json_output:
            print(json.dumps({"error": f"Column '{column_ref}' not found in lineage graph"}))
        else:
            Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Column '{column_ref}' not found in lineage graph")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
        return

    target = result["target"]
    console.print(f"[bold green]{target['model']}.{target['column']}[/bold green] [dim]({target['data_type'] or '?'})[/dim]")
    console.print(f"[dim]Direct downstream ({result['stats']['direct_count']}):[/dim]")
    for n in result["direct"]:
        console.print(f"  → {n['model']}.{n['column']}")
    if result["stats"]["total_count"] > result["stats"]["direct_count"]:
        console.print(f"[dim]All descendants ({result['stats']['total_count']}):[/dim]")
        for n in result["all"]:
            console.print(f"  · {n['model']}.{n['column']}")


@lineage_app.command("stats")
def lineage_stats_cmd(
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output"),
    artifact: Optional[str] = typer.Option(None, "--artifact", help="Explicit lineage.json path"),
) -> None:
    """Print summary stats for a lineage artifact (size, age, warnings).

    Examples:
        meta lineage stats
        meta lineage stats -j
    """
    from dbt_meta.command_impl import lineage as lineage_impl

    artifact_path = _lineage_artifact_path(artifact)
    info = lineage_impl.lineage_stats(artifact_path)

    if json_output:
        print(json.dumps({"artifact": artifact_path, **info}, indent=2))
        return

    console.print(f"[dim]Artifact:[/dim] {artifact_path}")
    console.print(f"   Schema version: {info['schema_version']}")
    console.print(f"   Generated:      {info['generated_at']}")
    console.print(f"   Nodes:          {info['nodes']}")
    console.print(f"   Edges:          {info['edges']}")
    if info["warnings"]:
        console.print(f"   [yellow]Warnings:[/yellow]      {len(info['warnings'])}")


# ============================================================================
# Optimization Advisors (column-usage-aware)
# ============================================================================

def _load_manifest_and_catalog(
    use_dev: bool,
    manifest_path: Optional[str],
    catalog_path: Optional[str],
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Load manifest + catalog (best-effort) for advisor commands.

    Returns ``(manifest_dict, catalog_dict, resolved_manifest_path)``.
    The resolved path is used by advisors to locate the dbt project root
    for the disk-compiled SQL fallback and the on-demand ``dbt compile``.
    """
    import os

    import orjson

    manifest_file, _ = get_manifest_path(manifest_path, use_dev=use_dev)
    try:
        manifest = orjson.loads(Path(manifest_file).read_bytes())
    except (OSError, ValueError) as e:
        Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] failed to read manifest: {e!s}")
        raise typer.Exit(code=1) from None

    if catalog_path is None:
        config = Config.from_config_or_env()
        catalog_path = config.dev_catalog_path if use_dev else config.prod_catalog_path

    catalog: dict[str, Any] = {}
    if catalog_path and os.path.exists(catalog_path):
        try:
            catalog = orjson.loads(Path(catalog_path).read_bytes())
        except (OSError, ValueError) as e:
            Console(stderr=True).print(
                f"[yellow]Warning:[/yellow] failed to read catalog ({e!s}); proceeding without column types"
            )
    return manifest, catalog, manifest_file


def _preflight_compiled_sql_by_path(
    manifest_file: str,
    explicit_manifest_path: Optional[str],
    no_compile: bool,
    json_output: bool,
) -> None:
    """Path-based pre-flight wrapper for commands that pass manifest as a path.

    Single-model commands (``sql``, ``validate``, ``scan``, ``analyze``,
    ``branch``) hand the manifest path straight into the ``commands``
    layer; they never materialise the dict in the CLI. To still enforce
    "manifest must be compiled or auto-compile it", this wrapper loads,
    checks, and (if needed) runs ``dbt compile`` — re-writing the file
    on disk so the downstream layer reads the freshly compiled version.
    """
    import orjson

    try:
        manifest = orjson.loads(Path(manifest_file).read_bytes())
    except (OSError, ValueError):
        # Let the downstream layer surface the load error; preflight is
        # advisory.
        return
    _ensure_manifest_compiled(
        manifest=manifest,
        manifest_file=manifest_file,
        explicit_manifest_path=explicit_manifest_path,
        no_compile=no_compile,
        json_output=json_output,
    )


def _ensure_manifest_compiled(
    *,
    manifest: dict[str, Any],
    manifest_file: str,
    explicit_manifest_path: Optional[str],
    no_compile: bool,
    json_output: bool,
) -> dict[str, Any]:
    """Mandatory pre-flight for commands that need compiled SQL (universal).

    Used by ``lineage build`` and ``optimize cluster/partition``. The
    rule is the same in every case: if fewer than half the manifest's
    models have a populated ``compiled_code`` field, the manifest is
    almost certainly from ``dbt parse`` (no Jinja rendering). Either run
    ``dbt compile`` for the WHOLE project (no ``--select``, per project
    convention — selective compiles leave gaps), or fail with a path-
    tagged error.

    Why full-project compile rather than ``<target>+``: incremental
    selectors only compile the requested chain, leaving siblings and
    unrelated upstreams empty in the manifest. The next command — and
    even the same command on a different target — would re-trigger a
    compile. One full pass amortises the cost.
    """
    from pathlib import Path

    import orjson

    model_nodes = [
        node
        for uid, node in manifest.get("nodes", {}).items()
        if uid.startswith("model.")
    ]
    if not model_nodes:
        return manifest
    with_sql = sum(
        1 for n in model_nodes if (n.get("compiled_code") or "").strip()
    )
    total = len(model_nodes)
    if with_sql * 2 >= total:
        return manifest

    home_state = str(Path.home() / "dbt-state" / "manifest.json")
    using_prod = manifest_file == home_state
    is_explicit = explicit_manifest_path is not None

    def _fail(extra: Optional[str] = None) -> NoReturn:
        coverage = f"{with_sql}/{total} models"
        if using_prod:
            msg = (
                f"Production manifest at {manifest_file} has compiled SQL "
                f"for only {coverage}. Re-sync it from your dbt-state "
                "source — this command needs compiled_code across the "
                "project."
            )
        elif is_explicit:
            msg = (
                f"Explicit manifest at {manifest_file} has compiled SQL for "
                f"only {coverage}. Pass a manifest produced by `dbt compile` "
                "(not `dbt parse`)."
            )
        elif no_compile:
            msg = (
                f"Manifest at {manifest_file} has compiled SQL for only "
                f"{coverage} and `--no-compile` was set. Remove `--no-compile` "
                "to let the command auto-run `dbt compile`, or run "
                "`dbt compile` manually in the project."
            )
        else:
            msg = (
                f"Manifest at {manifest_file} has compiled SQL for only "
                f"{coverage} — this command can't analyse uncompiled models. "
                "Most likely cause: manifest produced by `dbt parse`. Fixes:\n"
                "  - run `dbt compile` in the project (populates all "
                "compiled_code), or\n"
                f"  - use the prod manifest: `--manifest {home_state}`, or\n"
                f"  - set DBT_PROD_MANIFEST_PATH={home_state} so the lookup "
                "prefers prod by default."
            )
        if extra:
            msg = f"{msg}\n\n{extra}"
        if json_output:
            print(json.dumps({"error": msg}))
        else:
            Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {msg}")
        raise typer.Exit(code=1)

    if no_compile or using_prod or is_explicit:
        _fail()

    from dbt_meta.usage.advisor_refresh import (
        _find_dbt_executable,
        _infer_project_root,
    )

    project_root = _infer_project_root(manifest_file)
    dbt_cmd = _find_dbt_executable(project_root) if project_root else None
    if not project_root or not dbt_cmd:
        _fail(
            "Auto-compile skipped: "
            + ("dbt CLI not found on PATH or in project venv." if project_root
               else "no `dbt_project.yml` found walking up from the manifest path.")
        )

    Console(stderr=True).print(
        f"[dim]ℹ️  Only {with_sql}/{total} models have compiled SQL — "
        f"running full `dbt compile` in {project_root} to populate the "
        "rest. This can take 5-15 min on large projects; pass "
        "`--no-compile` to skip.[/dim]"
    )
    import os
    import subprocess as _subprocess

    compile_argv: list[str] = [dbt_cmd, "compile"]
    # Use the project-local profiles.yml when present; otherwise let dbt fall
    # back to ~/.dbt/profiles.yml. Without this, projects that ship their own
    # profiles.yml at root produce 0/N compiled because dbt can't find any
    # profile and silently fails per model.
    if os.path.isfile(os.path.join(project_root, "profiles.yml")):
        compile_argv += ["--profiles-dir", project_root]

    try:
        result = _subprocess.run(
            compile_argv,
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 min — full-project compile budget
        )
    except (_subprocess.TimeoutExpired, OSError) as exc:
        _fail(f"`dbt compile` failed to launch: {exc}")

    if result.returncode != 0:
        tail = "\n      ".join((result.stderr or result.stdout or "").splitlines()[-10:])
        _fail(f"`dbt compile` exited with code {result.returncode}:\n      {tail}")

    try:
        manifest = orjson.loads(Path(manifest_file).read_bytes())
    except (OSError, ValueError) as e:
        Console(stderr=True).print(
            f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] failed to reload manifest after compile: {e!s}"
        )
        raise typer.Exit(code=1) from None

    new_model_nodes = [
        n
        for uid, n in manifest.get("nodes", {}).items()
        if uid.startswith("model.")
    ]
    new_with_sql = sum(
        1 for n in new_model_nodes if (n.get("compiled_code") or "").strip()
    )
    new_total = len(new_model_nodes)
    if new_with_sql * 2 < new_total:
        _fail(
            "`dbt compile` finished but compiled_code is still sparse "
            f"({new_with_sql}/{new_total}); check dbt project config or "
            "run `dbt compile` manually."
        )

    Console(stderr=True).print(
        f"[dim]✓ Manifest now has {new_with_sql}/{new_total} models with "
        "compiled SQL.[/dim]"
    )
    return manifest


@optimize_app.command("cluster")
def optimize_cluster(
    model: str = typer.Argument(..., help="Target model short name (e.g. core_clients)"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev manifest/catalog"),
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output"),
    top_n: int = typer.Option(4, "--top", help="Max recommendations (BigQuery cap is 4)"),
    manifest_path: Optional[str] = typer.Option(None, "--manifest", help="Explicit manifest.json path"),
    catalog_path: Optional[str] = typer.Option(None, "--catalog", help="Explicit catalog.json path"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """Recommend BigQuery clustering keys based on downstream WHERE/JOIN usage.

    Examples:
        meta optimize cluster core_internal_tracking__sessions
        meta optimize cluster -j core_clients
    """
    from dataclasses import asdict

    from dbt_meta.usage import ClusterAdvisor

    manifest, catalog, manifest_file = _load_manifest_and_catalog(use_dev, manifest_path, catalog_path)
    manifest = _ensure_manifest_compiled(
        manifest=manifest,
        manifest_file=manifest_file,
        explicit_manifest_path=manifest_path,
        no_compile=no_compile,
        json_output=json_output,
    )
    advisor = ClusterAdvisor(manifest, catalog, top_n=top_n)
    result = advisor.recommend(model)

    if json_output:
        payload = {
            "target_model": result.target_model,
            "direct_downstream_count": result.direct_downstream_count,
            "analysed_downstream_count": result.analysed_downstream_count,
            "current_partition_by": result.target_partition_by,
            "current_cluster_by": result.current_cluster_by,
            "matches_current": result.matches_current,
            "recommendations": [asdict(r) for r in result.recommendations],
            "excluded": result.excluded,
            "warnings": result.warnings,
        }
        print(json.dumps(payload, indent=2))
        return

    if result.warnings:
        for w in result.warnings:
            console.print(f"[yellow]⚠ {w}[/yellow]")
        if not result.recommendations:
            raise typer.Exit(code=1)

    console.print(f"[bold green]Clustering advisor for {result.target_model}[/bold green]")
    console.print(f"   Manifest: [dim]{manifest_file}[/dim]")
    console.print(
        f"   Direct downstream: {result.direct_downstream_count} models, "
        f"{result.analysed_downstream_count} with analyzable column references"
    )
    if result.target_partition_by:
        console.print(f"   Current partition_by: {', '.join(result.target_partition_by)}")
    if result.current_cluster_by:
        console.print(f"   Current cluster_by:   {', '.join(result.current_cluster_by)}")
    if not result.recommendations:
        console.print("[yellow]No clustering recommendations (no qualifying downstream usage)[/yellow]")
        return
    if result.matches_current:
        console.print(
            "\n[bold green]✓ Current cluster_by is already optimal "
            "— no changes needed.[/bold green]"
        )
    console.print(f"\n   Recommended cluster keys (top {len(result.recommendations)}):")
    for i, rec in enumerate(result.recommendations, 1):
        console.print(f"     {i}. [cyan]{rec.column}[/cyan] ({rec.data_type or '?'})  score={rec.score}")
        for line in rec.reasoning:
            console.print(f"        · {line}")
    if result.excluded:
        console.print("\n   [dim]Excluded:[/dim]")
        for e in result.excluded:
            console.print(f"     · {e['column']} — {e['reason']}")


@optimize_app.command("partition")
def optimize_partition(
    model: str = typer.Argument(..., help="Target model short name"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev manifest/catalog"),
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output"),
    manifest_path: Optional[str] = typer.Option(None, "--manifest", help="Explicit manifest.json path"),
    catalog_path: Optional[str] = typer.Option(None, "--catalog", help="Explicit catalog.json path"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt compile` when the local manifest lacks compiled SQL"),
) -> None:
    """Recommend BigQuery partition column based on downstream range/equality filters.

    Examples:
        meta optimize partition core_clients
        meta optimize partition -j core_events
    """
    from dataclasses import asdict

    from dbt_meta.usage import PartitionAdvisor

    manifest, catalog, manifest_file = _load_manifest_and_catalog(use_dev, manifest_path, catalog_path)
    manifest = _ensure_manifest_compiled(
        manifest=manifest,
        manifest_file=manifest_file,
        explicit_manifest_path=manifest_path,
        no_compile=no_compile,
        json_output=json_output,
    )
    advisor = PartitionAdvisor(manifest, catalog)
    result = advisor.recommend(model)

    if json_output:
        payload = {
            "target_model": result.target_model,
            "direct_downstream_count": result.direct_downstream_count,
            "analysed_downstream_count": result.analysed_downstream_count,
            "incremental_count": result.incremental_count,
            "non_incremental_count": result.non_incremental_count,
            "current_partition_by": result.current_partition_by,
            "matches_current": result.matches_current,
            "recommendation": asdict(result.recommendation) if result.recommendation else None,
            "alternatives": [asdict(a) for a in result.alternatives],
            "warnings": result.warnings,
        }
        print(json.dumps(payload, indent=2))
        return

    if result.warnings:
        for w in result.warnings:
            console.print(f"[yellow]⚠ {w}[/yellow]")
        if not result.recommendation:
            raise typer.Exit(code=1)

    console.print(f"[bold green]Partition advisor for {result.target_model}[/bold green]")
    console.print(f"   Manifest: [dim]{manifest_file}[/dim]")
    console.print(
        f"   Direct downstream: {result.direct_downstream_count} models "
        f"({result.incremental_count} incremental, "
        f"{result.non_incremental_count} table/view); "
        f"{result.analysed_downstream_count} with analyzable references"
    )
    if result.current_partition_by:
        console.print(f"   Current partition_by: {', '.join(result.current_partition_by)}")

    rec = result.recommendation
    if rec is None:
        console.print("[yellow]No partition recommendation (no qualifying downstream usage)[/yellow]")
        return

    if result.matches_current:
        console.print(
            f"\n[bold green]✓ Current partitioning by '{rec.column}' is already "
            "optimal — no changes needed.[/bold green]"
        )
    else:
        console.print("\n   [cyan]Recommended:[/cyan]")
        console.print(f"     Column:      {rec.column} ({rec.data_type or '?'})")
        console.print(f"     Granularity: {rec.granularity}")
        console.print(f"     Score:       {rec.score}")
        console.print("     Reasoning:")
        for line in rec.reasoning:
            console.print(f"       · {line}")

    _print_partition_breakdown(rec)

    if not result.matches_current and result.alternatives:
        console.print("\n   [dim]Alternatives:[/dim]")
        for alt in result.alternatives:
            console.print(f"     · {alt.column} ({alt.data_type or '?'}) score={alt.score} pruning=~{alt.pruning_impact_pct}%")


def _print_partition_breakdown(rec: Any) -> None:
    """Render the materialization-aware downstream breakdown.

    Three buckets matter to the reader:
      1. Incremental WITH pruning — correctly implemented.
      2. Incremental WITHOUT pruning — the bug class. Each one means
         the incremental run scans the whole upstream every time.
      3. Non-incremental — full scan is expected (table/view refresh).
    """
    inc_with = rec.incremental_with_pruning
    inc_without = rec.incremental_without_pruning
    non_inc_with = rec.non_incremental_with_pruning
    non_inc_scan = rec.non_incremental_full_scan

    if inc_with:
        console.print(
            f"\n   [green]✓ {len(inc_with)} incremental downstream use "
            f"partition pruning on '{rec.column}':[/green]"
        )
        for m in inc_with[:10]:
            console.print(f"     · {m}")
        if len(inc_with) > 10:
            console.print(f"     [dim](+{len(inc_with) - 10} more)[/dim]")

    if inc_without:
        console.print(
            f"\n   [bold red]❗ {len(inc_without)} incremental downstream "
            f"read this table WITHOUT pruning on '{rec.column}' — every "
            f"incremental run scans the whole upstream:[/bold red]"
        )
        for m in inc_without:
            console.print(f"     · [red]{m}[/red]")
        console.print(
            "     [dim]Fix: add a WHERE clause filtering "
            f"'{rec.column}' (or a derived range) in each model's SQL.[/dim]"
        )

    if non_inc_with:
        console.print(
            f"\n   [dim]{len(non_inc_with)} table/view downstream still "
            "filter on this column (good practice, helps cache hits):[/dim]"
        )
        for m in non_inc_with[:5]:
            console.print(f"     · [dim]{m}[/dim]")
        if len(non_inc_with) > 5:
            console.print(f"     [dim](+{len(non_inc_with) - 5} more)[/dim]")

    if non_inc_scan:
        console.print(
            f"\n   [dim]{len(non_inc_scan)} table/view downstream "
            "scan the whole table (expected for full-refresh "
            "materializations):[/dim]"
        )
        for m in non_inc_scan[:5]:
            console.print(f"     · [dim]{m}[/dim]")
        if len(non_inc_scan) > 5:
            console.print(f"     [dim](+{len(non_inc_scan) - 5} more)[/dim]")


@optimize_app.command("refresh")
def optimize_refresh(
    models: list[str] = typer.Argument(None, help="Changed model short names (omit with -m to auto-detect from git)"),
    use_modified: bool = typer.Option(False, "-m", "--modified", help="Auto-detect changed models from git (committed-vs-base + uncommitted + untracked)"),
    base_branch: Optional[str] = typer.Option(None, "--base", help="Base branch for git diff (default: auto-detect origin/main → origin/master → main → master)"),
    cols: list[str] = typer.Option(None, "--cols", help="Limit changes to specific columns: --cols MODEL:c1,c2 (repeatable). Without it, the whole model is treated as affected and chain propagation is conservative."),
    json_output: bool = typer.Option(False, "-j", "--json", help="JSON output"),
    manifest_path: Optional[str] = typer.Option(None, "--manifest", help="Explicit dev manifest.json path (overrides default ./target/manifest.json)"),
    catalog_path: Optional[str] = typer.Option(None, "--catalog", help="Explicit catalog.json path"),
    no_compile: bool = typer.Option(False, "--no-compile", help="Skip auto `dbt parse` / `dbt compile` when manifest is stale or compiled SQL is missing"),
) -> None:
    """Plan minimal --full-refresh / incremental / skip set for changed models.

    Always uses the **dev** manifest. This command exists to plan the impact
    of *branch-local* changes, which exist only in dev — the production
    manifest is irrelevant here and is intentionally not supported.

    When ``-m`` is used and ``--no-compile`` is not set, the command runs
    ``dbt compile --select <changed>+`` in the project root once before
    planning. A single compile regenerates the manifest (picking up new
    branch-only models) AND populates ``compiled_code`` for every changed
    model plus its downstream — exactly what SQLGlot needs to attribute
    column usage. ``dbt parse`` alone would not be enough: it doesn't
    render Jinja, so ``compiled_code`` would stay empty and SQLGlot would
    have no SQL to parse.

    Examples:
        meta optimize refresh -m                          # auto from git
        meta optimize refresh model_a model_b             # explicit list
        meta optimize refresh -mj | jq '.summary'         # JSON output
        meta optimize refresh -m --no-compile             # use whatever manifest is on disk as-is
    """
    from dbt_meta.usage import RefreshAdvisor, changed_models_from_git
    from dbt_meta.usage.advisor_refresh import (
        _find_dbt_executable,
        _infer_project_root,
        _RefreshDecision,
    )

    manifest, catalog, manifest_file = _load_manifest_and_catalog(True, manifest_path, catalog_path)

    # Will be populated below from git output (when -m). Used twice: once
    # to drive the bulk `dbt compile`, then to map paths back to unique_ids.
    modified_files: set[str] = set()

    changes: dict[str, Optional[set[str]]] = {}
    used_base: Optional[str] = None
    file_sources: dict[str, str] = {}
    if use_modified:
        import subprocess

        def _git(cmd: list[str]) -> tuple[int, str]:
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                return r.returncode, r.stdout
            except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                return 1, ""

        # 1) Committed changes vs base branch
        bases_to_try = (
            (base_branch,) if base_branch
            else ("origin/main", "origin/master", "main", "master")
        )
        for base in bases_to_try:
            rc, out = _git(["git", "diff", f"{base}...HEAD", "--name-only"])
            if rc == 0:
                used_base = base
                for f in out.splitlines():
                    if f:
                        file_sources.setdefault(f, "committed")
                break
        if base_branch and used_base is None:
            Console(stderr=True).print(
                f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] explicit --base '{base_branch}' not found"
            )
            raise typer.Exit(code=1)

        # 2) Uncommitted (unstaged + staged) and untracked
        rc, out = _git(["git", "diff", "HEAD", "--name-only"])
        if rc == 0:
            for f in out.splitlines():
                if f:
                    file_sources[f] = "uncommitted"  # overrides 'committed'
        rc, out = _git(["git", "diff", "--cached", "--name-only"])
        if rc == 0:
            for f in out.splitlines():
                if f:
                    file_sources[f] = "uncommitted"
        # --untracked-files=all so git lists each new .sql file inside
        # untracked directories instead of collapsing the dir to a single
        # entry (e.g. ``?? models/intermediate/client/``). Without this
        # flag, models added in brand-new directories never match any
        # node's ``original_file_path`` and silently fall out of the plan.
        rc, out = _git(["git", "status", "--porcelain", "--untracked-files=all"])
        if rc == 0:
            for line in out.splitlines():
                if line.startswith("??"):
                    f = line[3:].strip()
                    if f:
                        file_sources[f] = "untracked"

        modified_files = set(file_sources.keys())
        if not modified_files and not models:
            Console(stderr=True).print(
                "[yellow]No git changes detected.[/yellow] "
                f"Base used: {used_base or '(none)'}. Try passing model names explicitly."
            )

        # Run `dbt compile --select <changed>+` ONCE before classification:
        # this single call regenerates the manifest (catching new branch-only
        # models) AND populates compiled_code for every changed model plus
        # its full downstream chain — exactly what SQLGlot needs. We derive
        # the model short name from the file basename (dbt's convention).
        if modified_files and not no_compile:
            import subprocess as _subprocess
            from pathlib import Path as _Path

            project_root = _infer_project_root(manifest_file)
            selectors = sorted({
                f"{_Path(f).stem}+"
                for f in modified_files
                if f.endswith(".sql") and "models/" in f and _Path(f).stem
            })
            dbt_cmd = _find_dbt_executable(project_root) if project_root else None
            if project_root and dbt_cmd and selectors:
                Console(stderr=True).print(
                    f"[dim]ℹ️  Running `dbt compile --select {' '.join(selectors)}` "
                    f"in {project_root} to refresh manifest + compiled SQL of the impacted chain "
                    f"(may take 1-5 min on first run)…[/dim]"
                )
                try:
                    compile_result = _subprocess.run(
                        [dbt_cmd, "compile", "--select", *selectors],
                        cwd=project_root,
                        capture_output=True,
                        text=True,
                        timeout=600,
                    )
                except (_subprocess.TimeoutExpired, OSError) as exc:
                    Console(stderr=True).print(
                        f"[yellow]⚠️  `dbt compile` did not complete ({exc}); using the on-disk manifest as-is.[/yellow]"
                    )
                    compile_result = None
                if compile_result is not None and compile_result.returncode == 0:
                    manifest, catalog, manifest_file = _load_manifest_and_catalog(True, manifest_path, catalog_path)
                elif compile_result is not None:
                    # Show the LAST chunk of stderr/stdout — dbt-fusion prints
                    # an "Execution Summary" footer and the actual error lines
                    # right above it, so the meaningful content is near the
                    # tail of the output.
                    err_text = (compile_result.stderr or compile_result.stdout or "").strip()
                    err_tail = "\n      ".join(err_text.splitlines()[-12:]) if err_text else "(no output captured)"
                    Console(stderr=True).print(
                        f"[yellow]⚠️  `dbt compile` failed (exit {compile_result.returncode}). "
                        f"Last lines of output:\n      {err_tail}\n"
                        f"    Falling back to the on-disk manifest — models without compiled_code "
                        f"will land in `full_refresh` conservatively.\n"
                        f"    Fix dbt project errors above and retry, or pass --no-compile to "
                        f"silence the auto-compile attempt.[/yellow]"
                    )

        for short in changed_models_from_git(modified_files, manifest):
            changes[short] = None
    if models:
        for m in models:
            changes[m] = None  # Whole-model change unless caller has a column-level diff

    # --cols MODEL:c1,c2 narrows specific models from whole-model to column-level
    for spec in (cols or []):
        if ":" not in spec:
            Console(stderr=True).print(
                f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] --cols expects 'MODEL:col1,col2', got: {spec!r}"
            )
            raise typer.Exit(code=1)
        model_part, _, col_part = spec.partition(":")
        col_set = {c.strip().lower() for c in col_part.split(",") if c.strip()}
        if not col_set:
            continue
        # Add the model to changes if not already there
        if model_part not in changes:
            changes[model_part] = set()
        existing = changes[model_part]
        if existing is None:
            changes[model_part] = col_set
        else:
            changes[model_part] = existing | col_set

    if not changes:
        msg = "No changed models specified. Use -m to auto-detect from git or pass model names."
        if json_output:
            print(json.dumps({"error": msg}))
        else:
            Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {msg}")
        raise typer.Exit(code=1)

    # Reject runs where none of the requested models exist in the active
    # manifest — otherwise the plan exits 0 with an empty summary plus a
    # buried warning, which scripts and humans both read as "nothing to do
    # / success". A user passing a typo or pointing at the wrong manifest
    # needs a hard failure, not a silent no-op.
    manifest_short_names = {
        uid.split(".")[-1]
        for uid in manifest.get("nodes", {})
        if uid.startswith("model.")
    }
    missing = [m for m in changes if m not in manifest_short_names]
    if missing and len(missing) == len(changes):
        msg = (
            f"None of the specified models exist in the active manifest: "
            f"{', '.join(sorted(missing))}. "
            f"The dev manifest is at {manifest_file}; "
            "run `dbt parse` (or `dbt compile`) in your project to refresh it."
        )
        if json_output:
            print(json.dumps({"error": msg}))
        else:
            Console(stderr=True).print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {msg}")
        raise typer.Exit(code=1)

    advisor = RefreshAdvisor(
        manifest, catalog,
        manifest_path=manifest_file,
        auto_compile=not no_compile,
    )
    plan = advisor.plan(changes)

    # Map each changed-model short name → source (committed / uncommitted /
    # untracked / explicit) for display.
    changes_source: dict[str, str] = {}
    if use_modified and file_sources:
        for unique_id, node in manifest.get("nodes", {}).items():
            if not unique_id.startswith("model."):
                continue
            short = unique_id.split(".")[-1]
            if short not in changes:
                continue
            path = node.get("original_file_path", "")
            if path in file_sources:
                changes_source[short] = file_sources[path]
    for m in changes:
        changes_source.setdefault(m, "explicit")

    if json_output:
        payload = plan.to_dict()
        payload["git"] = {
            "base_branch": used_base,
            "changed_models_source": changes_source,
        }
        full_list = [d["model"] for d in payload["needs_full_refresh"]]
        inc_list = [d["model"] for d in payload["needs_incremental"]]
        payload["dbt_commands"] = {
            "full_refresh": f"dbt run -fs {' '.join(full_list)}" if full_list else "",
            "incremental": f"dbt run -s {' '.join(inc_list)}" if inc_list else "",
        }
        print(json.dumps(payload, indent=2))
        return

    s = plan.to_dict()["summary"]
    console.print("[bold green]Refresh plan[/bold green]")
    if use_modified:
        console.print(f"   Git base: [cyan]{used_base or '(unknown)'}[/cyan]")
    formatted_changes = ", ".join(
        f"{m} [dim]({changes_source.get(m, '?')})[/dim]" for m in sorted(changes)
    )
    console.print(f"   Changed models: {len(changes)} — {formatted_changes}")
    console.print(f"   Summary: full_refresh={s['full_refresh']}  incremental={s['incremental']}  skip={s['skip']}")

    def _print_bucket(
        label: str,
        color: str,
        bucket: list["_RefreshDecision"],
        limit: int = 30,
    ) -> None:
        if not bucket:
            return
        console.print(f"\n   [{color}]{label} ({len(bucket)}):[/{color}]")
        for d in bucket[:limit]:
            reason = d.reasons[0] if d.reasons else ""
            console.print(f"     · {d.model}  [dim]{reason[:80]}[/dim]")
        if len(bucket) > limit:
            console.print(f"     [dim](+{len(bucket) - limit} more — use -j for full list)[/dim]")

    _print_bucket("FULL REFRESH", "red", plan.needs_full_refresh)
    _print_bucket("INCREMENTAL", "yellow", plan.needs_incremental)
    # Skip bucket is usually the longest and least interesting — cap at 5
    _print_bucket("SKIP", "dim", plan.can_skip, limit=5)

    # Ready-to-paste dbt commands. Print via plain ``print`` (not
    # ``console.print``) so Rich does not soft-wrap the command — pasted
    # multi-line text would otherwise be split into separate shell commands.
    full_models = [d.model for d in plan.needs_full_refresh]
    inc_models = [d.model for d in plan.needs_incremental]
    if full_models or inc_models:
        console.print()
        console.print("[bold cyan]Suggested commands:[/bold cyan]")
    if full_models:
        print(f"  dbt run -fs {' '.join(full_models)}")
    if inc_models:
        print(f"  dbt run -s {' '.join(inc_models)}")

    if plan.warnings:
        console.print()
        for w in plan.warnings:
            console.print(f"[yellow]⚠ {w}[/yellow]")


if __name__ == "__main__":
    app()
