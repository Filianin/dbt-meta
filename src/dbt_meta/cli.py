"""
CLI - Modern command-line interface using Typer

Provides dbt-meta CLI with:
- Type-hint based argument parsing
- Rich formatted output
- JSON output mode
- Auto-discovery of manifest.json
"""

import json
from typing import Any, Callable, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from dbt_meta import commands
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

# Rich console for formatted output
console = Console()

# Rich styles - reusable constants
STYLE_COMMAND = "cyan"
STYLE_DESCRIPTION = "white"
STYLE_HEADER = "bold green"
STYLE_ERROR = "red"
STYLE_DIM = "dim"
STYLE_GREEN = "green"


def handle_error(error: DbtMetaError, json_output: bool = False) -> None:
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


def _not_found_error(model_name: str, json_output: bool) -> None:
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
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=20)
    table.add_column(style=STYLE_DESCRIPTION)

    # Core commands (green)
    table.add_row("[bold green]Core:[/bold green]", "")
    table.add_row("  [green]info[/green]", "Model summary (name, schema, table, materialization, tags)")
    table.add_row("  [green]schema[/green]", "BigQuery table name (--dev for dev schema)")
    table.add_row("  [green]path[/green]", "Relative file path to .sql file")
    table.add_row("  [green]columns[/green]", "Column names and types (--dev for dev schema)")
    table.add_row("  [green]sql[/green]", "Compiled SQL (default) or raw SQL with --jinja")
    table.add_row("  [green]docs[/green]", "Column names, types, and descriptions")
    table.add_row("  [green]deps[/green]", "Dependencies by type (refs, sources, macros)")
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
    table.add_row("  [yellow]hotspots[/yellow]", "Find optimization opportunities (query cost, partitioning)")
    table.add_row("  [yellow]analyze[/yellow]", "Deep analysis of single model")
    table.add_row("  [yellow]branch[/yellow]", "Branch-level optimization impact")
    table.add_row("", "")

    # Integration (blue)
    table.add_row("[bold blue]Integration:[/bold blue]", "")
    table.add_row("  [blue]powerbi[/blue]", "Extract Power BI table mappings from workspace")
    table.add_row("", "")

    # Settings management (magenta)
    table.add_row("[bold magenta]Settings:[/bold magenta]", "")
    table.add_row("  [magenta]settings init[/magenta]", "Create config file from template")
    table.add_row("  [magenta]settings show[/magenta]", "Display current configuration")
    table.add_row("  [magenta]settings validate[/magenta]", "Validate config file")
    table.add_row("  [magenta]settings path[/magenta]", "Show path to active config file")

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
    table.add_row("[bold cyan]Specific flags:[/bold cyan]", "")
    table.add_row("-a, --all", "Recursive mode (parents/children commands)")
    table.add_row("--jinja", "Show raw SQL with Jinja (sql command)")
    table.add_row("--and", "AND logic for selectors (list command)")
    table.add_row("--group", "Group by tag combinations (list command)")
    table.add_row("-m, --modified", "Show git-modified models (list command)")
    table.add_row("-f, --full-refresh", "Show models for --full-refresh (list command)")

    return Panel(table, title="[bold white]🚩 Flags[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def _build_examples_panel() -> Panel:
    """Build Examples panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=45)
    table.add_column(style=STYLE_DESCRIPTION)

    table.add_row("[bold]Basic Usage:[/bold]", "")
    table.add_row("  meta schema customers", "my_project.analytics.customers")
    table.add_row("  meta path customers", "models/analytics/customers.sql")
    table.add_row("  meta columns -j orders", "Get columns as JSON")
    table.add_row("  meta sql customers", "View compiled SQL")
    table.add_row('  meta search "customer"', "Search by name/description")
    table.add_row("", "")
    table.add_row("[bold]Dev Workflow (with defer):[/bold]", "")
    table.add_row("  defer run --select customers", "Build dev table first")
    table.add_row("  meta schema --dev customers", "personal_USERNAME.customers")
    table.add_row("  meta columns --dev -j customers", "Get dev table columns")
    table.add_row("", "")
    table.add_row("[bold]Model filtering (list):[/bold]", "")
    table.add_row("  meta list tag:daily", "Models with daily tag")
    table.add_row("  meta list path:models/core/ tag:daily --and", "Core models with daily tag")
    table.add_row("  meta list -m", "Git-modified models")
    table.add_row("", "")
    table.add_row("[bold]Combined flags:[/bold]", "")
    table.add_row("  meta schema -dj customers", "Dev + JSON output")
    table.add_row("", "")
    table.add_row("[bold]Configuration:[/bold]", "")
    table.add_row("  meta settings init", "Create config file")
    table.add_row("  meta settings show -j", "View current settings as JSON")
    table.add_row("  meta -m ~/custom.json list", "Use custom manifest")

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
    table.add_row("  [cyan]DBT_DEV_SCHEMA[/cyan]          → Dev schema override")
    table.add_row("  [cyan]DBT_FALLBACK_TARGET[/cyan]     → Enable dev manifest fallback")
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

    # Print all sections (Commands first, then Flags)
    console.print(_build_commands_panel())
    console.print(_build_flags_panel())
    console.print(_build_examples_panel())
    console.print(_build_configuration_panel())

    # Footer with links
    console.print()
    console.print("─" * 80)
    console.print("📚 Docs:   https://github.com/Filianin/dbt-meta")
    console.print("🐛 Issues: https://github.com/Filianin/dbt-meta/issues")
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
def info(
    model_name: str = typer.Argument(..., help="Model name (e.g., core_client__events)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Model summary (name, schema, table, materialization, tags)

    Examples:
        meta info -j customers               # Production
        meta info --dev -j customers         # Dev (personal_USERNAME)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = commands.info(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

        if not result:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()
            table = Table(title=f"[bold green not italic]Model: {result['name']}[/bold green not italic]", show_header=False)
            table.add_column("Field", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Value", style="white")

            table.add_row("Database:", result['database'])
            table.add_row("Schema:", result['schema'])
            table.add_row("Table:", result['table'])
            table.add_row("Full Name:", result['full_name'])
            table.add_row("Materialized:", result['materialized'])
            table.add_row("File:", result['file'])
            table.add_row("Tags:", ', '.join(result['tags']) if result['tags'] else '(none)')

            console.print(table)

    except DbtMetaError as e:
        handle_error(e, json_output)


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
        result = commands.schema(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

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
        result = commands.columns(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

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
        result = commands.config(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

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
def deps(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Dependencies by type (refs, sources, macros)

    Examples:
        meta deps -j model_name              # Production
        meta deps --dev -j model_name        # Dev (personal_USERNAME)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = commands.deps(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()

            # Refs table
            if result['refs']:
                table_refs = Table(title=f"[bold green not italic]Refs ({len(result['refs'])})[/bold green not italic]", header_style="bold green")
                table_refs.add_column("Ref", style=STYLE_COMMAND)
                for ref in result['refs']:
                    table_refs.add_row(ref)
                console.print(table_refs)
                print()

            # Sources table
            if result['sources']:
                table_sources = Table(title=f"[bold green not italic]Sources ({len(result['sources'])})[/bold green not italic]", header_style="bold green")
                table_sources.add_column("Source", style=STYLE_COMMAND)
                for source in result['sources']:
                    table_sources.add_row(source)
                console.print(table_sources)
                print()

            # Macros table
            if result.get('macros'):
                table_macros = Table(title=f"[bold green not italic]Macros ({len(result.get('macros', []))})[/bold green not italic]", header_style="bold green")
                table_macros.add_column("Macro", style=STYLE_COMMAND)
                for macro in result.get('macros', []):
                    table_macros.add_row(macro)
                console.print(table_macros)

    except DbtMetaError as e:
        handle_error(e, json_output)


@app.command()
def sql(
    model_name: str = typer.Argument(..., help="Model name"),
    jinja: bool = typer.Option(False, "--jinja", help="Show raw SQL with Jinja"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
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
        result = commands.sql(manifest_path, model_name, use_dev=effective_use_dev, raw=jinja, json_output=json_output)

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
) -> None:
    """
    Validate SQL syntax using BigQuery dry run

    Examples:
        meta validate model_name          # Validate production SQL
        meta validate --dev model_name    # Validate dev SQL
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = commands.validate(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

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
) -> None:
    """
    Estimate query scan size using BigQuery dry run

    Examples:
        meta scan model_name              # Show scan size for production SQL
        meta scan --dev model_name        # Show scan size for dev SQL
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = commands.scan(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

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
        result = commands.path(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

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
        result = commands.list_models(manifest_path, pattern)

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
        result = commands.search(manifest_path, query)

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
    full_refresh: bool = typer.Option(False, "-f", "--full-refresh", help="Show models requiring --full-refresh"),
    and_logic: bool = typer.Option(False, "--and", help="Require ALL tags (default: OR - at least one)"),
    group: bool = typer.Option(False, "--group", help="Group by tag combinations"),
    all_tree: bool = typer.Option(False, "-a", "--all", help="Tree view (--full-refresh only): show lineage from modified to downstream"),
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
      -f, --full-refresh     - Show models needing --full-refresh
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
      meta list -f                                # Models for --full-refresh
      meta list tag:verified -j                   # JSON output

    \b
    OUTPUT FORMATS:
      Default    - Space-separated model names (for copy-paste)
      --group    - Grouped by tag combinations with headers
      --json     - Structured metadata [{"model": "...", "tags": [...]}]
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        selector_list = list(selectors) if selectors else None

        # Validate --all flag usage
        if all_tree and not full_refresh:
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] --all flag only works with --full-refresh")
            raise typer.Exit(code=1)

        result = commands.ls(
            manifest_path,
            selectors=selector_list,
            modified=modified,
            refresh=full_refresh,
            and_logic=and_logic,
            group=group,
            tree_view=all_tree,
            use_dev=effective_use_dev,
            json_output=json_output
        )

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
        result = commands.parents(manifest_path, model_name, use_dev=effective_use_dev, recursive=all_ancestors, json_output=json_output)

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
        result = commands.children(manifest_path, model_name, use_dev=effective_use_dev, recursive=all_descendants, json_output=json_output)

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
        commands.refresh(use_dev=dev)
        console.print("[green]✅ Artifacts refreshed successfully[/green]")
    except DbtMetaError as e:
        handle_error(e)
    except Exception as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Failed to refresh artifacts: {e!s}")
        raise typer.Exit(code=1) from None


@app.command()
def docs(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
) -> None:
    """
    Column names, types, and descriptions

    Examples:
        meta docs customers              # Production
        meta docs --dev customers        # Dev (personal_USERNAME)
    """
    try:
        manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
        result = commands.docs(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

        if not result:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Rich table output with blank line first
            print()
            table = Table(title=f"[bold green not italic]Column Documentation: {model_name}[/bold green not italic]", header_style="bold green")
            table.add_column("Name", style=STYLE_COMMAND, no_wrap=True)
            table.add_column("Type", style="white")
            table.add_column("Description", style=STYLE_DESCRIPTION)

            for col in result:
                desc = col.get('description', '') or "(no description)"
                table.add_row(col['name'], col['data_type'], desc)

            console.print(table)

    except DbtMetaError as e:
        handle_error(e, json_output)


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
        result = commands.analyze(manifest_path, model_name, use_dev=False, json_output=json_output)

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            _print_analyze_result(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_analyze_result(result: dict) -> None:
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
        result = commands.hotspots(manifest_path, limit=limit, min_gb=min_gb, json_output=json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            _print_hotspots_result(result, limit)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_hotspots_result(result: dict, limit: int) -> None:
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
        result = commands.branch(manifest_path, model_name, use_dev=False, json_output=json_output)

        if result is None:
            _not_found_error(model_name, json_output)

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            _print_branch_result(result)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_branch_result(result: dict) -> None:
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


@app.command()
def powerbi(
    workspace_id: Optional[str] = typer.Argument(None, help="Power BI workspace ID"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    measures: bool = typer.Option(False, "--measures", help="Include measures with DAX expressions"),
    columns: bool = typer.Option(False, "--columns", help="Include column schemas"),
    full: bool = typer.Option(False, "--full", help="Include all metadata (measures + columns)"),
    by_table: bool = typer.Option(False, "--by-table", help="Group by tables instead of datasets"),
) -> None:
    """
    Extract BigQuery tables used by Power BI dashboards

    Maps Power BI datasets to BigQuery tables and dbt models.
    Optionally includes measures (DAX) and column schemas.
    Requires Power BI Admin API configured in settings.

    Examples:
        meta powerbi                          # Tables only
        meta powerbi --measures               # + Measures with DAX
        meta powerbi --columns                # + Column schemas
        meta powerbi --full                   # All metadata
        meta powerbi -j                       # JSON (always full)
        meta powerbi --by-table               # Group by tables
    """
    try:
        manifest_path, _ = get_manifest_path(manifest, False)
        result = commands.powerbi(
            manifest_path,
            workspace_id=workspace_id,
            json_output=json_output,
            show_measures=measures,
            show_columns=columns,
            show_full=full,
            by_table=by_table,
        )

        if json_output:
            print(json.dumps(result, indent=2))
        else:
            # Route to appropriate print function
            if result.get('view') == 'by_table':
                _print_powerbi_by_table(result)
            else:
                _print_powerbi_result(result, show_measures=measures or full, show_columns=columns or full)

    except DbtMetaError as e:
        handle_error(e, json_output)


def _print_powerbi_result(result: dict, show_measures: bool = False, show_columns: bool = False) -> None:
    """Pretty print powerbi command result with optional extended metadata.

    Args:
        result: Power BI scan result dictionary
        show_measures: Include measures with DAX expressions
        show_columns: Include column schemas
    """
    print()
    workspace = result.get('workspace', 'Unknown')
    summary = result.get('summary', {})

    console.print(f"[bold green]{workspace}[/bold green] ({summary.get('total_datasets', 0)} datasets, "
                  f"{summary.get('total_reports', 0)} reports, {summary.get('total_tables', 0)} BigQuery tables)")
    print()

    datasets = result.get('datasets', [])
    for dataset in datasets:
        # Dataset header with mode and refresh schedule
        name = dataset.get('name', '')
        mode = dataset.get('mode', 'Import')
        refresh = dataset.get('refresh_schedule')

        console.print(f"[bold cyan]{name}[/bold cyan]")

        # Configured by
        configured_by = dataset.get('configured_by', '')
        if configured_by:
            console.print(f"   [dim]Owner:[/dim] {configured_by}")

        # Mode and refresh schedule
        if mode == 'DirectQuery':
            console.print(f"   [dim]Mode:[/dim] [magenta]DirectQuery[/magenta] — real-time queries to BigQuery")
        else:
            # Import mode - show type and refresh schedule
            console.print(f"   [dim]Mode:[/dim] Import — cached data, updated by schedule")
            if refresh and refresh.get('enabled'):
                freq = refresh.get('frequency', '')
                times = refresh.get('times', [])
                times_str = ', '.join(times) if times else ''
                if times_str:
                    console.print(f"   [dim]Refresh:[/dim] [green]{freq}[/green] at {times_str}")
                else:
                    console.print(f"   [dim]Refresh:[/dim] [green]{freq}[/green]")
            elif refresh and not refresh.get('enabled'):
                console.print(f"   [dim]Refresh:[/dim] [yellow]disabled[/yellow]")
            else:
                console.print(f"   [dim]Refresh:[/dim] [yellow]no schedule[/yellow]")

        # Reports using this dataset (group similar reports)
        reports = dataset.get('reports', [])
        if reports:
            # Find pairs: "Name" and "[App] Name"
            app_reports = {r[6:] for r in reports if r.startswith('[App] ')}
            shown = set()
            report_lines = []

            for report in reports:
                if report in shown:
                    continue

                base_name = report[6:] if report.startswith('[App] ') else report
                has_app = base_name in app_reports
                has_regular = base_name in reports

                if has_app and has_regular and not report.startswith('[App] '):
                    # Both versions exist - show combined
                    report_lines.append(f"{base_name} [dim](+ App)[/dim]")
                    shown.add(base_name)
                    shown.add(f'[App] {base_name}')
                elif not report.startswith('[App] ') or base_name not in reports:
                    # Only one version
                    report_lines.append(report)
                    shown.add(report)

            for line in report_lines:
                console.print(f"   [dim]Report:[/dim] {line}")

        # Tables
        tables = dataset.get('tables', [])
        if tables:
            console.print(f"   [dim]Tables:[/dim]")
            for table in tables:
                bq_table = table.get('bigquery_table', '')
                dbt_model = table.get('dbt_model')
                in_manifest = table.get('in_manifest', False)

                if in_manifest:
                    console.print(f"   [green]   {bq_table}[/green] -> {dbt_model}")
                else:
                    console.print(f"   [yellow]   {bq_table}[/yellow] [dim](not in manifest)[/dim]")

                # Show measures if requested
                if show_measures and 'measures' in table:
                    measures = table['measures']
                    console.print(f"      [dim]Measures ({len(measures)}):[/dim]")
                    for measure in measures[:3]:  # Show first 3
                        name = measure['name']
                        expr = measure['expression'][:60] + '...' if len(measure['expression']) > 60 else measure['expression']
                        console.print(f"      [cyan]•[/cyan] {name}: {expr}")
                    if len(measures) > 3:
                        console.print(f"      [dim]... and {len(measures) - 3} more[/dim]")

                # Show columns if requested
                if show_columns and 'columns' in table:
                    columns = table['columns']
                    console.print(f"      [dim]Columns ({len(columns)}):[/dim]")
                    for col in columns[:5]:  # Show first 5
                        name = col['name']
                        dtype = col['data_type']
                        hidden = '[dim](hidden)[/dim]' if col.get('is_hidden') else ''
                        console.print(f"      [cyan]•[/cyan] {name} ({dtype}) {hidden}")
                    if len(columns) > 5:
                        console.print(f"      [dim]... and {len(columns) - 5} more[/dim]")

        print()

    # Summary
    tables_in = summary.get('tables_in_manifest', 0)
    tables_total = summary.get('total_tables', 0)
    pct = (tables_in / tables_total * 100) if tables_total > 0 else 0

    console.print(f"[dim]Summary: {tables_in}/{tables_total} tables in dbt manifest ({pct:.0f}%)[/dim]")


def _print_powerbi_by_table(result: dict) -> None:
    """Pretty print powerbi result in table-centric view.

    Args:
        result: Power BI scan result with tables aggregation
    """
    from rich.table import Table

    print()
    workspace = result.get('workspace', 'Unknown')
    summary = result.get('summary', {})

    # Workspace header
    console.print(
        f"[bold green]{workspace}[/bold green] "
        f"({summary.get('total_tables', 0)} BigQuery tables, "
        f"{summary.get('total_reports', 0)} reports, "
        f"{summary.get('total_datasets', 0)} datasets)"
    )
    print()

    # Create ASCII table
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("BigQuery Table", style="white", no_wrap=False, width=45)
    table.add_column("Reports", justify="right", style="cyan", width=8)
    table.add_column("Datasets", justify="right", style="cyan", width=9)
    table.add_column("dbt Model", style="green", no_wrap=False, width=40)

    # Add rows
    tables = result.get('tables', [])
    for table_info in tables:
        bq_table = table_info['bigquery_table']
        report_count = table_info['report_count']
        dataset_count = table_info['dataset_count']
        dbt_model = table_info['dbt_model'] or '[dim](not in manifest)[/dim]'

        # Color code based on manifest status
        if table_info['in_manifest']:
            bq_table_styled = f"[green]{bq_table}[/green]"
        else:
            bq_table_styled = f"[yellow]{bq_table}[/yellow]"

        table.add_row(
            bq_table_styled,
            str(report_count),
            str(dataset_count),
            dbt_model
        )

    console.print(table)
    print()

    # Summary
    tables_in = summary.get('tables_in_manifest', 0)
    tables_total = summary.get('total_tables', 0)
    pct = (tables_in / tables_total * 100) if tables_total > 0 else 0

    console.print(f"[dim]Summary: {tables_in}/{tables_total} tables in dbt manifest ({pct:.0f}%)[/dim]")


if __name__ == "__main__":
    app()
