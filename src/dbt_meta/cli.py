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


def handle_error(error: DbtMetaError) -> None:
    """Display formatted error message with suggestion and exit.

    Args:
        error: DbtMetaError instance with message and optional suggestion

    This function formats errors with Rich console for better readability:
    - Error message in red
    - Suggestion in yellow (if available)
    - Exits with code 1
    """
    error_console = Console(stderr=True)

    # Display error message
    error_console.print(f"\n[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {error.message}")

    # Display suggestion if available
    if error.suggestion:
        error_console.print(f"[yellow]Suggestion:[/yellow] {error.suggestion}")

    error_console.print()  # Empty line for better readability
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
    table.add_row("  [cyan]list[/cyan]", "List models (optionally filter by pattern)")
    table.add_row("  [cyan]search[/cyan]", "Search by name or description")
    table.add_row("  [cyan]refresh[/cyan]", "Sync prod artifacts (or parse local with --dev)")
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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

        if json_output:
            output = {
                "model_name": model_name,
                "full_name": result['full_name']
            }
            print(json.dumps(output, indent=2))
        else:
            console.print()
            console.print("[bold green]Table:[/]")
            print(result['full_name'])

    except DbtMetaError as e:
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
            console.print()
            sql_type = "Raw SQL:" if jinja else "Compiled SQL:"
            console.print(f"[bold green]{sql_type}[/]")
            print(result)

    except DbtMetaError as e:
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

        if json_output:
            output = {
                "model_name": model_name,
                "path": result
            }
            print(json.dumps(output, indent=2))
        else:
            console.print()
            console.print("[bold green]File path:[/]")
            print(result)

    except DbtMetaError as e:
        handle_error(e)


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
        handle_error(e)


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
        handle_error(e)


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
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
                # Flat table output
                mode = "All parents" if all_ancestors else "Direct parents"
                table = Table(title=f"[bold green not italic]{mode} for {model_name} ({len(result)})[/bold green not italic]", header_style="bold green")
                table.add_column("Path", style=STYLE_COMMAND)
                table.add_column("Table", style="white", min_width=30)
                table.add_column("Type", style="white", min_width=8)

                for parent in result:
                    table.add_row(parent['path'], parent['table'], parent.get('type', ''))

                console.print(table)

    except DbtMetaError as e:
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
                # Flat table output
                mode = "All children" if all_descendants else "Direct children"
                table = Table(title=f"[bold green not italic]{mode} for {model_name} ({len(result)})[/bold green not italic]", header_style="bold green")
                table.add_column("Path", style=STYLE_COMMAND)
                table.add_column("Table", style="white", min_width=30)
                table.add_column("Type", style="white", min_width=8)

                for child in result:
                    table.add_row(child['path'], child['table'], child.get('type', ''))

                console.print(table)

    except DbtMetaError as e:
        handle_error(e)


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
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
            raise typer.Exit(code=1)

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
        handle_error(e)


if __name__ == "__main__":
    app()
