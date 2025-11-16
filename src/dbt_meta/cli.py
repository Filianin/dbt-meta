"""
CLI - Modern command-line interface using Typer

Provides dbt-meta CLI with:
- Type-hint based argument parsing
- Rich formatted output
- JSON output mode
- Auto-discovery of manifest.json
"""

import json
import sys
from typing import Optional, Dict, Any, List
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from rich import print as rprint

from dbt_meta.manifest.finder import ManifestFinder
from dbt_meta import commands

# Create Typer app
app = typer.Typer(
    name="dbt-meta",
    help="AI-first CLI for dbt metadata extraction",
    add_completion=True,
    # Note: help is enabled for subcommands, custom help only for main app
)

# Rich console for formatted output
console = Console()

# Rich styles - reusable constants
STYLE_COMMAND = "cyan"
STYLE_DESCRIPTION = "white"
STYLE_HEADER = "bold green"
STYLE_ERROR = "red"
STYLE_DIM = "dim"
STYLE_GREEN = "green"


def _build_tree_recursive(parent_tree: Tree, nodes: List[Dict[str, Any]]) -> None:
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
    table.add_row("  [cyan]refresh[/cyan]", "Refresh manifest (runs dbt parse)")

    return Panel(table, title="[bold white]📊 Commands[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def _build_flags_panel() -> Panel:
    """Build Flags panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=20)
    table.add_column(style=STYLE_DESCRIPTION)

    table.add_row("[bold cyan]Global flags:[/bold cyan]", "")
    table.add_row("-h, --help", "Show this help message")
    table.add_row("-v, --version", "Show version and exit")
    table.add_row("-m, --manifest PATH", "Explicit path to manifest.json")
    table.add_row("-d, --dev", "Use dev manifest and schema")
    table.add_row("", "")
    table.add_row("[bold cyan]Command flags:[/bold cyan]", "")
    table.add_row("[green]-j, --json[/green]", "Output as JSON (AI-friendly structured data)")
    table.add_row("-a, --all", "Recursive mode (parents/children)")
    table.add_row("--jinja", "Show raw SQL with Jinja (sql command only)")

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
    table.add_row("[bold]Combined flags:[/bold]", "")
    table.add_row("  meta schema -dj customers", "Dev + JSON output")
    table.add_row("  meta parents -ajd model", "All + JSON + Dev")
    table.add_row("  meta columns -jm ~/custom.json model", "JSON + Custom manifest")
    table.add_row("", "")
    table.add_row("[bold]Works from anywhere:[/bold]", "")
    table.add_row("  cd /tmp && meta list", "Uses $DBT_PROD_MANIFEST_PATH")
    table.add_row("  meta schema -m ~/custom.json model", "Use custom manifest")

    return Panel(table, title="[bold white]💡 Examples[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def _build_configuration_panel() -> Panel:
    """Build unified Configuration panel (Simple + Production setups)"""
    from rich.console import Group

    # Part 1: Setup instructions (single column)
    setup_table = Table(show_header=False, box=None, padding=(0, 1))
    setup_table.add_column(style="white", no_wrap=False)

    # Simple Setup
    setup_table.add_row("[bold cyan][1] Simple Setup (single project, no defer):[/bold cyan]")
    setup_table.add_row("Just run: [cyan]dbt compile[/cyan]")
    setup_table.add_row("Commands automatically use [cyan]./target/manifest.json[/cyan]")
    setup_table.add_row("[bold cyan]No configuration needed![/bold cyan]")
    setup_table.add_row("")
    setup_table.add_row("Example:")
    setup_table.add_row("  dbt compile")
    setup_table.add_row("  meta schema customers                       → Uses ./target/manifest.json")
    setup_table.add_row("")
    setup_table.add_row("[dim]Note: Without production manifest, falls back to ./target/[/dim]")
    setup_table.add_row("[dim]      For clarity, use --dev flag explicitly[/dim]")
    setup_table.add_row("")

    # Production Setup
    setup_table.add_row("[bold cyan][2] Production Setup (with defer workflow):[/bold cyan]")
    setup_table.add_row("1. Set production manifest path in ~/.zshrc:")
    setup_table.add_row("   export DBT_PROD_MANIFEST_PATH=~/dbt-state/manifest.json")
    setup_table.add_row("")
    setup_table.add_row("2. Auto-update production manifest (hourly cron):")
    setup_table.add_row("   0 * * * * cp <project>/.dbt-state/manifest.json ~/dbt-state/")
    setup_table.add_row("")
    setup_table.add_row("3. Use [cyan]--dev[/cyan] flag for dev models:")
    setup_table.add_row("   defer run --select customers")
    setup_table.add_row("   meta schema --dev customers                → Uses ./target/manifest.json")
    setup_table.add_row("")

    # Part 2: Reference info (two columns)
    ref_table = Table(show_header=False, box=None, padding=(0, 1))
    ref_table.add_column(style="white", no_wrap=False, width=45)
    ref_table.add_column(style="white", no_wrap=False)

    # Manifest Discovery
    ref_table.add_row("[bold cyan]Manifest Discovery (priority order):[/bold cyan]", "")
    ref_table.add_row("Without --dev:", "")
    ref_table.add_row("  1. [cyan]-m/--manifest PATH[/cyan]", "Explicit override")
    ref_table.add_row("  2. [cyan]DBT_PROD_MANIFEST_PATH[/cyan]", "Production")
    ref_table.add_row("  3. [cyan]./target/manifest.json[/cyan]", "Simple mode")
    ref_table.add_row("", "")
    ref_table.add_row("With --dev:", "")
    ref_table.add_row("  1. [cyan]-m/--manifest PATH[/cyan]", "Override")
    ref_table.add_row("  2. [cyan]DBT_DEV_MANIFEST_PATH[/cyan]", "Dev manifest")
    ref_table.add_row("", "")

    # Environment Variables
    ref_table.add_row("[bold cyan]Environment Variables:[/bold cyan]", "")
    ref_table.add_row("", "")
    ref_table.add_row("Manifest:", "")
    ref_table.add_row("  [cyan]DBT_PROD_MANIFEST_PATH[/cyan]", "Production manifest path")
    ref_table.add_row("  [cyan]DBT_DEV_MANIFEST_PATH[/cyan]", "Dev manifest path")
    ref_table.add_row("", "")
    ref_table.add_row("Dev Workflow:", "")
    ref_table.add_row("  Uses schema/table from target/manifest.json", "(dbt-compiled values)")
    ref_table.add_row("  Fallback: DBT_DEV_SCHEMA.{alias|name}", "When not in manifest")
    ref_table.add_row("", "")
    ref_table.add_row("Naming Strategy (advanced):", "")
    ref_table.add_row("  [cyan]DBT_PROD_TABLE_NAME[/cyan]", "alias_or_name (default)")
    ref_table.add_row("  [cyan]DBT_PROD_SCHEMA_SOURCE[/cyan]", "config_or_model (default)")

    # Combine both tables
    group = Group(setup_table, ref_table)
    return Panel(group, title="[bold white]⚙️ Configuration[/bold white]", title_align="left", border_style="white", padding=(0, 1))


def show_help_with_examples(ctx: typer.Context):
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


def version_callback(value: bool):
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
):
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
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] {str(e)}")
        raise typer.Exit(code=1)


def handle_command_output(result, json_output: bool, formatter_func=None):
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
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Model summary (name, schema, table, materialization, tags)

    Examples:
        meta info -j customers               # Production
        meta info --dev -j customers         # Dev (personal_USERNAME)
    """
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


@app.command()
def schema(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Production table name (database.schema.table) or dev with --dev flag

    Examples:
        meta schema jaffle_shop__orders            # Production
        meta schema --dev jaffle_shop__orders      # Dev (personal_USERNAME)
    """
    manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
    result = commands.schema(manifest_path, model_name, use_dev=effective_use_dev, json_output=json_output)

    if not result:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        # Rich table output with blank line first
        print()
        table = Table(title=f"[bold green not italic]Schema: {model_name}[/bold green not italic]", show_header=False)
        table.add_column("Field", style=STYLE_COMMAND, no_wrap=True)
        table.add_column("Value", style="white")

        if 'database' in result and result['database']:
            table.add_row("Database:", result['database'])
        table.add_row("Schema:", result['schema'])
        table.add_row("Table:", result['table'])
        table.add_row("Full:", result['full_name'])

        console.print(table)


@app.command()
def columns(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema"),
):
    """
    Column names and types

    Examples:
        meta columns -j customers                # Production
        meta columns --dev -j customers          # Dev
    """
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


@app.command()
def config(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Full dbt config (29 fields: partition_by, cluster_by, etc.)

    Examples:
        meta config -j model_name              # Production
        meta config --dev -j model_name        # Dev (personal_USERNAME)
    """
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


@app.command()
def deps(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Dependencies by type (refs, sources, macros)

    Examples:
        meta deps -j model_name              # Production
        meta deps --dev -j model_name        # Dev (personal_USERNAME)
    """
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


@app.command()
def sql(
    model_name: str = typer.Argument(..., help="Model name"),
    jinja: bool = typer.Option(False, "--jinja", help="Show raw SQL with Jinja"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Compiled SQL (default) or raw SQL with --jinja

    Examples:
        meta sql model_name                  # Production compiled SQL
        meta sql --dev model_name            # Dev (personal_USERNAME)
        meta sql --jinja model_name          # Raw SQL with Jinja
    """
    manifest_path, effective_use_dev = get_manifest_path(manifest, use_dev)
    result = commands.sql(manifest_path, model_name, use_dev=effective_use_dev, raw=jinja, json_output=json_output)

    if result is None:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if not result:
        if not jinja:
            console.print(f"[{STYLE_ERROR}]Compiled code not found for model '{model_name}'[/{STYLE_ERROR}]")
            console.print("Note: Compiled code is only available in .dbt-state/manifest.json")
            console.print(f"Tip: Use 'meta sql {model_name} --jinja' to get raw SQL with Jinja templates")
            raise typer.Exit(code=1)
        else:
            console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Raw SQL not found for model '{model_name}'")
            raise typer.Exit(code=1)

    if json_output:
        output = {
            "model_name": model_name,
            "sql": result,
            "type": "raw" if jinja else "compiled"
        }
        print(json.dumps(output, indent=2))
    else:
        print(result)


@app.command()
def path(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Relative file path to .sql file

    Examples:
        meta path model_name              # Production
        meta path --dev model_name        # Dev (personal_USERNAME)
    """
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
        print()
        print(result)


@app.command("list")
def list_cmd(
    pattern: Optional[str] = typer.Argument(None, help="Filter pattern"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
):
    """
    List models (optionally filter by pattern)

    Example: meta list jaffle_shop
    """
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


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
):
    """
    Search by name or description

    Example: meta search "customers" --json
    """
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


@app.command()
def parents(
    model_name: str = typer.Argument(..., help="Model name"),
    all_ancestors: bool = typer.Option(False, "-a", "--all", help="Get all ancestors (recursive)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Upstream dependencies (direct or all ancestors)

    Examples:
        meta parents -j model_name                    # Direct parents (old format)
        meta parents -a model_name                    # Tree view
        meta parents -a -j model_name                 # Nested JSON (<=20) or flat array (>20)
    """
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
            tree = Tree(f"[bold green]📊 All ancestors: {model_name}[/bold green]")
            _build_tree_recursive(tree, result)
            console.print(tree)
        else:
            # Flat table output
            mode = "All ancestors" if all_ancestors else "Direct parents"
            table = Table(title=f"[bold green not italic]{mode} for {model_name} ({len(result)})[/bold green not italic]", header_style="bold green")
            table.add_column("Path", style=STYLE_COMMAND)
            table.add_column("Table", style="white", min_width=30)
            table.add_column("Type", style="white", min_width=8)

            for parent in result:
                table.add_row(parent['path'], parent['table'], parent.get('type', ''))

            console.print(table)


@app.command()
def children(
    model_name: str = typer.Argument(..., help="Model name"),
    all_descendants: bool = typer.Option(False, "-a", "--all", help="Get all descendants (recursive)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Downstream dependencies (direct or all descendants)

    Examples:
        meta children -j model_name                 # Direct children (old format)
        meta children -a model_name                 # Tree view
        meta children -a -j model_name              # Nested JSON (<=20) or flat array (>20)
    """
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
            tree = Tree(f"[bold green]📊 All descendants: {model_name}[/bold green]")
            _build_tree_recursive(tree, result)
            console.print(tree)
        else:
            # Flat table output
            mode = "All descendants" if all_descendants else "Direct children"
            table = Table(title=f"[bold green not italic]{mode} for {model_name} ({len(result)})[/bold green not italic]", header_style="bold green")
            table.add_column("Path", style=STYLE_COMMAND)
            table.add_column("Table", style="white", min_width=30)
            table.add_column("Type", style="white", min_width=8)

            for child in result:
                table.add_row(child['path'], child['table'], child.get('type', ''))

            console.print(table)


@app.command()
def refresh():
    """
    Refresh manifest (runs dbt parse)

    Updates manifest.json with latest model definitions
    """
    try:
        commands.refresh()
        console.print("[green]✓ Manifest refreshed successfully[/green]")
    except Exception as e:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Failed to refresh manifest: {str(e)}")
        raise typer.Exit(code=1)


@app.command()
def docs(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", "-m", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Column names, types, and descriptions

    Examples:
        meta docs customers              # Production
        meta docs --dev customers        # Dev (personal_USERNAME)
    """
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


if __name__ == "__main__":
    app()
