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
from typing import Optional
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
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


def _build_commands_panel() -> Panel:
    """Build Commands panel with categorized commands"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=20)
    table.add_column(style=STYLE_DESCRIPTION)

    # Core commands (green)
    table.add_row("[bold]Core:[/bold]", "")
    table.add_row("  [green]info[/green]", "Model summary (name, schema, table, materialization, tags)")
    table.add_row("  [green]schema[/green]", "Production table name (--dev for dev schema)")
    table.add_row("  [green]columns[/green]", "Column names and types (--dev for dev schema)")
    table.add_row("", "")

    # Advanced commands (yellow)
    table.add_row("[bold]Advanced:[/bold]", "")
    table.add_row("  [yellow]config[/yellow]", "Full dbt config (29 fields: partition_by, cluster_by, etc.)")
    table.add_row("  [yellow]deps[/yellow]", "Dependencies by type (refs, sources, macros)")
    table.add_row("  [yellow]sql[/yellow]", "Compiled SQL (default) or raw SQL with --jinja")
    table.add_row("  [yellow]docs[/yellow]", "Column names, types, and descriptions")
    table.add_row("  [yellow]path[/yellow]", "Relative file path to .sql file")
    table.add_row("  [yellow]parents[/yellow]", "Upstream dependencies (direct or --all ancestors)")
    table.add_row("  [yellow]children[/yellow]", "Downstream dependencies (direct or --all descendants)")
    table.add_row("", "")

    # Utilities (blue)
    table.add_row("[bold]Utilities:[/bold]", "")
    table.add_row("  [blue]list[/blue]", "List models (optionally filter by pattern)")
    table.add_row("  [blue]search[/blue]", "Search by name or description")
    table.add_row("  [blue]node[/blue]", "Full node details by unique_id or model name")
    table.add_row("  [blue]refresh[/blue]", "Refresh manifest (runs dbt parse)")

    return Panel(table, title="[b]📊 Commands[/b]", title_align="left", border_style="dim", padding=(0, 1))


def _build_flags_panel() -> Panel:
    """Build Flags panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=20)
    table.add_column(style=STYLE_DESCRIPTION)

    table.add_row("-h, --help", "Show this help message")
    table.add_row("-v, --version", "Show version and exit")
    table.add_row("-j, --json", "Output as JSON (most commands)")
    table.add_row("-d, --dev", "Use dev schema (schema/columns commands)")
    table.add_row("--all", "Recursive mode (parents/children)")
    table.add_row("--jinja", "Show raw SQL with Jinja (sql command only)")
    table.add_row("--manifest PATH", "Override manifest.json location")

    return Panel(table, title="[b]🚩 Flags[/b]", title_align="left", border_style="dim", padding=(0, 1))


def _build_examples_panel() -> Panel:
    """Build Examples panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=45)
    table.add_column(style=STYLE_DIM)

    table.add_row("meta schema jaffle_shop__customers", "Get production table name")
    table.add_row("meta schema --dev jaffle_shop__orders", "Get dev table name (personal_*)")
    table.add_row("meta columns -j jaffle_shop__orders", "Get columns as JSON")
    table.add_row("meta deps -j jaffle_shop__customers", "Get dependencies")
    table.add_row("meta sql jaffle_shop__customers", "View compiled SQL")
    table.add_row('meta search "customer"', "Search by name/description")

    return Panel(table, title="[b]💡 Examples[/b]", title_align="left", border_style="dim", padding=(0, 1))


def _build_manifest_priority_panel() -> Panel:
    """Build Manifest Priority panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style="bold", width=4, no_wrap=True)
    table.add_column(style="white")

    table.add_row("1.", "DBT_MANIFEST_PATH (if set)")
    table.add_row("2.", "./{DBT_PROD_STATE_PATH}/manifest.json [green](PRODUCTION)[/green]")
    table.add_row("3.", "./target/manifest.json")
    table.add_row("4.", "$DBT_PROJECT_PATH/{DBT_PROD_STATE_PATH}/manifest.json")
    table.add_row("5.", "Search upward for {DBT_PROD_STATE_PATH}/manifest.json")

    return Panel(table, title="[b]⚙️ Manifest Priority[/b]", title_align="left", border_style="dim", padding=(0, 1))


def _build_env_vars_panel() -> Panel:
    """Build Environment Variables panel"""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style=STYLE_COMMAND, no_wrap=True, width=27)
    table.add_column(style=STYLE_DESCRIPTION)

    table.add_row("DBT_PROJECT_PATH", "dbt project root directory")
    table.add_row("DBT_PROD_STATE_PATH", "Prod manifest dir: '.dbt-state'")
    table.add_row("DBT_USER", "Override username (default: $USER)")
    table.add_row("[yellow]DBT_DEV_DATASET[/yellow]", "[green]Dev dataset name (recommended)[/green]")
    table.add_row("DBT_DEV_TABLE_PATTERN", "Dev table pattern: 'name' (default)")
    table.add_row("DBT_PROD_TABLE_NAME", "Table: 'alias_or_name' (default)")
    table.add_row("DBT_PROD_SCHEMA_SOURCE", "Schema: 'config_or_model' (default)")
    table.add_row("DBT_VALIDATE_BIGQUERY", "Validate BigQuery names (opt-in)")

    return Panel(table, title="[b]🔧 Environment Variables[/b]", title_align="left", border_style="dim", padding=(0, 1))


def show_help_with_examples(ctx: typer.Context):
    """Show help with additional examples and usage info"""
    # Print usage and description
    rprint(" [bold]Usage:[/bold] meta [OPTIONS] COMMAND [ARGS]...")
    rprint()
    rprint(" AI-first CLI for dbt metadata extraction")
    rprint()
    rprint()

    # Print all sections
    console.print(_build_commands_panel())
    console.print(_build_flags_panel())
    console.print(_build_examples_panel())
    console.print(_build_manifest_priority_panel())
    console.print(_build_env_vars_panel())

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


def get_manifest_path(manifest_path: Optional[str] = None) -> str:
    """
    Get manifest path from explicit parameter or auto-discover

    Args:
        manifest_path: Optional explicit path

    Returns:
        Absolute path to manifest.json

    Raises:
        typer.Exit: If manifest not found
    """
    if manifest_path:
        return manifest_path

    try:
        return ManifestFinder.find()
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
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Model summary (name, schema, table, materialization, tags)

    Examples:
        meta info -j customers               # Production
        meta info --dev -j customers         # Dev (personal_USERNAME)
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.info(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if not result:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        # Rich formatted output
        rprint(f"[{STYLE_HEADER}]Model: {result['name']}[/{STYLE_HEADER}]")
        print(f"Database:      {result['database']}")
        print(f"Schema:        {result['schema']}")
        print(f"Table:         {result['table']}")
        print(f"Full Name:     {result['full_name']}")
        print(f"Materialized:  {result['materialized']}")
        print(f"File:          {result['file']}")
        print(f"Tags:          {', '.join(result['tags']) if result['tags'] else '(none)'}")


@app.command()
def schema(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Production table name (database.schema.table) or dev with --dev flag

    Examples:
        meta schema jaffle_shop__orders            # Production
        meta schema --dev jaffle_shop__orders      # Dev (personal_USERNAME)
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.schema(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if not result:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        if 'database' in result and result['database']:
            print(f"Database: {result['database']}")
        print(f"Schema:   {result['schema']}")
        print(f"Table:    {result['table']}")
        print(f"Full:     {result['full_name']}")


@app.command()
def columns(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema"),
):
    """
    Column names and types

    Examples:
        meta columns -j customers                # Production
        meta columns --dev -j customers          # Dev
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.columns(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if not result:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        # Rich table output
        table = Table(title=f"Columns: {model_name}")
        table.add_column("Name", style=STYLE_COMMAND, no_wrap=True)
        table.add_column("Type", style=STYLE_GREEN)

        for col in result:
            table.add_row(col['name'], col['data_type'])

        console.print(table)


@app.command()
def config(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Full dbt config (29 fields: partition_by, cluster_by, etc.)

    Examples:
        meta config -j model_name              # Production
        meta config --dev -j model_name        # Dev (personal_USERNAME)
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.config(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if result is None:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[{STYLE_HEADER}]Config: {model_name}[/{STYLE_HEADER}]")
        for key, value in result.items():
            print(f"{key:25s} {value}")


@app.command()
def deps(
    model_name: str = typer.Argument(..., help="Model name"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Dependencies by type (refs, sources, macros)

    Examples:
        meta deps -j model_name              # Production
        meta deps --dev -j model_name        # Dev (personal_USERNAME)
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.deps(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if result is None:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[{STYLE_HEADER}]Dependencies: {model_name}[/{STYLE_HEADER}]")
        print(f"\nRefs ({len(result['refs'])}):")
        for ref in result['refs']:
            print(f"  - {ref}")
        print(f"\nSources ({len(result['sources'])}):")
        for source in result['sources']:
            print(f"  - {source}")
        print(f"\nMacros ({len(result.get('macros', []))}):")
        for macro in result.get('macros', []):
            print(f"  - {macro}")


@app.command()
def sql(
    model_name: str = typer.Argument(..., help="Model name"),
    jinja: bool = typer.Option(False, "--jinja", help="Show raw SQL with Jinja"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Compiled SQL (default) or raw SQL with --jinja

    Examples:
        meta sql model_name                  # Production compiled SQL
        meta sql --dev model_name            # Dev (personal_USERNAME)
        meta sql --jinja model_name          # Raw SQL with Jinja
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.sql(manifest_path, model_name, use_dev=use_dev, raw=jinja, json_output=json_output)

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

    print(result)


@app.command()
def path(
    model_name: str = typer.Argument(..., help="Model name"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Relative file path to .sql file

    Examples:
        meta path model_name              # Production
        meta path --dev model_name        # Dev (personal_USERNAME)
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.path(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if result is None:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    print(result)


@app.command("list")
def list_cmd(
    pattern: Optional[str] = typer.Argument(None, help="Filter pattern"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
):
    """
    List models (optionally filter by pattern)

    Example: meta list jaffle_shop
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.list_models(manifest_path, pattern)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[{STYLE_HEADER}]Models ({len(result)}):[/{STYLE_HEADER}]")
        for model in result:
            print(f"  {model}")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
):
    """
    Search by name or description

    Example: meta search "customers" --json
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.search(manifest_path, query)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[{STYLE_HEADER}]Search results for '{query}' ({len(result)}):[/{STYLE_HEADER}]")
        for model in result:
            print(f"\n  [cyan]{model['name']}[/cyan]")
            if model['description']:
                print(f"  {model['description'][:100]}...")


@app.command()
def parents(
    model_name: str = typer.Argument(..., help="Model name"),
    all_ancestors: bool = typer.Option(False, "--all", help="Get all ancestors (recursive)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Upstream dependencies (direct or all ancestors)

    Examples:
        meta parents -j model_name                    # Production, direct parents
        meta parents --dev -j --all model_name        # Dev, all ancestors
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.parents(manifest_path, model_name, use_dev=use_dev, recursive=all_ancestors, json_output=json_output)

    if result is None:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        mode = "All ancestors" if all_ancestors else "Direct parents"
        rprint(f"[{STYLE_HEADER}]{mode} for {model_name} ({len(result)}):[/{STYLE_HEADER}]")
        for parent in result:
            print(f"  {parent['unique_id']}")


@app.command()
def children(
    model_name: str = typer.Argument(..., help="Model name"),
    all_descendants: bool = typer.Option(False, "--all", help="Get all descendants (recursive)"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Downstream dependencies (direct or all descendants)

    Examples:
        meta children model_name                    # Production, direct children
        meta children --dev --all model_name        # Dev, all descendants
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.children(manifest_path, model_name, use_dev=use_dev, recursive=all_descendants, json_output=json_output)

    if result is None:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        mode = "All descendants" if all_descendants else "Direct children"
        rprint(f"[{STYLE_HEADER}]{mode} for {model_name} ({len(result)}):[/{STYLE_HEADER}]")
        for child in result:
            print(f"  {child['unique_id']}")


@app.command()
def node(
    identifier: str = typer.Argument(..., help="Model name or unique_id"),
    json_output: bool = typer.Option(False, "-j", "--json", help="Output as JSON"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
):
    """
    Full node details by unique_id or model name

    Returns complete node metadata from manifest
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.node(manifest_path, identifier)

    if not result:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Node '{identifier}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[{STYLE_HEADER}]Node: {result.get('name', 'unknown')}[/{STYLE_HEADER}]")
        print(f"Unique ID:     {result.get('unique_id', 'N/A')}")
        print(f"Resource Type: {result.get('resource_type', 'N/A')}")
        print(f"Database:      {result.get('database', 'N/A')}")
        print(f"Schema:        {result.get('schema', 'N/A')}")
        print(f"Materialized:  {result.get('config', {}).get('materialized', 'N/A')}")


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
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Path to manifest.json"),
    use_dev: bool = typer.Option(False, "-d", "--dev", help="Use dev schema (personal_*)"),
):
    """
    Column names, types, and descriptions

    Examples:
        meta docs customers              # Production
        meta docs --dev customers        # Dev (personal_USERNAME)
    """
    manifest_path = get_manifest_path(manifest)
    result = commands.docs(manifest_path, model_name, use_dev=use_dev, json_output=json_output)

    if not result:
        console.print(f"[{STYLE_ERROR}]Error:[/{STYLE_ERROR}] Model '{model_name}' not found")
        raise typer.Exit(code=1)

    if json_output:
        print(json.dumps(result, indent=2))
    else:
        # Rich table output
        table = Table(title=f"Column Documentation: {model_name}")
        table.add_column("Name", style=STYLE_COMMAND, no_wrap=True)
        table.add_column("Type", style=STYLE_GREEN)
        table.add_column("Description", style=STYLE_DESCRIPTION)

        for col in result:
            desc = col.get('description', '')
            if len(desc) > 80:
                desc = desc[:77] + "..."
            table.add_row(col['name'], col['data_type'], desc or "(no description)")

        console.print(table)


if __name__ == "__main__":
    app()
