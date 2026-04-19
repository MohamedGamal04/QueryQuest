"""Rich table rendering helpers used by SQL preview and execution flows."""

from __future__ import annotations

from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table


def _format_cell(value: Any) -> str:
	"""Normalize values for compact terminal table rendering."""
	if value is None:
		return ""
	# Keep table output clean for missing numeric values.
	if pd.isna(value):
		return ""
	return str(value)


def _is_delete_statement(statement: str) -> bool:
	"""Return True when SQL statement starts with DELETE."""
	return statement.lstrip().lower().startswith("delete")


def print_sql_statements_table(sql_statements: list[str], console: Console) -> None:
	"""Render the SQL statement list as a numbered Rich table."""
	table = Table(title="SQL statements to execute", show_lines=True)
	table.add_column("#", style="cyan", no_wrap=True)
	table.add_column("Statement", style="white")

	has_delete_statement = False

	for index, statement in enumerate(sql_statements, start=1):
		if _is_delete_statement(statement):
			has_delete_statement = True
			table.add_row(str(index), f"[bold red]{statement}[/bold red]")
		else:
			table.add_row(str(index), statement)

	console.print(table)
	if has_delete_statement:
		console.print("[bold red]Warning:[/bold red] DELETE statements will remove rows from the selected table PERMANENTLY.")


def print_dataframe_as_table(df: pd.DataFrame, console: Console, title: str = "Query result") -> None:
	"""Render a DataFrame as a Rich table with folded columns."""
	table = Table(title=title, show_lines=True)

	for column_name in df.columns:
		table.add_column(str(column_name), overflow="fold")

	for row in df.itertuples(index=False, name=None):
		table.add_row(*[_format_cell(value) for value in row])

	if df.empty:
		console.print("No rows returned.")
		return

	console.print(table)
