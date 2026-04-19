from __future__ import annotations

from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table


def _format_cell(value: Any) -> str:
	if value is None:
		return ""
	# Keep table output clean for missing numeric values.
	if pd.isna(value):
		return ""
	return str(value)


def print_sql_statements_table(sql_statements: list[str], console: Console) -> None:
	table = Table(title="SQL statements to execute", show_lines=True)
	table.add_column("#", style="cyan", no_wrap=True)
	table.add_column("Statement", style="white")

	for index, statement in enumerate(sql_statements, start=1):
		table.add_row(str(index), statement)

	console.print(table)


def print_dataframe_as_table(df: pd.DataFrame, console: Console, title: str = "Query result") -> None:
	table = Table(title=title, show_lines=True)

	for column_name in df.columns:
		table.add_column(str(column_name), overflow="fold")

	for row in df.itertuples(index=False, name=None):
		table.add_row(*[_format_cell(value) for value in row])

	if df.empty:
		console.print("No rows returned.")
		return

	console.print(table)
