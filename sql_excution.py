import json
import re
import sys
from pathlib import Path
from typing import TypedDict

import duckdb
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt

from app_config import Excel_DIR
from read_excel_files import list_excel_files
from sql_preview import print_dataframe_as_table, print_sql_statements_table


class WorkbookRecord(TypedDict):
    file_path: Path
    original_table_name: str
    normalized_table_name: str
    original_sheet_name: str
    original_columns: list[str]
    normalized_columns: list[str]
    sheet_data: dict[str, pd.DataFrame]


def _table_name_from_file(file_name: str) -> str:
    stem = file_name.rsplit(".", 1)[0]
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", stem).strip("_")
    return normalized or "excel_data"


def _column_name_from_value(value: object) -> str:
    text = str(value).strip()
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return normalized or "column"


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed_columns: list[str] = []
    seen: dict[str, int] = {}
    for column in df.columns:
        base_name = _column_name_from_value(column)
        count = seen.get(base_name, 0)
        seen[base_name] = count + 1
        renamed_columns.append(base_name if count == 0 else f"{base_name}_{count + 1}")
    normalized_df = df.copy()
    normalized_df.columns = renamed_columns
    return normalized_df


def _normalize_sql_statement(statement: str) -> str:
    return statement.replace("`", '"')


def _quote_known_identifiers(statement: str, identifiers: set[str]) -> str:
    rewritten = statement
    for identifier in sorted(identifiers, key=len, reverse=True):
        if not identifier or " " not in identifier:
            continue
        pattern = re.compile(rf'(?<!")({re.escape(identifier)})(?!")')
        rewritten = pattern.sub(r'"\1"', rewritten)
    return rewritten


def _strip_identifier_quotes(identifier: str) -> str:
    cleaned = identifier.strip()
    if cleaned[:1] in {'`', '"', '['} and cleaned[-1:] in {'`', '"', ']'}:
        return cleaned[1:-1]
    return cleaned


def _is_dml_statement(statement: str) -> bool:
    return statement.lstrip().lower().startswith(("delete", "insert", "update"))


def _extract_target_table_name(statement: str) -> str | None:
    update_match = re.match(r"^\s*update\s+(.+?)\s+set\b", statement, flags=re.IGNORECASE)
    if update_match:
        return _strip_identifier_quotes(update_match.group(1))

    delete_match = re.match(r"^\s*delete\s+from\s+(.+?)(?:\s+where\b|\s*$)", statement, flags=re.IGNORECASE)
    if delete_match:
        return _strip_identifier_quotes(delete_match.group(1))

    insert_match = re.match(
        r"^\s*insert\s+into\s+(.+?)(?:\s*\(|\s+values\b|\s+select\b|\s*$)",
        statement,
        flags=re.IGNORECASE,
    )
    if insert_match:
        return _strip_identifier_quotes(insert_match.group(1))

    return None


def _save_dataframe_to_workbook(file_path: Path, sheet_name: str, sheet_data: dict[str, pd.DataFrame], df: pd.DataFrame) -> None:
    with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
        for current_sheet_name, current_df in sheet_data.items():
            data_to_write = df if current_sheet_name == sheet_name else current_df
            data_to_write.to_excel(writer, sheet_name=current_sheet_name, index=False)

def read_sql_handoff_file(file_path: str) -> list[str]:
    with open(file_path, 'r') as file:
        json_string = file.read()
    data = json.loads(json_string)
    return data.get("sql_statements", [])

def _print_sql_preview(sql_statements: list[str], console: Console) -> None:
    console.print()
    print_sql_statements_table(sql_statements, console)


def execute_sql_statements(sql_statements: list[str], console: Console | None = None) -> None:
    active_console = console or Console()
    if not sql_statements:
        active_console.print("No SQL statements to execute.")
        return

    _print_sql_preview(sql_statements, active_console)

    connection = duckdb.connect()
    column_identifiers: set[str] = set()
    table_identifiers: set[str] = set()
    workbook_records: dict[str, WorkbookRecord] = {}

    for file_path in list_excel_files(Excel_DIR):
        original_table_name = file_path.stem
        normalized_table_name = _table_name_from_file(file_path.name)
        table_identifiers.add(original_table_name)
        table_identifiers.add(normalized_table_name)
        workbook = pd.ExcelFile(file_path)
        sheet_names = [str(sheet_name) for sheet_name in workbook.sheet_names]
        first_sheet = sheet_names[0]
        sheet_data: dict[str, pd.DataFrame] = {sheet_name: workbook.parse(sheet_name) for sheet_name in sheet_names}
        original_df = sheet_data[first_sheet]
        normalized_df = _normalize_columns(original_df)
        column_identifiers.update(str(column) for column in original_df.columns)
        source_original_name = f"_source_{normalized_table_name}_original"
        source_normalized_name = f"_source_{normalized_table_name}_normalized"
        connection.register(source_original_name, original_df)
        connection.register(source_normalized_name, normalized_df)
        connection.execute(f'DROP TABLE IF EXISTS "{original_table_name}"')
        connection.execute(f'CREATE TABLE "{original_table_name}" AS SELECT * FROM {source_original_name}')
        connection.execute(f"DROP TABLE IF EXISTS {normalized_table_name}")
        connection.execute(f"CREATE TABLE {normalized_table_name} AS SELECT * FROM {source_normalized_name}")

        record: WorkbookRecord = {
            "file_path": file_path,
            "original_table_name": original_table_name,
            "normalized_table_name": normalized_table_name,
            "original_sheet_name": first_sheet,
            "original_columns": [str(column) for column in original_df.columns],
            "normalized_columns": [str(column) for column in normalized_df.columns],
            "sheet_data": sheet_data,
        }
        workbook_records[original_table_name] = record
        workbook_records[normalized_table_name] = record

    wrote_data = False
    for statement in sql_statements:
        normalized_statement = _normalize_sql_statement(statement)
        normalized_statement = _quote_known_identifiers(normalized_statement, table_identifiers)
        normalized_statement = _quote_known_identifiers(normalized_statement, column_identifiers)
        cursor = connection.execute(normalized_statement)
        if _is_dml_statement(statement):
            wrote_data = True
        if cursor.description is None:
            affected_rows = cursor.rowcount
            if affected_rows != -1:
                active_console.print(affected_rows)
            continue

        results = cursor.df()
        print_dataframe_as_table(results, active_console)

    if not wrote_data:
        return

    should_save = Prompt.ask(
        "Save changes back to the Excel files?",
        choices=["y", "n"],
        default="n",
        console=active_console,
    ).strip()
    if should_save.lower() != "y":
        return

    saved_files: set[Path] = set()
    saved_results: dict[str, pd.DataFrame] = {}
    for statement in sql_statements:
        if not _is_dml_statement(statement):
            continue
        target_table_name = _extract_target_table_name(_normalize_sql_statement(statement))
        if target_table_name is None:
            continue
        target_table_name = str(target_table_name)
        workbook_record = workbook_records.get(target_table_name)
        if workbook_record is None:
            continue

        file_path = workbook_record["file_path"]
        if not isinstance(file_path, Path) or file_path in saved_files:
            continue

        original_table_name = str(workbook_record["original_table_name"])
        normalized_table_name = str(workbook_record["normalized_table_name"])
        original_sheet_name = str(workbook_record["original_sheet_name"])
        original_columns = workbook_record["original_columns"]
        normalized_columns = workbook_record["normalized_columns"]
        sheet_data = dict(workbook_record["sheet_data"])

        current_df = connection.table(target_table_name).df()
        if list(current_df.columns) == normalized_columns:
            current_df = current_df.copy()
            current_df.columns = original_columns
        elif list(current_df.columns) != original_columns and len(current_df.columns) == len(original_columns):
            current_df = current_df.copy()
            current_df.columns = original_columns

        sheet_data[original_sheet_name] = current_df
        _save_dataframe_to_workbook(file_path, original_sheet_name, sheet_data, current_df)
        saved_files.add(file_path)
        saved_results[original_table_name] = current_df.copy()

    if saved_files:
        active_console.print("Changes saved.")
        for table_name, saved_df in saved_results.items():
            preview_df = saved_df.head(20)
            print_dataframe_as_table(preview_df, active_console, title=f"Query result ({table_name})")
            if len(saved_df) > len(preview_df):
                active_console.print(f"Showing first {len(preview_df)} of {len(saved_df)} rows.")

if __name__ == "__main__":
    handoff_file = sys.argv[1] if len(sys.argv) > 1 else "/home/jimmy/Documents/ExcelAgent/temporary_sql_input.json"
    sql_statements = read_sql_handoff_file(handoff_file)
    execute_sql_statements(sql_statements)