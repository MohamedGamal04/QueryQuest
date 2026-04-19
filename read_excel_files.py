from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from app_config import Excel_DIR

SUPPORTED_SUFFIXES = {".xlsx", ".xls"}


def normalize_excel_dir(excel_dir: str | Path | None = None) -> Path:
    path = Path(excel_dir) if excel_dir is not None else Excel_DIR
    return path.expanduser().resolve()


def list_excel_files(excel_dir: str | Path | None = None) -> list[Path]:
    directory = normalize_excel_dir(excel_dir)
    if not directory.exists() or not directory.is_dir():
        return []
    return sorted(
        [item for item in directory.iterdir() if item.is_file() and item.suffix.lower() in SUPPORTED_SUFFIXES],
        key=lambda item: item.name.lower(),
    )


def read_excel_file(file_path: Path) -> pd.ExcelFile:
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path.name} not found in {file_path.parent}")
    return pd.ExcelFile(file_path)


def get_col_info(df: pd.DataFrame) -> str:
    col_types = df.dtypes
    numeric_cols = df.select_dtypes(include="number")
    col_max = numeric_cols.max().to_dict() if not numeric_cols.empty else {}
    col_min = numeric_cols.min().to_dict() if not numeric_cols.empty else {}
    rows = len(df)
    cols = len(df.columns)
    info = "\n".join([f"{col}: {dtype}" for col, dtype in col_types.items()])
    if not info:
        info = "No columns found"
    info += f"\nColumns: {cols}"
    info += f"\nMax values: {col_max}"
    info += f"\nMin values: {col_min}"
    info += f"\nRows: {rows}"
    return info


def get_sample_rows(df: pd.DataFrame, sample_size: int = 3) -> str:
    if df.empty:
        return "Sample rows: No rows found"

    sample_df = df.head(sample_size)
    headers = [str(column).strip() for column in sample_df.columns]
    lines = [" | ".join(headers)]

    for row in sample_df.itertuples(index=False, name=None):
        values = [str(value).strip() for value in row]
        lines.append(" | ".join(values))

    return "Sample rows:\n" + "\n".join(lines)


def _snapshot_file(file_path: Path) -> dict[str, str | int]:
    stat = file_path.stat()
    return {
        "name": file_path.name,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def get_excel_snapshot(excel_dir: str | Path | None = None) -> tuple[str, list[dict[str, str | int]]]:
    files = list_excel_files(excel_dir)
    snapshot = [_snapshot_file(file_path) for file_path in files]
    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode("utf-8")).hexdigest()
    return digest, snapshot


def describe_excel_snapshot_changes(
    previous_snapshot: list[dict[str, str | int]] | None,
    current_snapshot: list[dict[str, str | int]] | None,
) -> str:
    previous_names = {
        str(item.get("name"))
        for item in (previous_snapshot or [])
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    current_names = {
        str(item.get("name"))
        for item in (current_snapshot or [])
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }

    added = sorted(current_names - previous_names)
    removed = sorted(previous_names - current_names)
    if not added and not removed:
        return ""

    sections: list[str] = ["### EXCEL FILE CHANGES"]
    if added:
        sections.append(f"Added files: {', '.join(added)}")
    if removed:
        sections.append(f"Removed files: {', '.join(removed)}")
    return "\n".join(sections)


def build_excel_files_info(excel_dir: str | Path | None = None) -> tuple[str, str, list[dict[str, str | int]]]:
    directory = normalize_excel_dir(excel_dir)
    files = list_excel_files(directory)
    digest, snapshot = get_excel_snapshot(directory)

    if not files:
        message = f"Excel directory: {directory}\nNo Excel files found."
        return message, digest, snapshot

    sections: list[str] = [f"Excel directory: {directory}", f"Excel files found: {len(files)}"]
    for file_path in files:
        xls = read_excel_file(file_path)
        sheet_names = [str(sheet_name) for sheet_name in xls.sheet_names]
        sheet_sections: list[str] = [f"File: {file_path.name}", f"Sheets: {', '.join(sheet_names)}"]
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            sheet_sections.append(f"Sheet: {sheet_name}")
            sheet_sections.append(get_col_info(df))
            sheet_sections.append(get_sample_rows(df))
        sections.append("\n".join(sheet_sections))

    return "\n\n".join(sections), digest, snapshot


def format_excel_context(info: str | None) -> str:
    if not info:
        return ""
    return f"### EXCEL FILE CONTEXT\n{info}"


if __name__ == "__main__":
    info, _, _ = build_excel_files_info()
    print(info)
