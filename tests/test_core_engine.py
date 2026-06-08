"""End-to-end tests for the async QueryEngine with a fake LLM.

Author: mohamedgamal04
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from queryquest.core.engine import QueryEngine
from queryquest.core.models import EngineConfig
from queryquest.core.policy import AutoApprovePolicy, DenyAllPolicy, Policy


def _config(excel_dir: Path) -> EngineConfig:
    return EngineConfig(
        base_url="http://fake",
        api_key="key",
        model="fake-model",
        provider_name="fake",
        system_prompt="system",
        excel_dir=excel_dir,
    )


def _fake_llm(statements: list[str], explanation: str = "ok"):
    """Build an async stand-in for core.llm.generate_sql."""

    async def _generate(config, user_prompt):
        raw = json.dumps({"sql_statements": statements, "explanation": explanation})
        return raw, statements, explanation, None

    return _generate


class _WritebackDenyPolicy(Policy):
    """Approves execution but refuses every write-back."""

    async def approve_execution(self, statements) -> bool:
        return True

    async def approve_writeback(self, target) -> bool:
        return False


class CoreEngineTests(unittest.IsolatedAsyncioTestCase):
    def _make_workbook(self, directory: Path) -> Path:
        path = directory / "listings.xlsx"
        pd.DataFrame({"id": [1, 2], "price": [10, 20]}).to_excel(path, index=False)
        return path

    async def test_select_returns_preview_rows(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            with patch("queryquest.core.engine.generate_sql", new=_fake_llm(["SELECT * FROM listings"])):
                result = await engine.run("show listings", AutoApprovePolicy(directory))

            self.assertTrue(result.executed)
            self.assertIsNone(result.error)
            self.assertEqual(len(result.statements), 1)
            statement = result.statements[0]
            self.assertIsNone(statement.error)
            self.assertEqual(statement.row_count, 2)
            self.assertIn("price", statement.columns)

    async def test_update_writes_back_when_approved(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            path = self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            sql = "UPDATE listings SET price = 99 WHERE id = 1"
            with patch("queryquest.core.engine.generate_sql", new=_fake_llm([sql])):
                result = await engine.run("bump price", AutoApprovePolicy(directory))

            self.assertTrue(result.executed)
            self.assertTrue(result.wrote_back)
            saved = pd.read_excel(path)
            self.assertEqual(int(saved.loc[saved["id"] == 1, "price"].iloc[0]), 99)
            # The write-back target carries a preview of the rows about to change.
            target = result.writeback_targets[0]
            self.assertEqual(target.affected_rows, 1)
            self.assertEqual(len(target.preview_rows), 1)
            self.assertIn("price", target.preview_columns)

    async def test_zero_row_update_offers_no_writeback(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            path = self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            sql = "UPDATE listings SET price = 5 WHERE id = 999"  # matches nothing
            with patch("queryquest.core.engine.generate_sql", new=_fake_llm([sql])):
                result = await engine.run("noop", AutoApprovePolicy(directory))

            self.assertTrue(result.executed)
            self.assertEqual(result.statements[0].row_count, 0)
            self.assertEqual(result.writeback_targets, [])
            self.assertFalse(result.wrote_back)
            saved = pd.read_excel(path)
            self.assertEqual(sorted(saved["price"].tolist()), [10, 20])

    async def test_insert_reports_affected_count_and_saves(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            path = self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            sql = "INSERT INTO listings VALUES (3, 30)"
            with patch("queryquest.core.engine.generate_sql", new=_fake_llm([sql])):
                result = await engine.run("add a row", AutoApprovePolicy(directory))

            self.assertEqual(result.statements[0].row_count, 1)  # not 0 despite DuckDB rowcount -1
            self.assertTrue(result.wrote_back)
            saved = pd.read_excel(path)
            self.assertEqual(len(saved), 3)
            self.assertIn(3, saved["id"].tolist())

    async def test_deny_all_skips_execution(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            with patch("queryquest.core.engine.generate_sql", new=_fake_llm(["SELECT * FROM listings"])):
                result = await engine.run("show listings", DenyAllPolicy())

            self.assertFalse(result.executed)
            self.assertFalse(result.wrote_back)

    async def test_writeback_denied_leaves_file_unchanged(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            path = self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            sql = "UPDATE listings SET price = 99 WHERE id = 1"
            with patch("queryquest.core.engine.generate_sql", new=_fake_llm([sql])):
                result = await engine.run("bump price", _WritebackDenyPolicy())

            self.assertTrue(result.executed)
            self.assertFalse(result.wrote_back)
            saved = pd.read_excel(path)
            self.assertEqual(int(saved.loc[saved["id"] == 1, "price"].iloc[0]), 10)

    async def test_sandbox_blocks_file_read(self) -> None:
        with TemporaryDirectory() as raw_dir:
            directory = Path(raw_dir)
            self._make_workbook(directory)
            engine = QueryEngine(_config(directory))

            with patch(
                "queryquest.core.engine.generate_sql",
                new=_fake_llm(["SELECT * FROM read_csv('/etc/passwd')"]),
            ):
                result = await engine.run("leak", AutoApprovePolicy(directory))

            self.assertFalse(result.executed)
            self.assertEqual(len(result.statements), 1)
            self.assertIsNotNone(result.statements[0].error)


if __name__ == "__main__":
    unittest.main()
