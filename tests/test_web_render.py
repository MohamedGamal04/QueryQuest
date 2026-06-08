"""Tests for the Chainlit result formatter.

Author: mohamedgamal04
"""

import unittest
from pathlib import Path

from queryquest.core.models import EngineResult, StatementResult, WritebackTarget
from queryquest.web.render import format_result_markdown


class FormatResultMarkdownTests(unittest.TestCase):
    def test_engine_error(self) -> None:
        result = EngineResult(prompt="p", error="Authentication failed")
        self.assertIn("Authentication failed", format_result_markdown(result))

    def test_select_renders_table(self) -> None:
        statement = StatementResult(
            sql="SELECT * FROM listings",
            kind="select",
            columns=["id", "price"],
            rows=[{"id": 1, "price": 10}, {"id": 2, "price": 20}],
            row_count=2,
        )
        out = format_result_markdown(EngineResult(prompt="p", statements=[statement]))
        self.assertIn("| id | price |", out)
        self.assertIn("| 1 | 10 |", out)

    def test_empty_join_hint(self) -> None:
        statement = StatementResult(sql="SELECT * FROM a JOIN b ON a.x = b.y", kind="select", row_count=0)
        self.assertIn("join matched no rows", format_result_markdown(EngineResult(prompt="p", statements=[statement])))

    def test_statement_error_shown(self) -> None:
        statement = StatementResult(sql="SELECT * FROM read_csv('/etc/passwd')", kind="select", error="'READ_CSV' is not allowed")
        out = format_result_markdown(EngineResult(prompt="p", statements=[statement]))
        self.assertIn("Refused/failed", out)
        self.assertIn("READ_CSV", out)

    def test_writeback_saved_note(self) -> None:
        statement = StatementResult(sql="UPDATE listings SET price = 9", kind="update", row_count=3)
        target = WritebackTarget(
            file_path=Path("/data/listings.xlsx"),
            sheet_name="Sheet1",
            table_name="listings__Sheet1",
            affected_rows=3,
        )
        result = EngineResult(
            prompt="p",
            statements=[statement],
            executed=True,
            wrote_back=True,
            writeback_targets=[target],
        )
        out = format_result_markdown(result)
        self.assertIn("listings.xlsx", out)
        self.assertIn("Saved", out)

    def test_pipe_in_cell_is_escaped(self) -> None:
        statement = StatementResult(
            sql="SELECT * FROM t",
            kind="select",
            columns=["note"],
            rows=[{"note": "a|b"}],
            row_count=1,
        )
        self.assertIn("a\\|b", format_result_markdown(EngineResult(prompt="p", statements=[statement])))


if __name__ == "__main__":
    unittest.main()
