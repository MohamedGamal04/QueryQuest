"""Tests for CLI rendering of engine results.

Author: mohamedgamal04
"""

import unittest

from rich.console import Console

from queryquest.chat_session import _render_result
from queryquest.core.models import EngineResult, StatementResult


def _render(statement: StatementResult) -> str:
    console = Console(record=True, width=200)
    result = EngineResult(prompt="p", executed=True, statements=[statement])
    _render_result(console, result, "groq", "model")
    return console.export_text()


class RenderResultTests(unittest.TestCase):
    def test_empty_join_explains_missing_key(self) -> None:
        statement = StatementResult(
            sql="SELECT * FROM a JOIN b ON a.x = b.y",
            kind="select",
            row_count=0,
        )
        self.assertIn("join matched no rows", _render(statement).lower())

    def test_empty_non_join_select_hint(self) -> None:
        statement = StatementResult(sql="SELECT * FROM a WHERE x = 1", kind="select", row_count=0)
        output = _render(statement).lower()
        self.assertIn("matched no rows", output)
        self.assertNotIn("join", output)

    def test_non_empty_select_has_no_hint(self) -> None:
        statement = StatementResult(
            sql="SELECT * FROM a",
            kind="select",
            columns=["x"],
            rows=[{"x": 1}],
            row_count=1,
        )
        self.assertNotIn("matched no rows", _render(statement).lower())


if __name__ == "__main__":
    unittest.main()
