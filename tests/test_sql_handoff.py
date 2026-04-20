import unittest
from unittest.mock import patch

from rich.console import Console

from queryquest.sql.handoff import extract_sql_statements, expose_sql_statements


class SqlHandoffTests(unittest.TestCase):
    def test_extract_sql_statements_plain_json(self) -> None:
        output = '{"sql_statements": ["SELECT * FROM table1"], "explanation": "ok"}'
        self.assertEqual(extract_sql_statements(output), ["SELECT * FROM table1"])

    def test_extract_sql_statements_fenced_with_explanation_text(self) -> None:
        output = (
            "I can do that.\n"
            "```json\n"
            "{\n"
            '  "sql_statements": ["SELECT * FROM listings"],\n'
            '  "explanation": "preview"\n'
            "}\n"
            "```"
        )
        self.assertEqual(extract_sql_statements(output), ["SELECT * FROM listings"])

    def test_extract_sql_statements_embedded_json_object(self) -> None:
        output = "Use this payload: {\"sql_statements\": [\"SELECT 1\"], \"explanation\": \"ok\"} thanks"
        self.assertEqual(extract_sql_statements(output), ["SELECT 1"])

    def test_extract_sql_statements_invalid_json(self) -> None:
        self.assertEqual(extract_sql_statements("not json"), [])

    @patch("queryquest.sql.handoff.append_log")
    @patch("queryquest.sql.handoff.execute_sql_statements")
    def test_expose_sql_statements_executes_and_logs(self, mock_execute, mock_log) -> None:
        console = Console(record=True)
        sql = ["SELECT 1"]

        expose_sql_statements(sql, provider="groq", model="llama", console=console)

        mock_execute.assert_called_once_with(sql, console=console, excel_dir=None)
        self.assertTrue(mock_log.called)


if __name__ == "__main__":
    unittest.main()
