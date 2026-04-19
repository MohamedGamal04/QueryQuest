import unittest

from rich.console import Console

from queryquest.sql.preview import print_sql_statements_table


class SqlPreviewTests(unittest.TestCase):
    def test_print_sql_statements_table_shows_delete_warning(self) -> None:
        console = Console(record=True)

        print_sql_statements_table(["DELETE FROM listings WHERE id = 1"], console)

        rendered = console.export_text()
        self.assertIn("DELETE FROM listings WHERE id = 1", rendered)
        self.assertIn("Warning:", rendered)

    def test_print_sql_statements_table_no_warning_for_non_delete(self) -> None:
        console = Console(record=True)

        print_sql_statements_table(["SELECT * FROM listings"], console)

        rendered = console.export_text()
        self.assertIn("SELECT * FROM listings", rendered)
        self.assertNotIn("Warning:", rendered)


if __name__ == "__main__":
    unittest.main()
