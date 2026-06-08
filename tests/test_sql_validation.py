"""Tests for the static SQL safety sandbox.

Author: mohamedgamal04
"""

import unittest

from queryquest.sql.validation import (
    validate_statement_static,
    validate_statement_tables,
)


class ValidateStatementStaticTests(unittest.TestCase):
    def test_allows_plain_select(self) -> None:
        self.assertIsNone(validate_statement_static("SELECT * FROM listings"))

    def test_allows_update_set_clause(self) -> None:
        # SET must not be treated as a blocked admin token.
        self.assertIsNone(validate_statement_static("UPDATE listings SET price = 10 WHERE id = 1"))

    def test_blocks_read_csv(self) -> None:
        self.assertEqual(
            validate_statement_static("SELECT * FROM read_csv('/etc/passwd')"),
            "'READ_CSV' is not allowed",
        )

    def test_blocks_read_parquet(self) -> None:
        self.assertEqual(
            validate_statement_static("SELECT * FROM read_parquet('/tmp/secret.parquet')"),
            "'READ_PARQUET' is not allowed",
        )

    def test_blocks_glob(self) -> None:
        self.assertEqual(
            validate_statement_static("SELECT * FROM glob('/home/*')"),
            "'GLOB' is not allowed",
        )

    def test_blocks_attach(self) -> None:
        self.assertEqual(
            validate_statement_static("ATTACH 'evil.db' AS e"),
            "'ATTACH' is not allowed",
        )

    def test_blocks_copy(self) -> None:
        self.assertEqual(
            validate_statement_static("COPY listings TO '/tmp/out.csv'"),
            "'COPY' is not allowed",
        )

    def test_blocks_pragma(self) -> None:
        self.assertEqual(
            validate_statement_static("PRAGMA database_list"),
            "'PRAGMA' is not allowed",
        )

    def test_blocks_comments_anywhere(self) -> None:
        self.assertEqual(
            validate_statement_static("SELECT * FROM listings -- sneaky"),
            "SQL comments are not allowed",
        )

    def test_blocks_block_comments(self) -> None:
        self.assertEqual(
            validate_statement_static("SELECT /* hi */ * FROM listings"),
            "SQL comments are not allowed",
        )

    def test_semicolon_inside_string_is_not_multistatement(self) -> None:
        # The historic bug: a literal ';' tripped the multi-statement guard.
        self.assertIsNone(
            validate_statement_static("SELECT * FROM listings WHERE note = 'a;b'")
        )

    def test_rejects_multiple_statements(self) -> None:
        self.assertEqual(
            validate_statement_static("SELECT 1; DROP TABLE users"),
            "multiple SQL statements are not allowed",
        )

    def test_trailing_semicolon_allowed(self) -> None:
        self.assertIsNone(validate_statement_static("SELECT * FROM listings;"))

    def test_rejects_schema_change(self) -> None:
        self.assertEqual(
            validate_statement_static("CREATE TABLE x (id INT)"),
            "'CREATE' is not allowed",
        )


class ValidateStatementTablesTests(unittest.TestCase):
    def test_accepts_registered_table(self) -> None:
        self.assertIsNone(
            validate_statement_tables("SELECT * FROM listings", {"listings"})
        )

    def test_rejects_unregistered_table(self) -> None:
        self.assertEqual(
            validate_statement_tables("SELECT * FROM secrets", {"listings"}),
            "table 'secrets' is not a registered workbook table",
        )

    def test_accepts_registered_join(self) -> None:
        self.assertIsNone(
            validate_statement_tables(
                "SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
                {"orders", "customers"},
            )
        )

    def test_rejects_unregistered_join_table(self) -> None:
        self.assertEqual(
            validate_statement_tables(
                "SELECT * FROM orders JOIN secrets ON orders.id = secrets.id",
                {"orders", "customers"},
            ),
            "table 'secrets' is not a registered workbook table",
        )


if __name__ == "__main__":
    unittest.main()
