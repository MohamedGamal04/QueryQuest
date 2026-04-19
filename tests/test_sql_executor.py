import unittest

from queryquest.sql.executor import (
    _normalize_single_quoted_table_identifiers,
    _quote_known_identifiers,
    _strip_identifier_quotes,
)


class SqlExecutorNormalizationTests(unittest.TestCase):
    def test_normalize_single_quoted_table_identifier_in_from_clause(self) -> None:
        statement = "SELECT * FROM 'Real Estate Listings' ORDER BY List Price DESC LIMIT 1"
        rewritten = _normalize_single_quoted_table_identifiers(statement, {"Real Estate Listings"})
        self.assertIn('FROM "Real Estate Listings"', rewritten)

    def test_does_not_requote_already_single_quoted_identifier(self) -> None:
        statement = "SELECT * FROM 'Real Estate Listings'"
        rewritten = _quote_known_identifiers(statement, {"Real Estate Listings"})
        self.assertEqual(rewritten, statement)

    def test_strip_identifier_quotes_handles_single_quotes(self) -> None:
        self.assertEqual(_strip_identifier_quotes("'Real Estate Listings'"), "Real Estate Listings")


if __name__ == "__main__":
    unittest.main()
