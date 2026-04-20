import unittest

from queryquest.sql.executor import (
    _prepare_statement,
    _normalize_single_quoted_table_identifiers,
    _quote_known_identifiers,
    _rewrite_to_normalized_identifiers,
    _validate_sql_allowlist,
    _strip_identifier_quotes,
    execute_sql_statements,
)
from rich.console import Console


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

    def test_rewrite_mixed_identifiers_to_normalized(self) -> None:
        statement = 'SELECT MAX("List Price") FROM Real_Estate_Listings'
        rewritten = _rewrite_to_normalized_identifiers(
            statement,
            table_name_map={"Real Estate Listings": "Real_Estate_Listings"},
            table_alias_map={"realestatelistings": "Real_Estate_Listings"},
            column_name_map={"List Price": "List_Price"},
            column_alias_map={"listprice": "List_Price"},
        )
        self.assertEqual(rewritten, "SELECT MAX(List_Price) FROM Real_Estate_Listings")

    def test_rewrite_original_table_and_columns_to_normalized(self) -> None:
        statement = 'SELECT MAX("List Price") FROM "Real Estate Listings"'
        rewritten = _rewrite_to_normalized_identifiers(
            statement,
            table_name_map={"Real Estate Listings": "Real_Estate_Listings"},
            table_alias_map={"realestatelistings": "Real_Estate_Listings"},
            column_name_map={"List Price": "List_Price"},
            column_alias_map={"listprice": "List_Price"},
        )
        self.assertEqual(rewritten, "SELECT MAX(List_Price) FROM Real_Estate_Listings")

    def test_rewrite_compact_table_alias_to_normalized(self) -> None:
        statement = 'SELECT MAX("List Price") FROM RealEstateListings'
        rewritten = _rewrite_to_normalized_identifiers(
            statement,
            table_name_map={"Real Estate Listings": "Real_Estate_Listings"},
            table_alias_map={"realestatelistings": "Real_Estate_Listings"},
            column_name_map={"List Price": "List_Price"},
            column_alias_map={"listprice": "List_Price"},
        )
        self.assertEqual(rewritten, "SELECT MAX(List_Price) FROM Real_Estate_Listings")

    def test_rewrite_qualified_compact_column_name_to_normalized(self) -> None:
        statement = "SELECT MC.CampaignName FROM Marketing_Campaigns MC"
        rewritten = _rewrite_to_normalized_identifiers(
            statement,
            table_name_map={},
            table_alias_map={"marketingcampaigns": "Marketing_Campaigns"},
            column_name_map={"Campaign Name": "Campaign_Name"},
            column_alias_map={"campaignname": "Campaign_Name"},
        )
        self.assertEqual(rewritten, "SELECT MC.Campaign_Name FROM Marketing_Campaigns MC")

    def test_rewrite_qualified_quoted_column_name_to_normalized(self) -> None:
        statement = 'SELECT MC."Campaign Name" FROM Marketing_Campaigns MC'
        rewritten = _rewrite_to_normalized_identifiers(
            statement,
            table_name_map={},
            table_alias_map={"marketingcampaigns": "Marketing_Campaigns"},
            column_name_map={"Campaign Name": "Campaign_Name"},
            column_alias_map={"campaignname": "Campaign_Name"},
        )
        self.assertEqual(rewritten, "SELECT MC.Campaign_Name FROM Marketing_Campaigns MC")

    def test_prepare_statement_rewrites_spaced_table_name_for_delete(self) -> None:
        statement = "DELETE FROM Real Estate Listings WHERE Listing_ID IS NULL"
        rewrite_context = {
            "table_identifiers": {"Real Estate Listings", "Real_Estate_Listings"},
            "column_identifiers": {"Listing ID", "Listing_ID"},
            "table_name_map": {"Real Estate Listings": "Real_Estate_Listings"},
            "table_alias_map": {"realestatelistings": "Real_Estate_Listings"},
            "column_name_map": {"Listing ID": "Listing_ID"},
            "column_alias_map": {"listingid": "Listing_ID"},
        }
        prepared = _prepare_statement(statement, rewrite_context)
        self.assertEqual(prepared, "DELETE FROM Real_Estate_Listings WHERE Listing_ID IS NULL")

    def test_validate_sql_allowlist_rejects_schema_changes(self) -> None:
        self.assertEqual(_validate_sql_allowlist("CREATE TABLE x (id INT)"), "'CREATE' is not allowed")

    def test_validate_sql_allowlist_rejects_multiple_statements(self) -> None:
        self.assertEqual(
            _validate_sql_allowlist("SELECT 1; DROP TABLE users"),
            "multiple SQL statements are not allowed",
        )

    def test_validate_sql_allowlist_rejects_join(self) -> None:
        self.assertEqual(
            _validate_sql_allowlist("SELECT * FROM a JOIN b ON a.id = b.id"),
            "'JOIN' is not allowed",
        )

    def test_execute_sql_statements_refuses_disallowed_sql(self) -> None:
        console = Console(record=True)

        execute_sql_statements(["DROP TABLE users"], console=console)

        output = console.export_text()
        self.assertIn("Refused", output)
        self.assertIn("DROP", output)
        self.assertNotIn("SQL statements to execute", output)

if __name__ == "__main__":
    unittest.main()
