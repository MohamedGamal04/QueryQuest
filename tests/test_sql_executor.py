import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from queryquest.sql.executor import (
    _build_update_change_predicate,
    _extract_update_table_name,
    _extract_update_where_clause,
    _prepare_statement,
    _normalize_single_quoted_table_identifiers,
    _quote_known_identifiers,
    _rewrite_to_normalized_identifiers,
    _update_statement_to_scope_query,
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

    def test_extract_update_table_name(self) -> None:
        statement = "UPDATE listings SET price = 2000 WHERE price = 1000"
        self.assertEqual(_extract_update_table_name(statement), "listings")

    def test_extract_update_where_clause(self) -> None:
        statement = "UPDATE listings SET price = 2000 WHERE price = 1000"
        self.assertEqual(_extract_update_where_clause(statement), "price = 1000")

    def test_extract_update_where_clause_missing(self) -> None:
        statement = "UPDATE listings SET price = 2000"
        self.assertIsNone(_extract_update_where_clause(statement))

    def test_update_statement_to_scope_query_with_where(self) -> None:
        statement = "UPDATE listings SET price = 2000 WHERE price = 1000"
        self.assertEqual(
            _update_statement_to_scope_query(statement),
            "SELECT * FROM listings WHERE (price = 1000) AND ((price IS DISTINCT FROM (2000)))",
        )

    def test_update_statement_to_scope_query_without_where(self) -> None:
        statement = "UPDATE listings SET price = 2000"
        self.assertEqual(
            _update_statement_to_scope_query(statement),
            "SELECT * FROM listings WHERE ((price IS DISTINCT FROM (2000)))",
        )

    def test_build_update_change_predicate_multiple_assignments(self) -> None:
        statement = "UPDATE listings SET price = 2000, status = 'active' WHERE id = 1"
        self.assertEqual(
            _build_update_change_predicate(statement),
            "(price IS DISTINCT FROM (2000)) OR (status IS DISTINCT FROM ('active'))",
        )

    def test_execute_sql_statements_refuses_disallowed_sql(self) -> None:
        console = Console(record=True)

        execute_sql_statements(["DROP TABLE users"], console=console)

        output = console.export_text()
        self.assertIn("Refused", output)
        self.assertIn("DROP", output)
        self.assertNotIn("SQL statements to execute", output)

    @patch("queryquest.sql.executor.Prompt.ask", return_value="y")
    def test_execute_sql_statements_updates_non_first_sheet_and_saves(self, _mock_prompt) -> None:
        with TemporaryDirectory() as tmp_dir:
            excel_path = Path(tmp_dir) / "inventory.xlsx"

            current_df = pd.DataFrame({"Item": ["A", "B"], "Qty": [1, 2]})
            archive_df = pd.DataFrame({"Item": ["A", "B"], "Qty": [10, 20]})

            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                current_df.to_excel(writer, sheet_name="Current", index=False)
                archive_df.to_excel(writer, sheet_name="Archive Data", index=False)

            console = Console(record=True)
            execute_sql_statements(
                ["UPDATE inventory__Archive_Data SET Qty = 99 WHERE Item = 'A'"],
                console=console,
                excel_dir=tmp_dir,
            )

            reloaded = pd.read_excel(excel_path, sheet_name=None)
            self.assertEqual(int(reloaded["Archive Data"].loc[0, "Qty"]), 99)
            self.assertEqual(int(reloaded["Archive Data"].loc[1, "Qty"]), 20)
            self.assertEqual(int(reloaded["Current"].loc[0, "Qty"]), 1)
            self.assertEqual(int(reloaded["Current"].loc[1, "Qty"]), 2)

if __name__ == "__main__":
    unittest.main()
