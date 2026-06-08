"""Tests that identifier rewriting handles multi-table JOIN reads.

Author: mohamedgamal04
"""

import unittest

from queryquest.sql.rewrite import (
    _normalize_single_quoted_table_identifiers,
    _rewrite_to_normalized_identifiers,
)


class RewriteJoinTests(unittest.TestCase):
    def test_rewrites_both_join_tables_via_alias_map(self) -> None:
        table_alias_map = {"orders": "orders__data", "customers": "customers__data"}
        statement = "SELECT * FROM orders o JOIN customers c ON o.id = c.order_id"

        result = _rewrite_to_normalized_identifiers(statement, {}, table_alias_map, {}, {})

        self.assertIn("FROM orders__data o", result)
        self.assertIn("JOIN customers__data c", result)

    def test_rewrites_table_name_map_for_join(self) -> None:
        table_name_map = {"Order List": "order_list__data", "Customer List": "customer_list__data"}
        statement = 'SELECT * FROM "Order List" JOIN "Customer List" ON 1 = 1'

        result = _rewrite_to_normalized_identifiers(statement, table_name_map, {}, {}, {})

        self.assertIn("FROM order_list__data", result)
        self.assertIn("JOIN customer_list__data", result)

    def test_single_quoted_table_after_join_is_double_quoted(self) -> None:
        statement = "SELECT * FROM orders JOIN 'Customer List' ON 1 = 1"

        result = _normalize_single_quoted_table_identifiers(statement, {"Customer List"})

        self.assertIn('JOIN "Customer List"', result)
        self.assertNotIn("'Customer List'", result)


if __name__ == "__main__":
    unittest.main()
