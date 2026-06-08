"""Tests for engine approval policies.

Author: mohamedgamal04
"""

import asyncio
import unittest
from pathlib import Path

from queryquest.core.models import WritebackTarget
from queryquest.core.policy import AutoApprovePolicy, DenyAllPolicy


def _target(path: str) -> WritebackTarget:
    return WritebackTarget(file_path=Path(path), sheet_name="Sheet1", table_name="t", affected_rows=1)


class AutoApprovePolicyTests(unittest.TestCase):
    def test_approves_execution(self) -> None:
        policy = AutoApprovePolicy("/data")
        self.assertTrue(asyncio.run(policy.approve_execution([])))

    def test_approves_writeback_inside_dir(self) -> None:
        policy = AutoApprovePolicy("/data")
        self.assertTrue(asyncio.run(policy.approve_writeback(_target("/data/listings.xlsx"))))

    def test_rejects_writeback_outside_dir(self) -> None:
        policy = AutoApprovePolicy("/data")
        self.assertFalse(asyncio.run(policy.approve_writeback(_target("/etc/passwd.xlsx"))))


class DenyAllPolicyTests(unittest.TestCase):
    def test_denies_everything(self) -> None:
        policy = DenyAllPolicy()
        self.assertFalse(asyncio.run(policy.approve_execution([])))
        self.assertFalse(asyncio.run(policy.approve_writeback(_target("/data/x.xlsx"))))


if __name__ == "__main__":
    unittest.main()
