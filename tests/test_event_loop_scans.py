"""Background scans must not block the Discord event loop."""

import ast
import unittest
from pathlib import Path
from unittest.mock import patch

import database


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")


class CountingUsers:
    def __init__(self, docs: list[dict]):
        self.docs = list(docs)
        self.find_calls = 0
        self.find_one_calls = 0
        self.update_one_calls = 0

    def find(self, filt=None, projection=None):
        self.find_calls += 1
        return [dict(doc) for doc in self.docs]

    def find_one(self, filt=None, projection=None):
        self.find_one_calls += 1
        return None

    def update_one(self, *args, **kwargs):
        self.update_one_calls += 1
        return None


def _sleep_zero_inside_for(func: ast.AsyncFunctionDef) -> bool:
    for node in ast.walk(func):
        if not isinstance(node, ast.For):
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Await) or not isinstance(inner.value, ast.Call):
                continue
            call = inner.value
            func_node = call.func
            name = ""
            if isinstance(func_node, ast.Attribute) and func_node.attr == "sleep":
                name = "sleep"
            if name != "sleep" or not call.args:
                continue
            arg0 = call.args[0]
            if isinstance(arg0, ast.Constant) and arg0.value == 0:
                return True
    return False


def _main_functions() -> dict[str, ast.AsyncFunctionDef]:
    tree = ast.parse(MAIN)
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
    }


class TestBloomRankFromCount(unittest.TestCase):
    def test_maps_known_bloom_counts(self):
        self.assertEqual(database.bloom_rank_from_count(0), "PINE I")
        self.assertEqual(database.bloom_rank_from_count(1), "PINE II")
        self.assertEqual(database.bloom_rank_from_count(2), "PINE III")
        self.assertEqual(database.bloom_rank_from_count(9), "MAPLE I")
        self.assertEqual(database.bloom_rank_from_count(18), "REDWOOD")
        self.assertEqual(database.bloom_rank_from_count(99), "REDWOOD")


class TestGetAllUsersRanksUsesScan(unittest.TestCase):
    def test_ranks_come_from_bloom_count_without_per_user_queries(self):
        users = CountingUsers([
            {"_id": 1, "bloom_count": 18},
            {"_id": 2, "bloom_count": 0},
            {"_id": "jackpot", "bloom_count": 18},
        ])
        with patch.object(database, "_get_users_collection", return_value=users):
            ranks = database.get_all_users_ranks()

        self.assertEqual(ranks, [(1, "REDWOOD"), (2, "PINE I")])
        self.assertEqual(users.find_calls, 1)
        self.assertEqual(users.find_one_calls, 0)
        self.assertEqual(users.update_one_calls, 0)


class TestMainDoesNotBlockLoop(unittest.TestCase):
    def test_leaderboard_fetches_run_in_threads(self):
        self.assertTrue(
            "await asyncio.to_thread(get_all_users_total_items)" in MAIN,
            "plants leaderboard scan must run in a thread",
        )
        self.assertTrue(
            "await asyncio.to_thread(get_all_users_balance)" in MAIN,
            "money leaderboard scan must run in a thread",
        )
        self.assertTrue(
            "await asyncio.to_thread(get_all_users_ranks)" in MAIN,
            "ranks leaderboard scan must run in a thread",
        )

    def test_market_and_stock_scans_run_in_threads(self):
        threaded = MAIN.count("await asyncio.to_thread(calculate_available_shares")
        self.assertGreaterEqual(threaded, 2, "marketboard and /stocks scans must run in threads")

    def test_gardener_and_gpu_loops_yield_between_users(self):
        funcs = _main_functions()
        self.assertTrue(_sleep_zero_inside_for(funcs["gardener_background_task"]))
        self.assertTrue(_sleep_zero_inside_for(funcs["gpu_background_task"]))


if __name__ == "__main__":
    unittest.main()
