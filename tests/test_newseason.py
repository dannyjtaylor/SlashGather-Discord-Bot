"""New season wipe: almanac, /stats, and achievements reset; invite awards stay."""

import copy
import unittest
from unittest.mock import patch

import database


class _UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class FakeUsersCollection:
    def __init__(self, docs: list[dict]):
        self.docs = {doc["_id"]: copy.deepcopy(doc) for doc in docs}

    def _child(self, obj, part):
        if isinstance(obj, dict):
            return obj.get(part)
        if isinstance(obj, list) and str(part).isdigit():
            idx = int(part)
            if 0 <= idx < len(obj):
                return obj[idx]
        return None

    def _nested(self, doc: dict, dotted: str):
        val = doc
        for part in dotted.split("."):
            val = self._child(val, part)
            if val is None:
                return None
        return val

    def _set_path(self, doc: dict, dotted: str, value):
        parts = dotted.split(".")
        cur = doc
        for part in parts[:-1]:
            nxt = self._child(cur, part)
            if nxt is None:
                nxt = {}
                if isinstance(cur, dict):
                    cur[part] = nxt
            cur = nxt
        if isinstance(cur, dict):
            cur[parts[-1]] = copy.deepcopy(value)

    def _match_id(self, cond, doc_id) -> bool:
        if not isinstance(cond, dict):
            return doc_id == cond
        if "$gt" in cond:
            try:
                if not (isinstance(doc_id, int) and not isinstance(doc_id, bool) and doc_id > cond["$gt"]):
                    return False
            except TypeError:
                return False
        if "$type" in cond:
            allowed = cond["$type"]
            if isinstance(doc_id, int) and not isinstance(doc_id, bool):
                ok = "int" in allowed or "long" in allowed
            else:
                ok = False
            if not ok:
                return False
        return True

    def _match(self, filt: dict, doc: dict) -> bool:
        for key, cond in filt.items():
            if key == "_id":
                if not self._match_id(cond, doc.get("_id")):
                    return False
                continue
            val = self._nested(doc, key)
            if isinstance(cond, dict):
                if "$exists" in cond:
                    exists = val is not None
                    if bool(cond["$exists"]) != exists:
                        return False
                    continue
                if "$gte" in cond:
                    try:
                        if val is None or not (val >= cond["$gte"]):
                            return False
                    except TypeError:
                        return False
                    continue
            elif val != cond:
                return False
        return True

    def find(self, filt=None, projection=None):
        filt = filt or {}
        return [copy.deepcopy(doc) for doc in self.docs.values() if self._match(filt, doc)]

    def find_one(self, filt=None, projection=None):
        matches = self.find(filt, projection)
        return matches[0] if matches else None

    def _apply_update(self, doc: dict, update: dict):
        if "$set" in update:
            for key, value in update["$set"].items():
                self._set_path(doc, key, value)
        if "$setOnInsert" in update:
            pass

    def update_one(self, filt, update, upsert=False):
        doc = self.find_one(filt)
        if doc is None:
            if not upsert:
                return _UpdateResult(0)
            new_id = filt.get("_id")
            doc = {"_id": new_id}
            if "$setOnInsert" in update:
                doc.update(copy.deepcopy(update["$setOnInsert"]))
            self.docs[new_id] = doc
        else:
            doc = self.docs[doc["_id"]]
        before = copy.deepcopy(doc)
        self._apply_update(doc, update)
        return _UpdateResult(0 if doc == before else 1)

    def update_many(self, filt, update):
        count = 0
        for doc in list(self.docs.values()):
            if not self._match(filt, doc):
                continue
            before = copy.deepcopy(doc)
            self._apply_update(doc, update)
            if doc != before:
                count += 1
        return _UpdateResult(count)

    def count_documents(self, filt=None):
        return len(self.find(filt))


STATS_ACHIEVEMENT_KEYS = (
    "gatherer",
    "coinflip_total",
    "coinflip_win_streak",
    "harvesting",
    "planter",
    "water_streak",
    "blooming",
    "russian_roulette",
    "slayer",
    "stealing",
    "almanac",
    "areas_unlocked",
    "slots",
    "jumping",
)


def _veteran_doc(user_id: int = 42) -> dict:
    return {
        "_id": user_id,
        "balance": 999_999.0,
        "items": {"Rose 🌹": 12},
        "ripeness_stats": {"ripe": 4},
        "almanac_entries": {"Rose 🌹||ripe": 1, "Apple 🍎||normal": 1},
        "gather_stats": {"total_items": 800, "categories": {"Flower": 400}, "items": {"Rose 🌹": 12}},
        "total_forage_count": 800,
        "bloom_cycle_plants": 120,
        "bloom_count": 3,
        "tree_rings": 40,
        "consecutive_water_days": 12,
        "water_count": 40,
        "battlepass_exp": 50_000,
        "battlepass_lv": 22,
        "seed_pods": 5,
        "hoe_enchantment": {"name": "Old Hoe", "rarity": "RARE"},
        "tractor_enchantment": {"name": "Old Tractor", "rarity": "RARE"},
        "shop_inventory": {"quavers_beat": 1, "the_world": 1},
        "achievements": {
            "gatherer": 8,
            "almanac": 7,
            "jumping": 4,
            "planter": 6,
            "hidden_achievements_discovered": 4,
            "hidden_achievements": {
                "maxed_out": True,
                "harvest_complete": True,
                "fully_stocked": True,
                "social_butterfly": True,
            },
        },
        "invite_stats": {
            "invites_created": 3,
            "total_joins": 25,
            "rewards_earned": 250_000.0,
            "invite_codes": ["abc"],
            "claimed_rewards": [1, 3, 20],
        },
        "total_jumps": 80,
        "dayboosts": {"jump_multi": ["9999999999"]},
    }


class TestNewSeasonResetsProgress(unittest.TestCase):
    def setUp(self):
        self.users = FakeUsersCollection([
            _veteran_doc(),
            {"_id": "jackpot_pool", "amount": 5000.0, "dodge_count": 9},
            {"_id": 0, "crypto_prices": {"RTC": 1.0, "TER": 1.0, "CNY": 1.0}},
        ])
        self.patcher = patch.object(database, "_get_users_collection", return_value=self.users)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_payload_zeros_every_stats_achievement_including_jumping(self):
        payload = database._new_season_set_payload()
        self.assertEqual(payload.get("almanac_entries"), {})
        self.assertEqual(payload.get("battlepass_exp"), 0)
        self.assertEqual(payload.get("battlepass_lv"), 0)
        self.assertEqual(payload.get("gather_stats", {}).get("total_items"), 0)
        for key in STATS_ACHIEVEMENT_KEYS:
            self.assertEqual(
                payload["achievements"].get(key),
                0,
                f"{key} should reset to 0 on a new season",
            )

    def test_start_new_season_clears_almanac_stats_and_achievements(self):
        database.start_new_season()
        doc = self.users.docs[42]

        self.assertEqual(doc.get("almanac_entries"), {})
        self.assertEqual(doc["gather_stats"]["total_items"], 0)
        self.assertEqual(doc.get("items"), {})
        self.assertEqual(doc.get("bloom_cycle_plants"), 0)
        self.assertEqual(doc.get("bloom_count"), 0)
        self.assertEqual(doc.get("battlepass_exp"), 0)
        self.assertEqual(doc.get("battlepass_lv"), 0)
        self.assertEqual(doc.get("seed_pods"), 0)
        self.assertIsNone(doc.get("hoe_enchantment"))
        self.assertEqual(doc.get("shop_inventory"), {})
        self.assertEqual(doc.get("consecutive_water_days"), 0)
        self.assertEqual(doc.get("total_jumps"), 0)

        ach = doc.get("achievements") or {}
        for key in STATS_ACHIEVEMENT_KEYS:
            self.assertEqual(int(ach.get(key, 0) or 0), 0, f"{key} should be 0")
        hidden = ach.get("hidden_achievements") or {}
        self.assertFalse(hidden.get("maxed_out"))
        self.assertFalse(hidden.get("harvest_complete"))
        self.assertFalse(hidden.get("fully_stocked"))

    def test_keeps_invite_awards_and_reapplies_consumable_effects(self):
        database.start_new_season()
        doc = self.users.docs[42]
        invites = doc["invite_stats"]
        self.assertEqual(invites["claimed_rewards"], [1, 3, 20])
        self.assertEqual(invites["total_joins"], 25)
        grants = database.invite_reward_reapply_grants([1, 3, 20])
        self.assertEqual(doc["tree_rings"], grants["tree_rings"])
        self.assertEqual(doc["balance"], database._get_default_balance() + grants["money"])
        hidden = (doc.get("achievements") or {}).get("hidden_achievements") or {}
        self.assertTrue(hidden.get("social_butterfly"))

    def test_dossier_looks_like_a_new_player_except_invites(self):
        database.start_new_season()
        dossier = database.get_user_dossier(42)
        self.assertEqual(dossier["almanac_entries"], {})
        self.assertEqual(dossier["gather_stats_total_items"], 0)
        self.assertEqual(dossier["battlepass_exp"], 0)
        self.assertEqual(dossier["battlepass_lv"], 0)
        self.assertEqual(int((dossier["achievements"] or {}).get("almanac", 0) or 0), 0)
        self.assertEqual(int((dossier["achievements"] or {}).get("jumping", 0) or 0), 0)
        self.assertEqual(dossier["invite_stats"]["claimed_rewards"], [1, 3, 20])


if __name__ == "__main__":
    unittest.main()
