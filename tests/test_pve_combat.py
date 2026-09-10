"""PvE fight buttons: persist across restarts, survive failed defer, embed-only HP edits."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import database


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")


class _DeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class FakeEvents:
    def __init__(self, docs=None):
        self.docs = {doc["_id"]: copy.deepcopy(doc) for doc in (docs or [])}

    def _match(self, filt, doc):
        if not filt:
            return True
        for key, value in filt.items():
            if doc.get(key) != value:
                return False
        return True

    def find(self, filt=None, projection=None):
        filt = filt or {}
        return [copy.deepcopy(doc) for doc in self.docs.values() if self._match(filt, doc)]

    def find_one(self, filt=None, projection=None):
        matches = self.find(filt)
        return matches[0] if matches else None

    def replace_one(self, filt, replacement, upsert=False):
        existing = self.find_one(filt)
        copied = copy.deepcopy(replacement)
        if existing is None:
            if upsert:
                self.docs[copied["_id"]] = copied
            return
        del self.docs[existing["_id"]]
        self.docs[copied["_id"]] = copied

    def delete_one(self, filt):
        existing = self.find_one(filt)
        if existing is None:
            return _DeleteResult(0)
        del self.docs[existing["_id"]]
        return _DeleteResult(1)

    def delete_many(self, filt):
        to_delete = [key for key, doc in self.docs.items() if self._match(filt, doc)]
        for key in to_delete:
            del self.docs[key]
        return _DeleteResult(len(to_delete))


def _block(src: str, start: str, end: str) -> str:
    i = src.index(start)
    j = src.index(end, i + len(start))
    return src[i:j]


class CombatPersistTestCase(unittest.TestCase):
    def setUp(self):
        self.events = FakeEvents()
        patcher = patch.object(database, "_get_events_collection", return_value=self.events)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_pve_combat_id(self):
        self.assertEqual(database.combat_pve_id(42), "combat:pve:42")
        self.assertEqual(database.combat_boss_id(99), "combat:boss:99")

    def test_attackers_round_trip_int_keys(self):
        rows = database.combat_attackers_to_rows({7: 3, 8: 1})
        restored = database.combat_attackers_from_rows(rows)
        self.assertEqual(restored, {7: 3, 8: 1})
        from_strings = database.combat_attackers_from_rows(
            [{"user_id": "7", "damage": "3"}]
        )
        self.assertEqual(from_strings, {7: 3})

    def test_save_list_get_delete_wild_combat(self):
        doc = {
            "_id": database.combat_pve_id(100),
            "kind": "wild",
            "channel_id": 100,
            "message_id": 200,
            "hp": 4,
            "max_hp": 10,
            "attackers": database.combat_attackers_to_rows({5: 6}),
        }
        database.save_active_combat(doc)

        listed = database.list_active_combats()
        self.assertEqual(len(listed), 1)
        self.assertTrue(listed[0].get("combat"))
        self.assertEqual(listed[0]["hp"], 4)

        loaded = database.get_active_combat(database.combat_pve_id(100))
        self.assertEqual(loaded["message_id"], 200)

        database.delete_active_combat(database.combat_pve_id(100))
        self.assertIsNone(database.get_active_combat(database.combat_pve_id(100)))
        self.assertEqual(database.list_active_combats(), [])

    def test_delete_combats_for_channels_and_guilds(self):
        database.save_active_combat({"_id": database.combat_pve_id(1), "kind": "wild"})
        database.save_active_combat({"_id": database.combat_pve_id(2), "kind": "wild"})
        database.save_active_combat({"_id": database.combat_boss_id(9), "kind": "boss"})
        database.delete_active_combats_for_channels([1])
        database.delete_active_combats_for_guilds([9])
        ids = {doc["_id"] for doc in database.list_active_combats()}
        self.assertEqual(ids, {database.combat_pve_id(2)})


class CombatButtonSourceTestCase(unittest.TestCase):
    def test_progress_edits_do_not_resend_view(self):
        for cls in (
            "WildAnimalView",
            "BulletAntView",
            "BeeView",
            "BossView",
            "SansView",
            "ObsidianTowerView",
            "EnderDragonView",
        ):
            start = f"class {cls}"
            body = _block(MAIN, start, "\nclass " if cls != "EnderDragonView" else "\nasync def _ender_dragon_regen_loop")
            debounce = body[body.index("async def _debounced_update"):]
            debounce = debounce.split("    async def ")[0] if "    async def " in debounce[debounce.index("async def _debounced_update") + 10:] else debounce
            # Only the debounce method: from _debounced_update to the next method
            rest = body.split("async def _debounced_update", 1)[1]
            method = rest.split("\n    async def ", 1)[0].split("\n    def ", 1)[0]
            self.assertIn("target.edit(embed=", method, msg=cls)
            self.assertNotIn("view=self", method, msg=f"{cls} HP updates must not re-attach the view")

    def test_attack_continues_when_defer_fails(self):
        for cls, method in (
            ("WildAnimalView", "async def attack"),
            ("BulletAntView", "async def attack"),
            ("BeeView", "async def attack"),
            ("BossView", "async def attack"),
            ("SansView", "async def fight"),
            ("ObsidianTowerView", "async def attack"),
            ("EnderDragonView", "async def attack"),
        ):
            start = f"class {cls}"
            nxt = MAIN.find("\nclass ", MAIN.find(start) + 1)
            body = MAIN[MAIN.find(start):nxt]
            fn = body[body.index(method):]
            fn = fn.split("\n    async def ", 1)[0].split("\n    def ", 1)[0] if "async def " in fn[10:] else fn
            self.assertIn("safe_defer", fn, msg=cls)
            self.assertNotIn(
                "if not await safe_defer(interaction, ephemeral=False):\n            return",
                fn,
                msg=f"{cls} must apply the hit even when defer fails",
            )

    def test_on_ready_restores_persisted_combats(self):
        ready = _block(MAIN, "async def on_ready", "async def on_member_join")
        self.assertIn("_restore_persisted_combats", ready)

    def test_unstick_deletes_persisted_combats(self):
        clearer = _block(MAIN, "async def _clear_stuck_pve_state", '@bot.tree.command(name="unstick"')
        self.assertIn("delete_active_combats_for_channels", clearer)
        self.assertIn("delete_active_combats_for_guilds", clearer)


if __name__ == "__main__":
    unittest.main()
