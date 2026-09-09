"""Single-slot database backup and restore."""

import copy
import unittest
from unittest.mock import patch

import database


class _DeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = {doc["_id"]: copy.deepcopy(doc) for doc in (docs or [])}

    def _match(self, filt: dict, doc: dict) -> bool:
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
        matches = self.find(filt, projection)
        return matches[0] if matches else None

    def delete_many(self, filt):
        filt = filt or {}
        to_delete = [key for key, doc in self.docs.items() if self._match(filt, doc)]
        for key in to_delete:
            del self.docs[key]
        return _DeleteResult(len(to_delete))

    def insert_many(self, docs):
        for doc in docs:
            copied = copy.deepcopy(doc)
            self.docs[copied["_id"]] = copied

    def replace_one(self, filt, replacement, upsert=False):
        existing = self.find_one(filt)
        copied = copy.deepcopy(replacement)
        if existing is None:
            if upsert:
                self.docs[copied["_id"]] = copied
            return
        del self.docs[existing["_id"]]
        self.docs[copied["_id"]] = copied


class BackupRestoreTestCase(unittest.TestCase):
    def setUp(self):
        self.users = FakeCollection([
            {"_id": 42, "balance": 500.0, "items": {"Rose 🌹": 3}},
            {"_id": "jackpot_pool", "amount": 80.0, "dodge_count": 2},
        ])
        self.giveaways = FakeCollection([
            {"_id": 111, "message_id": 111, "guild_id": 7, "prize_display": "Apple"},
        ])
        self.events = FakeCollection([
            {"_id": "hourly", "event_id": "hourly", "event_name": "Rain"},
        ])
        self.jump_state = FakeCollection([
            {"_id": 7, "jump_counter": 12, "repair_until": 0.0},
        ])
        self.backups = FakeCollection()
        self.backup_data = FakeCollection()

        patches = [
            patch.object(database, "_get_users_collection", return_value=self.users),
            patch.object(database, "_get_giveaways_collection", return_value=self.giveaways),
            patch.object(database, "_get_events_collection", return_value=self.events),
            patch.object(database, "_get_jump_state_collection", return_value=self.jump_state),
            patch.object(database, "_get_backups_collection", return_value=self.backups, create=True),
            patch.object(database, "_get_backup_data_collection", return_value=self.backup_data, create=True),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_backup_copies_every_live_collection(self):
        meta = database.create_database_backup(created_by=99, created_by_name="Admin")

        self.assertEqual(meta["created_by"], 99)
        self.assertEqual(meta["created_by_name"], "Admin")
        self.assertEqual(meta["counts"]["users"], 2)
        self.assertEqual(meta["counts"]["giveaways"], 1)
        self.assertEqual(meta["counts"]["events"], 1)
        self.assertEqual(meta["counts"]["jump_state"], 1)
        self.assertIsNone(meta.get("replaced_previous_at"))
        self.assertEqual(self.users.docs[42]["balance"], 500.0)

    def test_second_backup_overwrites_the_slot(self):
        first = database.create_database_backup(created_by=1, created_by_name="First")
        self.users.docs[42]["balance"] = 12.0
        self.users.docs[99] = {"_id": 99, "balance": 1.0}
        del self.users.docs["jackpot_pool"]

        second = database.create_database_backup(created_by=2, created_by_name="Second")

        self.assertEqual(second["created_by"], 2)
        self.assertEqual(second["replaced_previous_at"], first["created_at"])
        self.assertEqual(second["counts"]["users"], 2)
        self.assertEqual(len(self.backups.docs), 1)

        self.users.docs[42]["balance"] = 999.0
        database.restore_database_backup()
        self.assertEqual(self.users.docs[42]["balance"], 12.0)
        self.assertIn(99, self.users.docs)
        self.assertNotIn("jackpot_pool", self.users.docs)

    def test_restore_puts_mutated_live_data_back(self):
        database.create_database_backup(created_by=1, created_by_name="Admin")
        self.users.docs[42]["balance"] = 0.0
        self.users.docs[42]["items"] = {}
        self.giveaways.docs.clear()
        self.events.docs.clear()
        self.jump_state.docs[7]["jump_counter"] = 0

        restored = database.restore_database_backup()

        self.assertEqual(self.users.docs[42]["balance"], 500.0)
        self.assertEqual(self.users.docs[42]["items"], {"Rose 🌹": 3})
        self.assertEqual(self.users.docs["jackpot_pool"]["amount"], 80.0)
        self.assertEqual(self.giveaways.docs[111]["prize_display"], "Apple")
        self.assertEqual(self.events.docs["hourly"]["event_name"], "Rain")
        self.assertEqual(self.jump_state.docs[7]["jump_counter"], 12)
        self.assertEqual(restored["counts"]["users"], 2)
        self.assertEqual(len(self.backups.docs), 1)
        self.assertTrue(self.backup_data.docs)

    def test_restore_without_backup_raises(self):
        with self.assertRaises(ValueError):
            database.restore_database_backup()

    def test_backup_info_is_none_until_a_backup_exists(self):
        self.assertIsNone(database.get_database_backup_info())
        database.create_database_backup(created_by=5, created_by_name="Pat")
        info = database.get_database_backup_info()
        self.assertEqual(info["created_by"], 5)
        self.assertEqual(info["created_by_name"], "Pat")


if __name__ == "__main__":
    unittest.main()
