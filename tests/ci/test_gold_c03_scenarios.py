"""Offline tests for the native Gold fixture assertions."""

import unittest

import gold_c03_scenarios as gold


class GoldEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.before = gold.State((
            gold.gold_row(2, 101, "Prior"),
            gold.gold_row(7, 101, "Winner"),
            gold.gold_row(7, 101, "Winner"),
            gold.gold_row(20, 909, "Target only"),
        ), ((1, None, "append"),))
        self.changed = {101: (7, "Updated")}
        self.inserted = {303: (21, "New")}
        self.after = gold.State((
            gold.gold_row(7, 101, "Updated"),
            gold.gold_row(20, 909, "Target only"),
            gold.gold_row(21, 303, "New"),
        ), ((1, None, "append"), (2, 1, "overwrite")))

    def test_accepts_highest_key_replacement_and_one_child_snapshot(self):
        self.assertEqual(gold.assert_replacement(self.before, self.after, self.changed, self.inserted),
                         (2, 1, "overwrite"))

    def test_rejects_duplicate_survivor_remapped_key_or_missing_target_only_row(self):
        for rows in (
            (*self.after.rows, self.before.rows[0]),
            (gold.gold_row(2, 101, "Updated"), *self.after.rows[1:]),
            tuple(row for row in self.after.rows if row[1] != 909),
        ):
            with self.subTest(rows=rows), self.assertRaises(AssertionError):
                gold.assert_replacement(self.before, self.after._replace(rows=rows),
                                        self.changed, self.inserted)

    def test_rejects_missing_extra_or_unrelated_snapshot(self):
        for snapshots in (
            self.before.snapshots,
            (*self.before.snapshots, (2, 9, "overwrite")),
            (*self.after.snapshots, (3, 2, "overwrite")),
        ):
            with self.subTest(snapshots=snapshots), self.assertRaises(AssertionError):
                gold.one_snapshot(self.before, self.after._replace(snapshots=snapshots))


if __name__ == "__main__":
    unittest.main()
