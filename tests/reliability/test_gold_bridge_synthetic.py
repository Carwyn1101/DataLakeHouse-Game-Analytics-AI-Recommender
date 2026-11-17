"""Synthetic contract checks for Gold bridges, allocation, and publication HOLD."""

import unittest
from unittest.mock import patch

from gold_bridge_synthetic import (
    BRIDGES,
    INT_MAX,
    ConflictError,
    Publication,
    SyntheticBridge,
    publication_status,
    r2_representatives,
    verify_fact_fks,
)


GENRE = "bridge_game_genre"


def desired(game, dimension, dimension_col="genre_sk"):
    return {"game_key": game, dimension_col: dimension}


def target(game, dimension, key, name=GENRE):
    dimension_col, bridge_col = BRIDGES[name]
    return {**desired(game, dimension, dimension_col), bridge_col: key}


class BridgePlanTests(unittest.TestCase):
    def setUp(self):
        self.bridge = SyntheticBridge(GENRE, known_fresh=True)

    def test_full_membership_map_for_each_bridge(self):
        expected_columns = {
            "bridge_game_developer": ("developer_sk", "game_dev_key"),
            "bridge_game_genre": ("genre_sk", "game_gen_key"),
            "bridge_game_publisher": ("publisher_sk", "game_pub_key"),
            "bridge_game_category": ("category_sk", "game_cate_key"),
            "bridge_game_tag": ("tag_sk", "game_tag_key"),
        }
        self.assertEqual(BRIDGES, expected_columns)
        for name, (dimension_col, _) in expected_columns.items():
            with self.subTest(bridge=name):
                bridge = SyntheticBridge(name, known_fresh=True)
                bridge.refresh([3, 2, 1], [
                    desired(3, 10, dimension_col),
                    desired(2, 15, dimension_col),
                    desired(1, 20, dimension_col),
                    desired(1, 10, dimension_col),
                ])
                plan = bridge.refresh([1, 2], [
                    desired(1, 30, dimension_col), desired(1, 20, dimension_col),
                ])
                self.assertEqual(plan.before, ((1, 10, 1), (1, 20, 2), (2, 15, 3), (3, 10, 4)))
                self.assertEqual(plan.deletes, ((1, 10, 1), (2, 15, 3)))
                self.assertEqual(plan.inserts, ((1, 30, 5),))
                self.assertEqual(plan.expected, ((1, 20, 2), (1, 30, 5), (3, 10, 4)))
                self.assertEqual(bridge.rows, plan.expected)

    def test_plan_freezes_inputs_and_handles_empty_insert_scope(self):
        self.bridge.refresh([1, 2], [desired(1, 10), desired(2, 20)])
        eligible, payload = [1], []
        plan = self.bridge.plan(eligible, payload)
        eligible.append(2)
        payload.append(desired(1, 20))
        self.bridge.reserve(plan)
        self.bridge.commit(plan)
        self.assertEqual(plan.eligible, frozenset({1}))
        self.assertEqual(plan.desired, ())
        self.assertEqual(plan.inserts, ())
        self.assertEqual(plan.deletes, ((1, 10, 1),))
        self.assertEqual(self.bridge.rows, ((2, 20, 2),))

    def test_deleting_the_highest_key_does_not_release_it(self):
        self.bridge.refresh([1, 2], [desired(1, 10), desired(2, 20)])
        self.bridge.refresh([2], [])
        self.assertEqual((self.bridge.rows, self.bridge.high_water), (((1, 10, 1),), 2))
        self.bridge.refresh([2], [desired(2, 30)])
        self.assertEqual(self.bridge.rows, ((1, 10, 1), (2, 30, 3)))

    def test_invalid_desired_pairs_fail_before_reservation(self):
        bad_payloads = (
            [desired(1, 10), desired(1, 10)],
            [desired(None, 10)],
            [desired(1, None)],
            [desired(1, True)],
            [desired(1, INT_MAX + 1)],
            [{"game_key": 1}],
            [desired(2, 10)],
        )
        for payload in bad_payloads:
            with self.subTest(payload=payload), patch.object(self.bridge, "reserve") as reserve:
                with self.assertRaises(ValueError):
                    self.bridge.refresh([1], payload)
                reserve.assert_not_called()
        with self.assertRaises(ValueError):
            self.bridge.refresh([None], [])
        self.assertEqual((self.bridge.rows, self.bridge.high_water), ((), 0))

    def test_invalid_existing_identity_is_rejected(self):
        bad_targets = (
            [target(1, 1, 1), target(1, 1, 1)],
            [target(1, 1, 1), target(1, 1, 2)],
            [target(1, 1, 1), target(2, 1, 1)],
            [target(1, None, 1)],
            [target(1, 1, None)],
            [target(1, 1, -1)],
        )
        for rows in bad_targets:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                SyntheticBridge(GENRE, rows=rows)

    def test_no_implicit_bootstrap_for_existing_rows_or_metadata(self):
        rows = [target(1, 1, 7)]
        for supplied in (None, 7, 99):
            with self.subTest(high_water=supplied):
                bridge = SyntheticBridge(GENRE, rows=rows, recorded_high_water=supplied)
                with self.assertRaises(ValueError):
                    bridge.refresh([1], [desired(1, 2)])
                self.assertEqual(bridge.rows, ((1, 1, 7),))
        with self.assertRaises(ValueError):
            SyntheticBridge(GENRE, known_fresh=True, rows=rows)

    def test_missing_corrupt_untrusted_and_regressed_high_water(self):
        self.bridge.refresh([1, 2], [desired(1, 10), desired(2, 20)])
        self.bridge.refresh([2], [])
        # A regressed high-water can still equal the live maximum.
        for observed in (None, "2", -1, False, INT_MAX + 1, 1, 3):
            with self.subTest(observed=observed):
                self.bridge.high_water = observed
                with self.assertRaises(ValueError):
                    self.bridge.refresh([1], [desired(1, 30)])
                self.assertEqual(self.bridge.rows, ((1, 10, 1),))
        self.bridge.high_water = 2
        self.bridge.refresh([2], [desired(2, 30)])
        self.assertEqual(self.bridge.rows, ((1, 10, 1), (2, 30, 3)))

    def test_allocation_overflow_fails_before_reservation(self):
        # Seed trusted synthetic history without generating billions of rows.
        with patch.multiple(self.bridge, high_water=INT_MAX, _trusted_high_water=INT_MAX):
            with patch.object(self.bridge, "reserve") as reserve:
                with self.assertRaisesRegex(ValueError, "overflow"):
                    self.bridge.refresh([1], [desired(1, 10)])
                reserve.assert_not_called()
            self.assertEqual(self.bridge.high_water, INT_MAX)
            self.assertEqual(self.bridge.rows, ())


class BridgeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.bridge = SyntheticBridge(GENRE, known_fresh=True)
        self.bridge.refresh([1], [desired(1, 10)])
        self.input = [desired(1, 20)]

    def test_failure_before_reservation_preserves_state(self):
        with patch.object(self.bridge, "reserve", side_effect=RuntimeError("reserve failed")):
            with self.assertRaisesRegex(RuntimeError, "reserve failed"):
                self.bridge.refresh([1], self.input)
        self.assertEqual((self.bridge.rows, self.bridge.high_water), (((1, 10, 1),), 1))

    def test_failure_after_reservation_leaves_gap_and_no_partial_rows(self):
        with patch.object(self.bridge, "commit", side_effect=RuntimeError("before commit")):
            with self.assertRaisesRegex(RuntimeError, "before commit"):
                self.bridge.refresh([1], self.input)
        self.assertEqual((self.bridge.rows, self.bridge.high_water), (((1, 10, 1),), 2))
        self.bridge.refresh([1], self.input)
        self.assertEqual(self.bridge.rows, ((1, 20, 3),))

    def test_logical_merge_failure_does_not_install_any_delta(self):
        with patch.object(self.bridge, "_apply_delta", side_effect=RuntimeError("merge failed")):
            with self.assertRaisesRegex(RuntimeError, "merge failed"):
                self.bridge.refresh([1], self.input)
        self.assertEqual((self.bridge.rows, self.bridge.high_water), (((1, 10, 1),), 2))
        self.bridge.refresh([1], self.input)
        self.assertEqual(self.bridge.rows, ((1, 20, 3),))

    def test_lost_acknowledgement_replays_without_new_keys_or_data_commit(self):
        commit = self.bridge.commit

        def commit_then_lose_ack(plan):
            commit(plan)
            raise RuntimeError("ack lost")

        with patch.object(self.bridge, "commit", side_effect=commit_then_lose_ack):
            with self.assertRaisesRegex(RuntimeError, "ack lost"):
                self.bridge.refresh([1], self.input)
        self.assertEqual((self.bridge.rows, self.bridge.high_water), (((1, 20, 2),), 2))
        with patch.object(self.bridge, "_apply_delta", wraps=self.bridge._apply_delta) as merge:
            retry = self.bridge.refresh([1], self.input)
            merge.assert_not_called()
        self.assertEqual(retry.expected, ((1, 20, 2),))
        self.assertEqual((retry.inserts, retry.deletes, self.bridge.high_water), ((), (), 2))

    def test_competing_writers_cannot_reserve_the_same_range(self):
        first = self.bridge.plan([1], self.input)
        second = self.bridge.plan([1], [desired(1, 30)])
        self.assertEqual(first.reserved_through, second.reserved_through)
        self.bridge.reserve(first)
        with self.assertRaises(ConflictError):
            self.bridge.reserve(second)
        with self.assertRaises(ConflictError):
            self.bridge.commit(second)
        self.bridge.commit(first)
        self.assertEqual(self.bridge.rows, ((1, 20, 2),))

    def test_stale_delete_cannot_replace_newer_committed_target(self):
        self.bridge.refresh([1], [desired(1, 10), desired(1, 20)])
        first = self.bridge.plan([1], [])
        second = self.bridge.plan([1], [desired(1, 20)])
        self.bridge.reserve(first)
        self.bridge.reserve(second)
        self.bridge.commit(second)
        with self.assertRaises(ConflictError):
            self.bridge.commit(first)
        self.assertEqual((self.bridge.rows, self.bridge.high_water), (((1, 20, 2),), 2))


class FactAndPublicationTests(unittest.TestCase):
    def setUp(self):
        self.bridge_rows = {
            name: [target(1, 20, 1, name), target(1, 10, 2, name), target(2, 10, 3, name)]
            for name in BRIDGES
        }
        self.facts = [
            {"game_key": 1, **{key: 2 for _, key in BRIDGES.values()}},
            {"game_key": 2, **{key: 3 for _, key in BRIDGES.values()}},
            {"game_key": 3, **{key: -1 for _, key in BRIDGES.values()}},
        ]

    def test_r2_add_and_remove_members(self):
        for name in BRIDGES:
            with self.subTest(bridge=name):
                old = target(1, 20, 40, name)
                other = target(1, 30, 41, name)
                smaller = target(1, 10, 42, name)
                self.assertEqual(r2_representatives(name, [other, old]), {1: 40})
                self.assertEqual(r2_representatives(name, [other, old, smaller]), {1: 42})
                self.assertEqual(r2_representatives(name, [old, smaller]), {1: 42})
                self.assertEqual(r2_representatives(name, [old]), {1: 40})
                self.assertEqual(r2_representatives(name, []), {})

    def test_all_five_fks_enforce_same_game_and_r2(self):
        verify_fact_fks([1, 2, 3], self.facts, self.bridge_rows)
        for name, (_, bridge_col) in BRIDGES.items():
            for invalid in (-1, 1, 3, 99, None):
                with self.subTest(bridge=name, fk=invalid), self.assertRaises(ValueError):
                    bad = {**self.facts[0], bridge_col: invalid}
                    verify_fact_fks([1, 2, 3], [bad, *self.facts[1:]], self.bridge_rows)

    def test_duplicate_targets_facts_and_incomplete_fact_coverage_fail(self):
        duplicate_target = {**self.bridge_rows, GENRE: [*self.bridge_rows[GENRE], target(1, 10, 2)]}
        with self.assertRaises(ValueError):
            verify_fact_fks([1, 2, 3], self.facts, duplicate_target)
        for facts in ([], self.facts[:-1], [*self.facts, self.facts[0]]):
            with self.subTest(facts=facts), self.assertRaises(ValueError):
                verify_fact_fks([1, 2, 3], facts, self.bridge_rows)

    def test_publication_holds_until_verified_same_batch(self):
        complete = dict.fromkeys(BRIDGES, "batch-1")
        cases = (
            ({}, "batch-1", True, self.facts, Publication.HOLD_BRIDGES),
            ({**complete, GENRE: "old"}, "batch-1", True, self.facts, Publication.HOLD_BRIDGES),
            (complete, None, None, self.facts, Publication.HOLD_FACT_PENDING),
            (complete, "old", True, self.facts, Publication.HOLD_FACT_PENDING),
            (complete, "batch-1", False, self.facts, Publication.HOLD_FACT_FAILED),
            (complete, "batch-1", True, [], Publication.HOLD_FKS),
            (complete, "batch-1", True, self.facts, Publication.ELIGIBLE),
        )
        for bridges, fact_batch, succeeded, facts, expected in cases:
            with self.subTest(expected=expected, fact_batch=fact_batch):
                self.assertIs(publication_status(
                    "batch-1", bridges, fact_batch, succeeded, [1, 2, 3], facts, self.bridge_rows,
                ), expected)


if __name__ == "__main__":
    unittest.main()
