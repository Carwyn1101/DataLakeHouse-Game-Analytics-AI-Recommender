"""In-memory Gold bridge planning model. No catalog or storage connections."""

from dataclasses import dataclass
from enum import Enum


BRIDGES = {
    "bridge_game_developer": ("developer_sk", "game_dev_key"),
    "bridge_game_genre": ("genre_sk", "game_gen_key"),
    "bridge_game_publisher": ("publisher_sk", "game_pub_key"),
    "bridge_game_category": ("category_sk", "game_cate_key"),
    "bridge_game_tag": ("tag_sk", "game_tag_key"),
}
INT_MAX = (1 << 31) - 1


class ConflictError(RuntimeError):
    """The synthetic writer must replan against the current state."""


def _key(value):
    if type(value) is not int or not 1 <= value <= INT_MAX:
        raise ValueError("Expected a positive signed 32-bit membership key.")
    return value


def _pair(row, dimension_col):
    return _key(row.get("game_key")), _key(row.get(dimension_col))


def _target_rows(name, rows):
    dimension_col, bridge_col = BRIDGES[name]
    membership_keys = {}
    used_keys = set()
    for row in rows:
        pair = _pair(row, dimension_col)
        key = _key(row.get(bridge_col))
        if pair in membership_keys or key in used_keys:
            raise ValueError("Duplicate bridge pair or surrogate key.")
        membership_keys[pair] = key
        used_keys.add(key)
    return tuple(sorted((game, dimension, key) for (game, dimension), key in membership_keys.items()))


@dataclass(frozen=True)
class RefreshPlan:
    owner: object
    revision: int
    high_water: int
    eligible: frozenset
    desired: tuple
    before: tuple
    deletes: tuple
    inserts: tuple
    expected: tuple
    reserved_through: int


class SyntheticBridge:
    """A single bridge with separate, in-memory reservation and data commits."""

    def __init__(self, name, *, known_fresh=False, rows=(), recorded_high_water=None):
        self.name = name
        self._rows = _target_rows(name, rows)
        if known_fresh and (self._rows or recorded_high_water is not None):
            raise ValueError("Only an empty, fresh synthetic identity may initialize.")
        self.high_water = 0 if known_fresh else recorded_high_water
        # The test double has a trusted witness independent of the observed value.
        self._trusted_high_water = 0 if known_fresh else None
        self._identity = object()
        self._revision = 0
        self._reservation = None

    @property
    def rows(self):
        return self._rows

    def _read_high_water(self):
        high_water = self.high_water
        if type(high_water) is not int or not 0 <= high_water <= INT_MAX:
            raise ValueError("Missing or corrupt synthetic high-water metadata.")
        if high_water != self._trusted_high_water:
            raise ValueError("Untrusted or regressed synthetic high-water metadata.")
        return high_water

    def plan(self, eligible, desired):
        high_water = self._read_high_water()
        scope = frozenset(_key(game) for game in eligible)
        dimension_col, _ = BRIDGES[self.name]
        requested = [_pair(row, dimension_col) for row in desired]
        pairs = set(requested)
        if len(pairs) != len(requested):
            raise ValueError("Duplicate desired membership.")
        if any(game not in scope for game, _ in pairs):
            raise ValueError("Desired membership is outside the eligible scope.")

        existing = {(game, dimension): key for game, dimension, key in self.rows}
        new_pairs = sorted(pairs - existing.keys())
        reserved_through = high_water + len(new_pairs)
        if reserved_through > INT_MAX:
            raise ValueError("Bridge surrogate key allocation would overflow.")
        inserts = tuple(
            (game, dimension, key)
            for key, (game, dimension) in enumerate(new_pairs, start=high_water + 1)
        )
        deletes = tuple(
            (game, dimension, key)
            for game, dimension, key in self.rows
            if game in scope and (game, dimension) not in pairs
        )
        expected = [
            (game, dimension, existing[game, dimension])
            for game, dimension in pairs
            if (game, dimension) in existing
        ]
        expected.extend(row for row in self.rows if row[0] not in scope)
        expected.extend(inserts)
        return RefreshPlan(
            self._identity, self._revision, high_water, scope, tuple(sorted(pairs)),
            self.rows, deletes, inserts, tuple(sorted(expected)), reserved_through,
        )

    def _check_current(self, plan, expected_high_water):
        if plan.owner is not self._identity or plan.revision != self._revision:
            raise ConflictError("Bridge plan was created for a different target state.")
        if self._read_high_water() != expected_high_water:
            raise ConflictError("High-water changed after planning.")

    def reserve(self, plan):
        self._check_current(plan, plan.high_water)
        if plan.inserts:
            self.high_water = self._trusted_high_water = plan.reserved_through
            self._reservation = plan

    def _apply_delta(self, plan):
        remaining = set(self.rows) - set(plan.deletes)
        return tuple(sorted(remaining.union(plan.inserts)))

    def commit(self, plan):
        self._check_current(plan, plan.reserved_through)
        if plan.inserts and self._reservation is not plan:
            raise ConflictError("No matching reservation for this bridge plan.")
        if plan.deletes or plan.inserts:
            updated = self._apply_delta(plan)
            if updated != plan.expected:
                raise ValueError("Logical bridge commit does not match the planned target.")
            self._rows = updated
            self._revision += 1
            self._reservation = None

    def refresh(self, eligible, desired):
        plan = self.plan(eligible, desired)
        self.reserve(plan)
        self.commit(plan)
        return plan


def r2_representatives(name, rows):
    representatives = {}
    # Rows are sorted by game, dimension SK, then bridge SK.
    for game, _, bridge_key in _target_rows(name, rows):
        representatives.setdefault(game, bridge_key)
    return representatives


def verify_fact_fks(expected_games, facts, bridge_rows):
    expected = {_key(game) for game in expected_games}
    seen = set()
    representatives = {
        bridge_col: r2_representatives(name, bridge_rows[name])
        for name, (_, bridge_col) in BRIDGES.items()
    }
    for fact in facts:
        game = _key(fact.get("game_key"))
        if game not in expected or game in seen:
            raise ValueError("Unexpected or duplicate synthetic fact game.")
        seen.add(game)
        for bridge_col, game_keys in representatives.items():
            actual = fact.get(bridge_col)
            if type(actual) is not int or actual != game_keys.get(game, -1):
                raise ValueError(f"Invalid R2 fact reference for {bridge_col}.")
    if seen != expected:
        raise ValueError("Synthetic fact refresh does not cover the expected games.")


class Publication(Enum):
    HOLD_BRIDGES = "HOLD: bridge batch incomplete"
    HOLD_FACT_PENDING = "HOLD: fact refresh pending"
    HOLD_FACT_FAILED = "HOLD: fact refresh failed"
    HOLD_FKS = "HOLD: FK verification failed"
    ELIGIBLE = "Eligible for a separate publication decision"


def publication_status(
    batch, bridge_batches, fact_batch, fact_succeeded, expected_games, facts, bridge_rows
):
    if not batch or any(bridge_batches.get(name) != batch for name in BRIDGES):
        return Publication.HOLD_BRIDGES
    if fact_batch != batch or fact_succeeded is None:
        return Publication.HOLD_FACT_PENDING
    if fact_succeeded is not True:
        return Publication.HOLD_FACT_FAILED
    try:
        verify_fact_fks(expected_games, facts, bridge_rows)
    except (KeyError, ValueError):
        return Publication.HOLD_FKS
    return Publication.ELIGIBLE
