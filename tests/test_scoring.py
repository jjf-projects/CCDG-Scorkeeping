"""
tests/test_scoring.py

Unit tests for the "Percentage + Marnie" points algorithm and the
cycle-totals / drops logic in ccdg_standings.py.

These tests require no database, no Google API, and no files — they
exercise pure Python logic only, so they run instantly.

Run with:
    pytest tests/test_scoring.py -v
"""

import pytest
from ccdg.ccdg_standings import (
    calc_points_for_period,
    _tally_cycle_totals,
    _build_avg_points_rows,
)

# Scoring constants matching ccdg_settings.py production values
SCORING = {
    "percentage_modifier":  120,
    "score_based_modifier": 30,
    "cycle_len":            12,
    "keep_periods":         6,
}

MAX_POINTS = SCORING["percentage_modifier"] + SCORING["score_based_modifier"]  # 150


# ---------------------------------------------------------------------------
# calc_points_for_period
# ---------------------------------------------------------------------------

class TestCalcPointsForPeriod:

    def test_single_player_gets_full_percentage_points(self):
        """One player in the field gets full percentage points (120).
        The score-based component (30) is 0 because there is no field range to
        measure margin against — this is correct behaviour, not a bug."""
        result = calc_points_for_period([["Alice", -3]], SCORING)
        assert result == [["Alice", SCORING["percentage_modifier"]]]

    def test_winner_scores_higher_than_loser(self):
        """Lower score (better in disc golf) should earn more points."""
        result = calc_points_for_period([["Alice", -5], ["Bob", 0]], SCORING)
        pts = {r[0]: r[1] for r in result}
        assert pts["Alice"] > pts["Bob"]

    def test_winner_gets_max_points(self):
        """The best score in the field always earns max points."""
        result = calc_points_for_period([["Alice", -5], ["Bob", 0]], SCORING)
        pts = {r[0]: r[1] for r in result}
        assert pts["Alice"] == MAX_POINTS

    def test_last_place_gets_above_zero(self):
        """Last place still earns some points — the percentage component is never zero."""
        result = calc_points_for_period([["Alice", -5], ["Bob", 5]], SCORING)
        pts = {r[0]: r[1] for r in result}
        assert pts["Bob"] > 0

    def test_tied_players_get_equal_points(self):
        """Two players with identical scores must receive identical points."""
        result = calc_points_for_period([["Alice", -3], ["Bob", -3]], SCORING)
        assert result[0][1] == result[1][1]

    def test_three_way_tie_all_equal(self):
        """Three-way tie: all players must share the same points."""
        result = calc_points_for_period(
            [["Alice", 2], ["Bob", 2], ["Carol", 2]], SCORING
        )
        pts = [r[1] for r in result]
        assert pts[0] == pts[1] == pts[2]

    def test_tie_at_top_middle_player_gets_less(self):
        """Tied leaders share the top points; the sole last-place player gets less."""
        result = calc_points_for_period(
            [["Alice", -2], ["Bob", -2], ["Carol", 3]], SCORING
        )
        pts = {r[0]: r[1] for r in result}
        assert pts["Alice"] == pts["Bob"]
        assert pts["Carol"] < pts["Alice"]

    def test_none_score_excluded_from_results(self):
        """Players who didn't play (score=None) must not appear in results."""
        result = calc_points_for_period(
            [["Alice", -3], ["Bob", None]], SCORING
        )
        names = [r[0] for r in result]
        assert "Bob" not in names
        assert "Alice" in names

    def test_all_none_returns_empty(self):
        """No players played — result must be empty, not an error."""
        result = calc_points_for_period(
            [["Alice", None], ["Bob", None]], SCORING
        )
        assert result == []

    def test_empty_input_returns_empty(self):
        """Empty input list — result must be empty."""
        result = calc_points_for_period([], SCORING)
        assert result == []

    def test_all_tied_no_division_by_zero(self):
        """When everyone ties, score_range == 0.
        This guards against the divide-by-zero bug that existed in the old code."""
        result = calc_points_for_period(
            [["Alice", 0], ["Bob", 0], ["Carol", 0]], SCORING
        )
        assert len(result) == 3
        # All tied → all equal, and score-based component must be 0 (not an error)
        pts = [r[1] for r in result]
        assert pts[0] == pts[1] == pts[2]

    def test_points_total_never_exceeds_max(self):
        """No player should ever receive more than MAX_POINTS."""
        scores = [["P1", -10], ["P2", -5], ["P3", 0], ["P4", 5]]
        result = calc_points_for_period(scores, SCORING)
        for _, pts in result:
            assert pts <= MAX_POINTS

    def test_result_contains_all_players_who_played(self):
        """Every player with a non-None score must appear in the result."""
        scores = [["Alice", -2], ["Bob", 0], ["Carol", None], ["Dave", 1]]
        result = calc_points_for_period(scores, SCORING)
        result_names = {r[0] for r in result}
        assert result_names == {"Alice", "Bob", "Dave"}


# ---------------------------------------------------------------------------
# _tally_cycle_totals
# ---------------------------------------------------------------------------

class TestTallyCycleTotals:
    """
    _tally_cycle_totals(season_points, cycle, cycle_len, keep_periods)
    Returns: {"points_total": float, "points_after_drops": float}
    """

    def test_fewer_than_keep_periods_no_drops(self):
        """Player played 3 of 12 weeks — all 3 count, no drops applied."""
        pts = [100, 90, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        result = _tally_cycle_totals(pts, cycle=1, cycle_len=12, keep_periods=6)
        assert result["points_total"] == 270
        assert result["points_after_drops"] == 270

    def test_exactly_keep_periods_no_drops(self):
        """Player played exactly 6 of 12 weeks — all count, drops don't remove anything."""
        pts = [100, 90, 80, 70, 60, 50, 0, 0, 0, 0, 0, 0]
        result = _tally_cycle_totals(pts, cycle=1, cycle_len=12, keep_periods=6)
        assert result["points_total"] == 450
        assert result["points_after_drops"] == 450

    def test_drops_keeps_best_six(self):
        """Player played all 12 weeks — only the best 6 count after drops."""
        # Best 6: 120+110+100+90+80+70 = 570
        pts = [120, 110, 100, 90, 80, 70, 60, 50, 40, 30, 20, 10]
        result = _tally_cycle_totals(pts, cycle=1, cycle_len=12, keep_periods=6)
        assert result["points_total"] == sum(pts)
        assert result["points_after_drops"] == 570

    def test_drops_with_unordered_scores(self):
        """Drops should pick the best 6 regardless of week order."""
        # Mix low and high scores out of order
        pts = [50, 120, 30, 110, 20, 100, 10, 90, 5, 80, 3, 70]
        result = _tally_cycle_totals(pts, cycle=1, cycle_len=12, keep_periods=6)
        assert result["points_after_drops"] == 120 + 110 + 100 + 90 + 80 + 70

    def test_only_counts_current_cycle(self):
        """Scores from a prior cycle must not bleed into the current cycle total."""
        # Cycle 1 scores: all 0. Cycle 2 scores: all 100.
        season = [0] * 12 + [100] * 12
        result = _tally_cycle_totals(season, cycle=2, cycle_len=12, keep_periods=6)
        assert result["points_total"] == 1200
        assert result["points_after_drops"] == 600  # best 6 of 12 × 100

    def test_zero_scores_not_counted_in_drops(self):
        """Zero-point weeks (didn't play) should be ignored by drops — they're already 0."""
        pts = [100, 90, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        result = _tally_cycle_totals(pts, cycle=1, cycle_len=12, keep_periods=6)
        # Only 3 non-zero scores; best 6 of [100, 90, 80, 0×9] = 270 (no change)
        assert result["points_after_drops"] == 270

    def test_returns_rounded_values(self):
        """Totals should be rounded to 2 decimal places."""
        pts = [100.555, 100.555, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        result = _tally_cycle_totals(pts, cycle=1, cycle_len=12, keep_periods=6)
        assert result["points_total"] == round(sum(pts), 2)


# ---------------------------------------------------------------------------
# _build_avg_points_rows
# ---------------------------------------------------------------------------

class TestBuildAvgPointsRows:
    """
    Input row shape: [name, division, total_pts, pts_after_drops, p1_pts, p2_pts, ...]
    Output row shape: [name, division, avg_pts]
    """

    def _make_row(self, name, division, *period_pts):
        """Helper — builds a points_row in the format _build_avg_points_rows expects."""
        total = sum(period_pts)
        return [name, division, total, total] + list(period_pts)

    def test_basic_average(self):
        """Simple three-week average."""
        rows = [self._make_row("Alice", "Alpha", 100, 90, 80)]
        result = _build_avg_points_rows(rows)
        assert result[0] == ["Alice", "Alpha", round((100 + 90 + 80) / 3, 2)]

    def test_zero_weeks_excluded_from_average(self):
        """Zero-point weeks (didn't play) must not bring the average down."""
        rows = [self._make_row("Bob", "Bravo", 100, 90, 0, 0)]
        result = _build_avg_points_rows(rows)
        # Only 2 weeks played: avg = (100+90)/2 = 95.0
        assert result[0][2] == 95.0

    def test_player_who_never_played_gets_zero_avg(self):
        """A registered player who never played should get avg=0, not an error."""
        rows = [self._make_row("Carol", "Charlie", 0, 0, 0)]
        result = _build_avg_points_rows(rows)
        assert result[0][2] == 0

    def test_sorted_alphabetically(self):
        """Output must be sorted A→Z by player name."""
        rows = [
            self._make_row("Zara",  "Alpha", 100),
            self._make_row("Alice", "Alpha", 90),
            self._make_row("Mike",  "Alpha", 80),
        ]
        result = _build_avg_points_rows(rows)
        names = [r[0] for r in result]
        assert names == ["Alice", "Mike", "Zara"]

    def test_output_shape(self):
        """Each output row must have exactly three elements: name, division, avg."""
        rows = [self._make_row("Alice", "Alpha", 100, 90)]
        result = _build_avg_points_rows(rows)
        assert len(result[0]) == 3

    def test_multiple_players_correct_averages(self):
        """End-to-end: multiple players, mixed played/missed weeks, sorted output."""
        rows = [
            self._make_row("Zara",  "Bravo", 100, 0, 80),   # avg of 100,80 = 90.0
            self._make_row("Alice", "Alpha", 90, 90, 90),    # avg = 90.0
        ]
        result = _build_avg_points_rows(rows)
        result_dict = {r[0]: r[2] for r in result}
        assert result_dict["Zara"]  == 90.0
        assert result_dict["Alice"] == 90.0
        assert result[0][0] == "Alice"   # sorted first
