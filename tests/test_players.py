"""
tests/test_players.py

Unit tests for player utility functions in ccdg_players.py.

These tests require no database, no Google API, and no files.

Run with:
    pytest tests/test_players.py -v
"""

from ccdg.ccdg_players import clean_player_name


class TestCleanPlayerName:
    """
    clean_player_name() normalises names so they match consistently between
    the UDisc export and the Player table in the database.
    """

    def test_strips_leading_trailing_whitespace(self):
        assert clean_player_name("  Jeff Smith  ") == "Jeff Smith"

    def test_title_cases_name(self):
        assert clean_player_name("jeff smith") == "Jeff Smith"

    def test_strips_at_symbol(self):
        """UDisc sometimes prefixes names with @."""
        assert clean_player_name("@JeffSmith") == "Jeffsmith"

    def test_strips_double_quotes(self):
        assert clean_player_name('"Jeff Smith"') == "Jeff Smith"

    def test_strips_single_quotes(self):
        assert clean_player_name("'Jeff Smith'") == "Jeff Smith"

    def test_combined_cleanup(self):
        """Real-world messy input from UDisc."""
        assert clean_player_name('  @"jeff smith"  ') == "Jeff Smith"

    def test_already_clean_unchanged(self):
        """A properly formatted name should pass through untouched."""
        assert clean_player_name("Jeff Smith") == "Jeff Smith"

    def test_single_word_name(self):
        assert clean_player_name("madonna") == "Madonna"

    def test_hyphenated_name(self):
        """Title-case should capitalise after a hyphen."""
        assert clean_player_name("mary-jane watson") == "Mary-Jane Watson"

    def test_all_caps_input(self):
        assert clean_player_name("JEFF SMITH") == "Jeff Smith"
