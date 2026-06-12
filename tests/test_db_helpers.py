"""P2.04/P2.11 — pure-function DB helpers."""
from src import llmii_db


class TestNegationGuard:
    def test_negated_keywords_blocked(self):
        for kw in ("not nude", "no tattoos", "without piercings",
                   "non-nude", "non nude", "Not Nude"):
            assert llmii_db._is_negated_or_uncertain(kw), kw

    def test_uncertain_keywords_blocked(self):
        for kw in ("possibly nude", "appears to be topless",
                   "may have tattoos", "looks like lingerie"):
            assert llmii_db._is_negated_or_uncertain(kw), kw

    def test_plain_keywords_pass(self):
        for kw in ("nude", "fully nude", "shaved pussy", "tattoo on arm",
                   "nonchalant pose"):  # 'non' prefix inside a word is fine
            assert not llmii_db._is_negated_or_uncertain(kw), kw


class TestParseZipMetadata:
    def test_docstring_example(self):
        studio, performers = llmii_db.parse_zip_metadata(
            "Studio Name - 2023-01-15 Set Title (Alice, Bob) [12] [1280x960].zip")
        assert studio == "Studio Name"
        assert performers == ["Alice", "Bob"]

    def test_last_paren_block_wins(self):
        # Title contains its own parenthesised text before the performer block
        studio, performers = llmii_db.parse_zip_metadata(
            "Studio - Set (Behind The Scenes) (Alice, Bob).zip")
        assert performers == ["Alice", "Bob"]

    def test_no_performers(self):
        studio, performers = llmii_db.parse_zip_metadata("Just A Gallery Name.zip")
        assert studio is None
        assert performers == []

    def test_date_studio_rule(self):
        studio, performers = llmii_db.parse_zip_metadata(
            "Pornstar Platinum 2010-07-14 Erotic Reading (Charisma Cappelli) [17].zip")
        assert studio == "Pornstar Platinum"
        assert performers == ["Charisma Cappelli"]
