"""P3.24-P3.29 — de_pluralize and JSON-repair robustness."""
import pytest

from src.llmii_utils import JsonFixError, de_pluralize, first_json, repair_json


class TestDePluralize:
    def test_pies_and_ties(self):
        # exact-word match: 'pies'/'ties' protected, but regular -ies
        # plurals still follow the 'ies$ -> y' rule
        assert de_pluralize("pies") == "pie"
        assert de_pluralize("ties") == "tie"
        assert de_pluralize("panties") == "panty"

    def test_regular_ies_plurals_not_corrupted(self):
        # suffix-matching 'pie'/'tie' corrupted these to partie/citie/puppie
        assert de_pluralize("parties") == "party"
        assert de_pluralize("cities") == "city"
        assert de_pluralize("puppies") == "puppy"
        assert de_pluralize("activities") == "activity"
        # long -ie entries still suffix/exact match correctly
        assert de_pluralize("cookies") == "cookie"
        assert de_pluralize("zombies") == "zombie"

    def test_lone_s_not_emptied(self):
        assert de_pluralize("s") == "s"

    def test_char_class_alternation(self):
        # '([octop|vir])i$' was a character class, not alternation
        assert de_pluralize("octopi") == "octopus"
        assert de_pluralize("spaghetti") == "spaghetti"
        assert de_pluralize("yeti") == "yeti"

    def test_regular_plurals_still_work(self):
        assert de_pluralize("boots") == "boot"
        assert de_pluralize("buses") == "bus"
        assert de_pluralize("dresses") == "dress"
        assert de_pluralize("dress") == "dress"  # double-s ending preserved
        assert de_pluralize("women") == "woman"

    def test_mutable_default_not_shared(self):
        de_pluralize("cats")
        assert de_pluralize("cats", None) == "cat"
        assert de_pluralize("cats", {"cats": "kitty"}) == "kitty"


class TestRepairJsonTruncated:
    @pytest.mark.parametrize("payload", [
        '{"a": "b"   ',
        '{   ',
        '{"a"',
        '{"a": ',
        '{"a":1,',
    ])
    def test_truncated_raises_jsonfixerror_not_indexerror(self, payload):
        with pytest.raises(JsonFixError):
            repair_json(payload)

    def test_valid_json_still_repairs(self):
        assert repair_json('{"a": 1}')

    def test_first_json_still_finds_object(self):
        text = 'some prose {"Keywords": ["a", "b"]} trailing'
        found = first_json(text)
        assert '"Keywords"' in found
