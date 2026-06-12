"""Phase 3 — keyword pipeline pure-function regressions."""
from src.llmii import (
    FileProcessor,
    _coerce_keyword_list,
    clean_string,
    clean_tags,
    markdown_list_to_dict,
    normalize_keyword,
    split_on_internal_capital,
)


class PipelineConfig:
    normalize_keywords = True
    depluralize_keywords = True
    limit_word_count = True
    max_words_per_keyword = 2
    split_and_entries = True
    ban_prompt_words = True
    no_digits_start = True
    min_word_length = True
    latin_only = True
    tag_blacklist = []
    update_keywords = False


class StubMatcher:
    enabled = False

    def log_unmatched(self, kw):
        pass


def make_fp():
    fp = FileProcessor.__new__(FileProcessor)
    fp.config = PipelineConfig()
    fp.tag_matcher = StubMatcher()
    fp.tag_matcher_fallback = StubMatcher()
    fp.banned_words = []
    return fp


class TestSplitOnInternalCapital:
    def test_all_caps_not_mangled(self):
        assert split_on_internal_capital("BLONDE") == "BLONDE"
        assert split_on_internal_capital("LINGERIE") == "LINGERIE"

    def test_camel_case_still_splits(self):
        assert split_on_internal_capital("microService") == "micro Service"


class TestNormalizeKeyword:
    def test_hyphenated_single_char_part_kept(self):
        # t-shirt / v-neck used to be rejected by min_word_length
        assert normalize_keyword("t-shirt", []) == "t-shirt"
        assert normalize_keyword("v-neck", []) == "v-neck"

    def test_hyphenated_plural_depluralized(self):
        assert normalize_keyword("ice-creams", []) == "ice-cream"

    def test_and_split_no_double_depluralize(self):
        # 'buses' must not become 'bu' via double de_pluralize
        assert normalize_keyword("boots and buses", []) == "boot bus"

    def test_and_exception_idiom_untouched(self):
        assert normalize_keyword("facts and figures", []) == "facts and figures"
        assert normalize_keyword("salt and pepper", []) == "salt and pepper"

    def test_word_count_limit_strict_without_and(self):
        # max 2 words; the +1 allowance is only for and/or compounds
        assert normalize_keyword("big red shiny", []) is None
        assert normalize_keyword("red shiny", []) == "red shiny"


class TestCleanString:
    def test_smart_quotes_normalized(self):
        assert clean_string("a “nice” day.") == 'a "nice" day.'

    def test_newlines_become_spaces(self):
        assert "oneline" not in clean_string("line one\nline two.")
        assert "one line" in clean_string("line one\nline two.")


class TestMarkdownListToDict:
    def test_prose_with_numbers_not_a_list(self):
        assert markdown_list_to_dict("She looks about 25. The lighting is soft") is None

    def test_real_numbered_list_extracted(self):
        text = "1. first thing\n2. second thing"
        assert markdown_list_to_dict(text) == {"Keywords": ["first thing", "second thing"]}

    def test_bullet_list_extracted(self):
        assert markdown_list_to_dict("- alpha\n- beta")["Keywords"] == ["alpha", "beta"]


class TestCleanTags:
    def test_string_keywords_split_not_char_exploded(self):
        result = clean_tags({"Keywords": "beach, sunset, golden hour"})
        assert result["Keywords"] == ["beach", "sunset", "golden hour"]

    def test_mixed_types_do_not_crash(self):
        result = clean_tags({"Keywords": ["beach", None, 42, {"x": "y"}, "sunset"]})
        assert "beach" in result["Keywords"]
        assert "sunset" in result["Keywords"]
        assert all(isinstance(k, str) for k in result["Keywords"])

    def test_raw_list_still_works(self):
        assert clean_tags(["a", "b", "a"])["Keywords"] == ["a", "b"]


class TestCoerceKeywordList:
    def test_none(self):
        assert _coerce_keyword_list(None) == []

    def test_string_split(self):
        assert _coerce_keyword_list("a; b, c") == ["a", "b", "c"]

    def test_numbers_kept_bools_dropped(self):
        assert _coerce_keyword_list(["x", 3, True, None]) == ["x", "3"]


class TestProcessKeywords:
    def test_piercing_canonical_survives_no_matcher(self):
        fp = make_fp()
        result = fp.process_keywords({}, ["nipple piercing"])
        assert "Piercing - Nipple" in result

    def test_belly_ring_is_navel(self):
        fp = make_fp()
        assert "Piercing - Navel" in fp.process_keywords({}, ["belly ring"])

    def test_necklace_is_not_a_piercing(self):
        fp = make_fp()
        result = fp.process_keywords({}, ["necklace"])
        assert not any(r.startswith("Piercing") for r in result)
        assert "necklace" in result

    def test_nude_color_garments_not_tagged_nude(self):
        fp = make_fp()
        for kw in ("nude stockings", "nude heels", "nude lipstick"):
            result = fp.process_keywords({}, [kw])
            assert "Nude" not in result, kw

    def test_actual_nudity_still_tagged(self):
        fp = make_fp()
        assert "Nude" in fp.process_keywords({}, ["fully nude"])
        assert "Nude" in fp.process_keywords({}, ["nude woman"])

    def test_negated_keywords_dropped(self):
        fp = make_fp()
        assert fp.process_keywords({}, ["not nude", "without tattoos"]) == []

    def test_and_keywords_split_into_two(self):
        fp = make_fp()
        result = fp.process_keywords({}, ["stockings and heels"])
        assert "stocking" in result
        assert "heel" in result

    def test_caption_param_used_for_extraction(self):
        fp = make_fp()
        result = fp.process_keywords({}, [], caption="She is completely nude.")
        assert "Nude" in result

    def test_caption_without_subject_not_tagged(self):
        fp = make_fp()
        result = fp.process_keywords({}, [], caption="The wall is painted a nude shade.")
        assert "Nude" not in result

    def test_explicit_caption_overrides_stale_metadata(self):
        fp = make_fp()
        meta = {"MWG:Description": "She is completely nude."}
        # caption='' means: fresh image, model produced no caption — the
        # stale metadata caption must not leak tags in
        result = fp.process_keywords(meta, [], caption="")
        assert "Nude" not in result

    def test_blacklist_applies_on_fallback_path(self):
        fp = make_fp()
        fp.config.tag_blacklist = ["watermark"]
        result = fp.process_keywords({}, ["watermark"])
        assert result == []
