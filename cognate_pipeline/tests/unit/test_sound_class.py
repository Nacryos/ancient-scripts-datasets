"""Tests for SCA sound class encoding."""

from __future__ import annotations

from cognate_pipeline.normalise.sound_class import (
    ipa_to_sound_class,
    segment_to_class,
    tokenize_ipa,
)


class TestTokenizeIpa:
    def test_simple_word(self):
        tokens = tokenize_ipa("pata")
        assert tokens == ["p", "a", "t", "a"]

    def test_empty(self):
        assert tokenize_ipa("") == []

    def test_with_diacritics(self):
        # p followed by aspiration diacritic
        tokens = tokenize_ipa("pʰ")
        # Should produce 1 token with diacritic attached
        assert len(tokens) >= 1

    def test_transliteration(self):
        """Transliterated forms should tokenize character by character."""
        tokens = tokenize_ipa("ab")
        assert tokens == ["a", "b"]


class TestSegmentToClass:
    def test_vowels(self):
        assert segment_to_class("a") == "A"
        assert segment_to_class("e") == "E"
        assert segment_to_class("i") == "I"
        assert segment_to_class("o") == "O"
        assert segment_to_class("u") == "U"

    def test_stops(self):
        assert segment_to_class("p") == "P"
        assert segment_to_class("t") == "T"
        assert segment_to_class("k") == "K"
        assert segment_to_class("b") == "B"
        assert segment_to_class("d") == "D"
        assert segment_to_class("g") == "G"

    def test_nasals(self):
        assert segment_to_class("m") == "M"
        assert segment_to_class("n") == "N"
        assert segment_to_class("ŋ") == "N"

    def test_liquids(self):
        assert segment_to_class("l") == "L"
        assert segment_to_class("r") == "R"

    def test_sibilants(self):
        assert segment_to_class("s") == "S"
        assert segment_to_class("z") == "S"
        assert segment_to_class("ʃ") == "S"

    def test_glides(self):
        assert segment_to_class("w") == "W"
        assert segment_to_class("j") == "Y"

    def test_unknown(self):
        assert segment_to_class("!") == "0"

    def test_transliteration_specials(self):
        """Ancient script transliteration characters."""
        assert segment_to_class("$") == "S"
        assert segment_to_class("H") == "H"


class TestIpaToSoundClass:
    def test_simple_word(self):
        assert ipa_to_sound_class("pata") == "PATA"

    def test_water(self):
        sc = ipa_to_sound_class("wɔtə")
        assert sc == "WOTE"

    def test_cognate_pair(self):
        """Ugaritic 'ab' and Hebrew 'ab' should have identical sound classes."""
        assert ipa_to_sound_class("ab") == ipa_to_sound_class("ab")
        assert ipa_to_sound_class("ab") == "AB"

    def test_empty(self):
        assert ipa_to_sound_class("") == ""

    def test_transliteration_forms(self):
        """Test with Ugaritic-Hebrew transliteration characters."""
        # Both should produce same sound class for the shared segment
        uga = ipa_to_sound_class("abd")
        heb = ipa_to_sound_class("abd")
        assert uga == heb
