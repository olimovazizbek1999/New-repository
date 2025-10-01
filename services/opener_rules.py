"""Shared opener validation rules used across the application.

This module provides the authoritative opener validation logic used by both
utils.validate_opener() and services.validation for consistent metrics.
"""
import re


def validate_opener_rules(opener: str, text: str = "", seen: set = None) -> tuple[str, str]:
    """
    AUTHORITATIVE opener validation - these are the rules that determine final output.

    Enhanced opener validation according to strict 2-sentence rules:
    - Exactly 2 sentences (10-18 words each)
    - Must use 'you'/'your' for direct address
    - No greetings, questions, or generic phrases
    - Duplicate detection across batch
    - UK spelling enforced (organisation, optimise, etc.)

    Args:
        opener: The opener text to validate
        text: Optional source text (unused, for compatibility)
        seen: Optional set of already-seen openers for duplicate detection

    Returns:
        Tuple of (validated_opener, error_message)
        - If valid: (opener, "")
        - If invalid: ("", error_description)
    """
    if not opener or opener.strip() == "":
        return "", "empty opener"

    # Clean and normalize
    opener = opener.strip()

    # Duplicate detection (if seen set is provided)
    if seen is not None and opener in seen:
        return "", "duplicate opener"

    # Split into sentences for validation
    sentences = [s.strip() for s in opener.replace('!', '.').split('.') if s.strip()]
    if len(sentences) != 2:
        return "", f"must be exactly 2 sentences (found {len(sentences)} sentences)"

    # Validate each sentence word count (10-18 words each)
    for i, sentence in enumerate(sentences):
        word_count = len(sentence.split())
        if word_count < 10:
            return "", f"sentence {i+1} too short ({word_count} words, need 10-18)"
        if word_count > 18:
            return "", f"sentence {i+1} too long ({word_count} words, need 10-18)"

    # Total word count check (20-36 words)
    total_words = len(opener.split())
    if total_words < 20:
        return "", f"too short ({total_words} words, need 20-36)"
    if total_words > 36:
        return "", f"too long ({total_words} words, need 20-36)"

    # Must use 'you' or 'your' to address website owner
    opener_lower = opener.lower()
    if not any(word in opener_lower for word in ['you ', 'your ', ' you', ' your', 'you.', 'your.', 'you!', 'your!']):
        return "", "missing direct address (must use 'you' or 'your')"

    # Check for professional tone (no questions, greetings)
    if any(word in opener_lower for word in ['hello', 'hi ', 'dear ', 'greetings', '?']):
        return "", "contains greeting or question (not allowed)"

    # Must reference specific content (not be too generic)
    generic_phrases = ['your company', 'your business', 'your website', 'looks great', 'impressive work']
    if any(phrase in opener_lower for phrase in generic_phrases):
        return "", "too generic (must reference specific content)"

    # Check for US spelling - enforce UK spelling
    us_spelling_patterns = [
        (r'\borganization\b', 'organisation'),
        (r'\borganizations\b', 'organisations'),
        (r'\boptimize\b', 'optimise'),
        (r'\boptimized\b', 'optimised'),
        (r'\boptimizing\b', 'optimising'),
        (r'\bcolor\b', 'colour'),
        (r'\bcolors\b', 'colours'),
        (r'\banalyze\b', 'analyse'),
        (r'\banalyzed\b', 'analysed'),
        (r'\banalyzing\b', 'analysing'),
        (r'\brealize\b', 'realise'),
        (r'\brealized\b', 'realised'),
        (r'\brealizing\b', 'realising'),
        (r'\bspecialize\b', 'specialise'),
        (r'\bspecialized\b', 'specialised'),
        (r'\bspecializing\b', 'specialising'),
    ]

    for us_pattern, uk_word in us_spelling_patterns:
        if re.search(us_pattern, opener_lower):
            return "", f"uses US spelling (should be '{uk_word}')"

    # Add to seen set if provided
    if seen is not None:
        seen.add(opener)

    return opener, ""
