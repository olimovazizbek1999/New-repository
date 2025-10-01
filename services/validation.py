import re
import pandas as pd
from typing import Dict, List, Tuple, Optional

def validate_email_format(email: str) -> bool:
    """Validate email format."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))

def validate_name(name: str) -> Tuple[bool, str]:
    """Validate and score name quality."""
    if not name or not name.strip():
        return False, "empty"

    name = name.strip()

    # Check for common patterns of bad names
    bad_patterns = [
        r'^[^a-zA-Z]',  # Starts with non-letter
        r'[0-9]{3,}',   # Contains 3+ digits
        r'^(test|sample|example|demo)',  # Test data
        r'[<>{}\[\]()=+@#$%^&*]',  # Special characters (brackets properly escaped)
    ]

    for pattern in bad_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return False, "invalid_pattern"

    # Length checks
    if len(name) < 2:
        return False, "too_short"
    if len(name) > 50:
        return False, "too_long"

    return True, "valid"

def validate_company_name(company: str) -> Tuple[bool, str, float]:
    """Validate company name and return confidence score.

    Handles edge cases:
    - Short uppercase names (IBM, SAP, 3M, HP, GE)
    - Names with numbers (3M, 7-Eleven)
    - Multi-dot domains (company.co.uk)
    """
    if not company or not company.strip():
        return False, "empty", 0.0

    company = company.strip()

    # Common invalid company indicators
    invalid_indicators = [
        "unknown", "n/a", "test", "example", "sample", "demo",
        "website", "homepage", "site", "domain"
    ]

    if company.lower() in invalid_indicators:
        return False, "generic", 0.0

    # Quality indicators
    confidence = 0.5  # Base confidence

    # Special case: Short uppercase acronyms (IBM, SAP, HP, GE, 3M, etc.)
    # These are valid company names despite being short
    if re.match(r'^[A-Z0-9]{2,4}$', company):
        # 2-4 character all-caps acronyms are likely valid company names
        confidence += 0.4
        return True, "valid_acronym", min(1.0, confidence)

    # Positive indicators
    if any(suffix in company.lower() for suffix in ["gmbh", "ag", "inc", "llc", "ltd", "corp", "co"]):
        confidence += 0.3

    if len(company.split()) >= 2:  # Multi-word company names are usually better
        confidence += 0.2

    if re.search(r'[A-Z][a-z]', company):  # Proper capitalization
        confidence += 0.1

    # Negative indicators (with exceptions)
    if len(company) < 3:
        # Already handled by acronym check above
        confidence -= 0.3

    # Allow names with some numbers (like 3M, 7-Eleven) but penalize too many
    if re.search(r'[0-9]{3,}', company):
        # Check if it's a phone number pattern (very bad)
        if re.search(r'\d{3,}[-\s]?\d{3,}', company):
            confidence -= 0.5  # Definitely not a company name
        else:
            confidence -= 0.2  # Just many numbers, slightly suspicious

    # Check for domain-like patterns but don't penalize legitimate multi-dot domains like co.uk
    # Only penalize if it has many dots AND doesn't look like a legitimate domain extension
    dot_count = company.count('.')
    if dot_count > 2:
        # Check if it's a legitimate multi-part domain (e.g., company.co.uk)
        # Legitimate patterns: ends with .co.uk, .com.au, etc.
        legitimate_multi_dot = re.search(r'\.(co\.(uk|za|nz|au|jp|kr)|com\.(au|br|mx|cn)|ac\.(uk|nz)|org\.(uk|au))$', company.lower())
        if not legitimate_multi_dot:
            confidence -= 0.4

    confidence = max(0.0, min(1.0, confidence))

    return confidence > 0.3, "valid" if confidence > 0.3 else "low_confidence", confidence

def validate_opener_for_metrics(opener: str) -> Tuple[bool, str, float]:
    """
    Validate opener using the SAME strict criteria as production.
    This ensures metrics reflect what actually gets shipped.

    Uses shared opener_rules module for consistency.
    """
    from services.opener_rules import validate_opener_rules

    # Don't use seen set for metrics (no duplicate checking needed)
    validated_opener, error = validate_opener_rules(opener, "", seen=None)

    # Convert result to metrics format
    if validated_opener:
        return True, "valid", 1.0
    else:
        return False, error, 0.0

def validate_chunk_results(df: pd.DataFrame) -> Dict[str, any]:
    """Validate entire chunk results and return quality metrics."""
    results = {
        "total_rows": len(df),
        "valid_names": 0,
        "valid_companies": 0,
        "valid_openers": 0,
        "quality_score": 0.0,
        "validation_errors": [],
        "confidence_scores": {
            "names": [],
            "companies": [],
            "openers": []
        }
    }

    for _, row in df.iterrows():
        # Validate names
        first_valid, _ = validate_name(row.get("First Name", ""))
        last_valid, _ = validate_name(row.get("Last Name", ""))
        if first_valid and last_valid:
            results["valid_names"] += 1

        # Validate company
        company_valid, _, company_conf = validate_company_name(row.get("Company", ""))
        if company_valid:
            results["valid_companies"] += 1
        results["confidence_scores"]["companies"].append(company_conf)

        # Validate opener using same criteria as production filtering
        opener_valid, _, opener_conf = validate_opener_for_metrics(row.get("Opener", ""))
        if opener_valid:
            results["valid_openers"] += 1
        results["confidence_scores"]["openers"].append(opener_conf)

    # Calculate overall quality score
    if results["total_rows"] > 0:
        name_score = results["valid_names"] / results["total_rows"]
        company_score = results["valid_companies"] / results["total_rows"]
        opener_score = results["valid_openers"] / results["total_rows"]
        results["quality_score"] = (name_score + company_score + opener_score) / 3

    return results

def suggest_improvements(validation_results: Dict) -> List[str]:
    """Suggest improvements based on validation results."""
    suggestions = []

    if validation_results["quality_score"] < 0.6:
        suggestions.append("Overall data quality is below acceptable threshold (60%)")

    name_ratio = validation_results["valid_names"] / max(1, validation_results["total_rows"])
    if name_ratio < 0.8:
        suggestions.append("Consider improving name cleaning algorithms - many names appear invalid")

    company_ratio = validation_results["valid_companies"] / max(1, validation_results["total_rows"])
    if company_ratio < 0.7:
        suggestions.append("Company extraction needs improvement - consider better website scraping or AI prompts")

    opener_ratio = validation_results["valid_openers"] / max(1, validation_results["total_rows"])
    if opener_ratio < 0.6:
        suggestions.append("Email opener quality is low - review AI prompts and website text quality")

    avg_company_conf = sum(validation_results["confidence_scores"]["companies"]) / max(1, len(validation_results["confidence_scores"]["companies"]))
    if avg_company_conf < 0.5:
        suggestions.append("Company name confidence is low - may need manual review")

    return suggestions


# ============================================================================
# Test Cases for Validation Functions
# ============================================================================

def test_validation_edge_cases():
    """Test validation functions with edge cases.

    Run with: python -c "from services.validation import test_validation_edge_cases; test_validation_edge_cases()"
    """
    print("Running validation edge case tests...\n")

    # Test 1: Short uppercase company names (should be VALID)
    short_companies = [
        ("IBM", True, "Short acronym IBM"),
        ("SAP", True, "Short acronym SAP"),
        ("3M", True, "Company with number 3M"),
        ("HP", True, "Short acronym HP"),
        ("GE", True, "Short acronym GE"),
        ("AWS", True, "Short acronym AWS"),
        ("IKEA", True, "4-letter acronym IKEA"),
    ]

    print("Test 1: Short Uppercase Company Names")
    print("-" * 60)
    for company, should_pass, description in short_companies:
        valid, reason, confidence = validate_company_name(company)
        status = "✅ PASS" if valid == should_pass else "❌ FAIL"
        print(f"{status} | {description:25} | Valid: {valid:5} | Confidence: {confidence:.2f} | Reason: {reason}")

    # Test 2: Companies with numbers (mixed results)
    numbered_companies = [
        ("7-Eleven", True, "Legitimate company with number"),
        ("3M Company", True, "3M with suffix"),
        ("123456789", False, "Just a phone number"),
        ("555-1234", False, "Phone number pattern"),
    ]

    print("\nTest 2: Companies with Numbers")
    print("-" * 60)
    for company, should_pass, description in numbered_companies:
        valid, reason, confidence = validate_company_name(company)
        status = "✅ PASS" if valid == should_pass else "❌ FAIL"
        print(f"{status} | {description:25} | Valid: {valid:5} | Confidence: {confidence:.2f} | Reason: {reason}")

    # Test 3: Multi-dot domains (should be VALID)
    multi_dot_domains = [
        ("company.co.uk", True, "UK domain"),
        ("example.com.au", True, "Australian domain"),
        ("test.org.uk", True, "UK organization"),
        ("something.foo.bar.baz", False, "Too many arbitrary dots"),
    ]

    print("\nTest 3: Multi-dot Domains")
    print("-" * 60)
    for company, should_pass, description in multi_dot_domains:
        valid, reason, confidence = validate_company_name(company)
        status = "✅ PASS" if valid == should_pass else "❌ FAIL"
        print(f"{status} | {description:25} | Valid: {valid:5} | Confidence: {confidence:.2f} | Reason: {reason}")

    # Test 4: Edge cases
    edge_cases = [
        ("", False, "Empty string"),
        ("   ", False, "Whitespace only"),
        ("test", False, "Generic keyword"),
        ("unknown", False, "Generic keyword"),
        ("Microsoft Corporation", True, "Normal company name"),
        ("Acme Inc.", True, "Company with suffix"),
    ]

    print("\nTest 4: Edge Cases")
    print("-" * 60)
    for company, should_pass, description in edge_cases:
        valid, reason, confidence = validate_company_name(company)
        status = "✅ PASS" if valid == should_pass else "❌ FAIL"
        print(f"{status} | {description:25} | Valid: {valid:5} | Confidence: {confidence:.2f} | Reason: {reason}")

    print("\n" + "=" * 60)
    print("Test suite complete!")
    print("=" * 60)


if __name__ == "__main__":
    # Run tests if this file is executed directly
    test_validation_edge_cases()