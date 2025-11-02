#!/usr/bin/env python3
"""
Test Episode Parsing - Verify the new episode parsing logic
"""
import re

# Copy the regex patterns from app.py
REGEX_PATTERNS = {
    'number': re.compile(r'(\d+)'),
    'episode': re.compile(r'(?:الحلقة|Episode)\s*(\d+(?:\.\d+)?)', re.IGNORECASE),
    'episode_merged': re.compile(r'(?:الحلقة|Episode)\s*(\d+)\s*(?:و|&|and)\s*(\d+)', re.IGNORECASE),
    'episode_special': re.compile(r'(?:الحلقة|Episode)\s*(?:الخاصة|Special)', re.IGNORECASE),
    'episode_decimal': re.compile(r'(\d+\.\d+)'),
}

def extract_number_from_text(text: str):
    """Simplified version"""
    if not text: return None
    m = REGEX_PATTERNS['number'].search(text)
    if m: return int(m.group(1))
    return None

def extract_episode_number(ep_title: str, ep_num_text: str):
    """Extract episode number from title/text, handling special cases."""
    # Check for merged episodes like "12 و 13" - skip these
    merged = REGEX_PATTERNS['episode_merged'].search(ep_title) or REGEX_PATTERNS['episode_merged'].search(ep_num_text)
    if merged:
        return None  # Skip merged episodes
    
    # Check for special episodes - skip these
    if REGEX_PATTERNS['episode_special'].search(ep_title) or REGEX_PATTERNS['episode_special'].search(ep_num_text):
        return None  # Skip special episodes
    
    # Check for decimal episode numbers like 1115.5
    decimal_match = REGEX_PATTERNS['episode_decimal'].search(ep_title) or REGEX_PATTERNS['episode_decimal'].search(ep_num_text)
    if decimal_match:
        try:
            return float(decimal_match.group(1))
        except ValueError:
            pass
    
    # Standard episode number extraction
    ep_num = REGEX_PATTERNS['episode'].search(ep_title) or REGEX_PATTERNS['episode'].search(ep_num_text)
    if ep_num:
        try:
            return float(ep_num.group(1))
        except ValueError:
            pass
    
    # Fallback to text extraction
    num = extract_number_from_text(ep_title) or extract_number_from_text(ep_num_text)
    return float(num) if num else None

# Test cases
test_cases = [
    # (title, text, expected_result, description)
    ("الحلقة 1", "Episode 1", 1.0, "Standard episode"),
    ("الحلقة 1115.5", "1115.5", 1115.5, "Decimal episode"),
    ("الحلقة 12 و 13", "12 and 13", None, "Merged episodes (should skip)"),
    ("الحلقة الخاصة", "Special Episode", None, "Special episode (should skip)"),
    ("Episode 5", "5", 5.0, "English format"),
    ("مسلسل Avatar: The Last Airbender الموسم الثاني الحلقة 12 و 13 مترجمة", "12 & 13", None, "Merged in description"),
    ("انمي Aishang Ta de Liyou الحلقة الخاصة مترجمة", "Special", None, "Special in Arabic"),
    ("الحلقة 100", "100", 100.0, "Triple digit episode"),
    ("Episode 25.5", "25.5", 25.5, "OVA/Half episode"),
]

print("=" * 80)
print("EPISODE PARSING TEST")
print("=" * 80)

passed = 0
failed = 0

for title, text, expected, description in test_cases:
    result = extract_episode_number(title, text)
    status = "✅ PASS" if result == expected else "❌ FAIL"
    
    if result == expected:
        passed += 1
    else:
        failed += 1
    
    print(f"\n{status} - {description}")
    print(f"  Input: '{title}' / '{text}'")
    print(f"  Expected: {expected}")
    print(f"  Got: {result}")

print("\n" + "=" * 80)
print(f"RESULTS: {passed} passed, {failed} failed")
print("=" * 80)

if failed == 0:
    print("✅ All tests passed!")
else:
    print(f"⚠️  {failed} test(s) failed")
