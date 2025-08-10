import re

def clean_invisible_spaces(text):
    """Remove invisible spaces like non-breaking space and zero-width spaces."""
    return re.sub(r'[\u00A0\u2000-\u200B\u202F\u205F\u3000]', ' ', text).strip()

PHRASES_PATTERN = re.compile(
    r'Jakarta, CNBC Indonesia|'
    r'IDXChannel—|KONTAN\.CO\.ID JAKARTA\.?|'
    r'Bisnis\.com , JAKARTA|Bisnis\.com, JAKARTA|'
    r'JAKARTA, investor\.id|Pasardana\.id|KONTAN\.CO\.ID|'
    r'IQPlus,{0,}\s{0,}\(\d+/\d+\)',
    re.IGNORECASE
)
LEADING_PUNCT_PATTERN = re.compile(r'^[^\w\s]+')
DASH_PATTERN = re.compile(r'\s*[-–—]\s*')
MULTISPACE_PATTERN = re.compile(r'\s+')

def clean_text(text):
    """Cleans news text by removing boilerplate phrases, leading punctuation,
    dashes, invisible spaces, and collapsing multiple spaces."""
    if not isinstance(text, str):
        return text
    text = clean_invisible_spaces(text)
    text = PHRASES_PATTERN.sub(' ', text).strip()
    text = LEADING_PUNCT_PATTERN.sub('', text)
    text = DASH_PATTERN.sub(' ', text)
    text = MULTISPACE_PATTERN.sub(' ', text)
    return text.strip()