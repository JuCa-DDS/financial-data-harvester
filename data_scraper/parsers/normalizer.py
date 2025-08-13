import re

_NUMBER_RE = re.compile(r'([-+]?\d+(?:\.\d+)?)')

def clean_numeric_with_suffix(value, percent_as='fraction'):
    if value is None:
        return None
    
    s = str(value).strip().replace('\u00A0', ' ')
    if not s or s in {'N/A', 'NA', '--', '-'}:
        return None

    negative = s.startswith('(') and s.endswith(')')
    if negative:
        s = s[1:-1].strip()
    
    s = s.replace(',', '').replace(' ', '')

    has_percent = s.endswith('%')
    if has_percent:
        s = s[:-1]

    m = _NUMBER_RE.search(s)
    if not m:
        return None
    number = float(m.group(1))

    suffix = s[-1].upper() if s and s[-1].isalpha() else ''
    suffix_map = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}
    multiplier = suffix_map.get(suffix, 1)

    out = number * multiplier

    if has_percent:
        if percent_as == 'fraction':
            out /= 100
    
    if negative:
        out = -out
        
    return out