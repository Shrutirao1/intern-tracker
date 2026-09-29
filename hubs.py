"""Top US tech hubs and location matching, shared by the tracker and the scanners."""
import re

HUBS = {
    "SF Bay Area": ["san francisco", "south san francisco", "oakland", "berkeley", "emeryville",
                    "palo alto", "menlo park", "mountain view", "sunnyvale", "santa clara",
                    "san jose", "cupertino", "redwood city", "san mateo", "foster city",
                    "burlingame", "san carlos", "los altos", "los gatos", "milpitas", "fremont",
                    "pleasanton", "san ramon", "bay area", "silicon valley", "sf"],
    "Seattle": ["seattle", "bellevue", "redmond", "kirkland", "bothell", "issaquah", "renton"],
    "New York City": ["new york", "new york city", "nyc", "brooklyn", "manhattan", "queens",
                      "long island city", "jersey city", "hoboken"],
    "Chicago": ["chicago", "evanston", "naperville", "schaumburg", "oak brook"],
    "Texas": ["austin", "dallas", "houston", "san antonio", "plano", "irving", "frisco",
              "fort worth", "richardson", "round rock", "addison", "the woodlands"],
    "Boston": ["boston", "cambridge", "somerville", "waltham", "burlington", "lexington"],
    "Los Angeles": ["los angeles", "santa monica", "culver city", "playa vista", "irvine",
                    "pasadena", "el segundo", "hawthorne", "burbank", "venice"],
    "San Diego": ["san diego", "la jolla", "carlsbad"],
    "Washington DC": ["washington", "washington dc", "arlington", "reston", "mclean",
                      "herndon", "tysons", "bethesda", "alexandria", "chantilly"],
    "Denver / Boulder": ["denver", "boulder", "broomfield", "louisville", "englewood"],
    "Atlanta": ["atlanta", "alpharetta"],
    "Raleigh-Durham": ["raleigh", "durham", "research triangle", "cary", "morrisville"],
    "Pittsburgh": ["pittsburgh"],
    "Salt Lake City": ["salt lake city", "lehi", "provo", "draper", "south jordan"],
    "Portland": ["portland", "hillsboro", "beaverton"],
    "Phoenix": ["phoenix", "tempe", "scottsdale", "chandler"],
    "Minneapolis": ["minneapolis", "st. paul", "saint paul"],
    "Philadelphia": ["philadelphia"],
    "Miami": ["miami", "miami beach", "fort lauderdale", "boca raton"],
    "Detroit / Ann Arbor": ["detroit", "ann arbor", "dearborn", "southfield"],
    "Columbus": ["columbus", "dublin, oh"],
    "Nashville": ["nashville", "franklin"],
}
REMOTE_WORDS = ("remote",)


def build_city_index(hubs):
    return {c: h for h, cities in hubs.items() for c in cities}


HUB_STATES = {
    "SF Bay Area": {"CA"}, "Seattle": {"WA"}, "New York City": {"NY", "NJ"}, "Chicago": {"IL"},
    "Texas": {"TX"}, "Boston": {"MA"}, "Los Angeles": {"CA"}, "San Diego": {"CA"},
    "Washington DC": {"DC", "VA", "MD"}, "Denver / Boulder": {"CO"}, "Atlanta": {"GA"},
    "Raleigh-Durham": {"NC"}, "Pittsburgh": {"PA"}, "Salt Lake City": {"UT"}, "Portland": {"OR"},
    "Phoenix": {"AZ"}, "Minneapolis": {"MN"}, "Philadelphia": {"PA"}, "Miami": {"FL"},
    "Detroit / Ann Arbor": {"MI"}, "Columbus": {"OH"}, "Nashville": {"TN"},
}
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}
STATE_CODES = set(STATE_NAMES.values())


def _states(segment: str) -> set[str]:
    """US states mentioned in a location like 'Columbus, MS' or 'Cary, Illinois'."""
    found = {m for m in re.findall(r"\b([A-Z]{2})\b", segment) if m in STATE_CODES}
    low = segment.lower()
    for name, code in STATE_NAMES.items():
        if re.search(rf"\b{name}\b", low):
            found.add(code)
    if re.search(r"\bwashington,?\s*d\.?c\b", low):
        found.discard("WA")                              # "Washington, DC" is not Washington state
        found.add("DC")
    if "west virginia" in low and "virginia" not in low.replace("west virginia", ""):
        found.discard("VA")
    return found


def hubs_in(text: str, city_index: dict, include_remote: bool) -> set[str]:
    """Which tech hubs a location string is in. A city only counts if its state matches."""
    found = set()
    for seg in re.split(r"[;|/\n·]", text or ""):
        norm = re.sub(r"[-_]+", " ", seg)
        low = norm.lower()
        first = low.split(",")[0].strip()
        states = _states(norm)
        for city, hub in city_index.items():
            if hub in found:
                continue
            if (len(city) > 3 and re.search(rf"\b{re.escape(city)}\b", low)) or first == city:
                if states and not (states & HUB_STATES.get(hub, set())):
                    continue                             # e.g. Columbus, MS is not Columbus, OH
                found.add(hub)
    if include_remote and any(w in (text or "").lower() for w in REMOTE_WORDS):
        found.add("Remote (US)")
    return found


FOREIGN = ("canada", "united kingdom", " uk", "london", "india", "bangalore", "bengaluru",
           "hyderabad", "germany", "berlin", "munich", "france", "paris", "brazil", "mexico",
           "spain", "madrid", "japan", "tokyo", "china", "shanghai", "beijing", "singapore",
           "australia", "sydney", "poland", "ireland", "dublin", "netherlands", "amsterdam",
           "israel", "tel aviv", "emea", "apac", "latam", "europe", "toronto", "vancouver",
           "montreal", "waterloo", "taiwan", "korea", "vietnam", "malaysia", "philippines",
           "italy", "switzerland", "greece", "costa rica", "argentina", "colombia", "portugal",
           "england", "scotland", "wales", "hungary", "romania", "czech", "sweden", "norway",
           "denmark", "finland", "austria", "belgium", "turkey", "egypt", "nigeria", "kenya",
           "south africa", "uae", "dubai", "abu dhabi", "saudi", "qatar", "pakistan", "bangladesh",
           "sri lanka", "indonesia", "thailand", "hong kong", "new zealand", "chile", "peru",
           "uruguay", "ukraine", "serbia", "croatia", "bulgaria", "estonia", "lithuania", "latvia",
           "luxembourg", "slovakia", "slovenia", "cyprus", "malta", "iceland", "morocco", "ghana",
           "shenzhen", "hangzhou", "chengdu", "taipei", "hsinchu", "seoul", "osaka", "munich",
           "zurich", "geneva", "stockholm", "warsaw", "krakow", "prague", "budapest", "bucharest",
           "barcelona", "milan", "lisbon", "copenhagen", "oslo", "helsinki", "vienna", "brussels",
           "tel-aviv", "herzliya", "haifa", "gurgaon", "gurugram", "noida", "pune", "chennai",
           "mumbai", "delhi", "kuala lumpur", "penang", "manila", "jakarta", "ho chi minh", "hanoi",
           "montréal", "quebec", "ontario", "british columbia", "alberta", "calgary", "ottawa",
           "mississauga", "brampton", "edmonton", "winnipeg", "sao paulo", "são paulo", "mexico city",
           "guadalajara", "monterrey", "bogota", "bogotá", "buenos aires", "santiago, chile",
           "latin america")


def location_ok(location: str, city_index: dict) -> bool:
    """True if a posting is in a US tech hub, US-remote, or its location is unknown."""
    low = (location or "").lower().strip()
    if not low or re.fullmatch(r"\d+\s+locations?", low):
        return True                                          # unknown: don't risk missing it
    if hubs_in(low, city_index, False):
        return True
    if "remote" in low and not any(f in low for f in FOREIGN):
        return True
    return False


US_WORDS_RE = re.compile(r"\b(united states|usa|u\.s\.a?\.?|us|america)\b", re.I)


def us_ok(location: str, city_index: dict) -> bool:
    """True if a posting is anywhere in the US (or US-remote, or its location is unknown)."""
    text = (location or "").strip()
    low = text.lower()
    if not low or re.fullmatch(r"\d+\s+locations?", low):
        return True                                       # unknown: don't risk missing it
    if hubs_in(text, city_index, False):
        return True
    for seg in re.split(r"[;|\n·]", text):
        seg_low = seg.lower()
        if any(f.strip() in seg_low for f in FOREIGN):
            continue                                      # this location is outside the US
        if US_WORDS_RE.search(seg) or "remote" in seg_low:
            return True                                   # "United States", "USA", "Remote - US"
        if _states(re.sub(r"[-_]+", " ", seg)):
            return True                                   # "Des Moines, IA", "Columbus, MS"
    return not any(f.strip() in low for f in FOREIGN)     # no country info at all -> allow
