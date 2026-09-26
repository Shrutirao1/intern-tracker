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


def hubs_in(text: str, city_index: dict, include_remote: bool) -> set[str]:
    found = set()
    low = (text or "").lower()
    for place in re.split(r"[;|/\n]", low):
        city = place.split(",")[0].strip()
        if city in city_index:
            found.add(city_index[city])
    for city, hub in city_index.items():
        if len(city) > 3 and hub not in found and re.search(rf"\b{re.escape(city)}\b", low):
            found.add(hub)
    if include_remote and any(w in low for w in REMOTE_WORDS):
        found.add("Remote (US)")
    return found



FOREIGN = ("canada", "united kingdom", " uk", "london", "india", "bangalore", "bengaluru",
           "hyderabad", "germany", "berlin", "munich", "france", "paris", "brazil", "mexico",
           "spain", "madrid", "japan", "tokyo", "china", "shanghai", "beijing", "singapore",
           "australia", "sydney", "poland", "ireland", "dublin", "netherlands", "amsterdam",
           "israel", "tel aviv", "emea", "apac", "latam", "europe", "toronto", "vancouver",
           "montreal", "waterloo", "taiwan", "korea", "vietnam", "malaysia", "philippines",
           "italy", "switzerland", "greece", "costa rica", "argentina", "colombia", "portugal")


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
