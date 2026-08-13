"""Unified New World Screwworm (NWS) case dataset.

Merges the three NWS sources scraped in :mod:`screwworm` into one schema:

* ``aphis_mx`` — USDA APHIS weekly status CSV (confirmed cases in Mexico)
* ``aphis_us`` — USDA APHIS Public Reporting Tableau dashboard (US cases)
* ``omsa``     — OMSA/WOAH regional Power BI report (foci across 9 countries)

The three sources report at different grains, so the merge keeps each source's
native grain and labels it:

* ``grain='focus'`` (OMSA) — one *outbreak*, which may cover many animals and
  several species. ``total_cases``/``total_susceptible`` carry the counts.
* ``grain='case'`` (APHIS) — one confirmed animal or fly-trap detection;
  ``total_cases`` is 1 and ``total_susceptible`` is null.

Two outputs share ``event_uid``:

* ``nws_unified_events`` — one row per event (wide), plus a point layer
  ``nws_unified_events_points``.
* ``nws_unified_event_species`` — one row per (event, species) with case and
  susceptible counts. OMSA's per-species columns melt into this table; APHIS
  rows contribute a single species row with ``case_count=1``.

**Overlap warning.** OMSA covers Mexico and the USA as well, so it overlaps both
APHIS sources. Nothing is dropped: rows carry ``source`` and a coarse
``duplicate_candidate`` flag (same country + admin1 + month reported by more than
one source). A naive ``SUM(total_cases)`` over the whole table therefore
double-counts Mexico and the US — filter to a single ``source`` first.
"""

import hashlib
import unicodedata
from datetime import datetime

import geopandas as gpd
import pandas as pd
import pytz
from dagster import (
    asset,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetIn,
    AssetKey,
    Output,
    RunRequest,
    define_asset_job,
    get_dagster_logger,
    schedule,
)

from resilient_core.utils import store_assets

from .screwworm import (
    NWS_DASHBOARD_VIEW,
    NWS_OMSA_VIEW,
    NWS_STATUS_PAGE,
    OMSA_MEASURE_COLS,
    SCREWWORM_LATEST_PATH,
    SCREWWORM_OUTPUT_PATH,
)

# ---------------------------------------------------------------------------
# Controlled vocabularies
# ---------------------------------------------------------------------------

# Species names used across all sources. "fly" covers fly-trap detections (an
# APHIS US case type that is not an infested animal); "multiple" is used as a
# focus-level species_primary when an OMSA outbreak spans more than one species.
SPECIES_VOCAB = [
    "bovine",
    "canine",
    "equine",
    "swine",
    "ovine",
    "caprine",
    "feline",
    "poultry",
    "buffalo",
    "wild_birds",
    "terrestrial_wildlife",
    "domestic_rabbit",
    "human",
    "fly",
    "other",
    "unknown",
]

# Free-text species values (APHIS, English/Spanish) -> vocabulary term. Keys are
# compared after _norm() (accents stripped, lowercased, whitespace collapsed).
SPECIES_ALIASES = {
    "bovine": "bovine",
    "bovino": "bovine",
    "cattle": "bovine",
    "cow": "bovine",
    "calf": "bovine",
    "canine": "canine",
    "canino": "canine",
    "dog": "canine",
    "perro": "canine",
    "equine": "equine",
    "equino": "equine",
    "horse": "equine",
    "caballo": "equine",
    "donkey": "equine",
    "mule": "equine",
    "swine": "swine",
    "porcino": "swine",
    "pig": "swine",
    "cerdo": "swine",
    "ovine": "ovine",
    "ovino": "ovine",
    "sheep": "ovine",
    "oveja": "ovine",
    "caprine": "caprine",
    "caprino": "caprine",
    "goat": "caprine",
    "cabra": "caprine",
    "feline": "feline",
    "felino": "feline",
    "cat": "feline",
    "gato": "feline",
    "poultry": "poultry",
    "aves de corral": "poultry",
    "chicken": "poultry",
    "buffalo": "buffalo",
    "bufalo": "buffalo",
    "water buffalo": "buffalo",
    "wild birds": "wild_birds",
    "aves silvestres": "wild_birds",
    "wildlife": "terrestrial_wildlife",
    "feral": "terrestrial_wildlife",
    "feral swine": "swine",
    "deer": "terrestrial_wildlife",
    "white-tailed deer": "terrestrial_wildlife",
    "white tailed deer": "terrestrial_wildlife",
    "venado": "terrestrial_wildlife",
    "terrestrial wildlife": "terrestrial_wildlife",
    "rabbit": "domestic_rabbit",
    "domestic rabbit": "domestic_rabbit",
    "conejo": "domestic_rabbit",
    "human": "human",
    "humano": "human",
    "fly": "fly",
    "flies": "fly",
    "fly trap": "fly",
    "mosca": "fly",
    "other": "other",
    "otro": "other",
    "unknown": "unknown",
    "desconocido": "unknown",
}

# APHIS qualifies some species inline — e.g. "Wildlife (American black bear)".
# Applied only when the exact alias lookup misses, in order, so a genuinely new
# species still falls through to "other" and trips the vocabulary check.
SPECIES_SUBSTRING_RULES = [
    ("fly", "fly"),
    ("mosca", "fly"),
    ("wildlife", "terrestrial_wildlife"),
    ("silvestre", "terrestrial_wildlife"),
    ("bear", "terrestrial_wildlife"),
    ("deer", "terrestrial_wildlife"),
    ("venado", "terrestrial_wildlife"),
    ("coyote", "terrestrial_wildlife"),
    ("javelina", "terrestrial_wildlife"),
    ("raccoon", "terrestrial_wildlife"),
    ("bird", "wild_birds"),
    ("aves", "wild_birds"),
    # "bovine" is checked before "ovine" — it contains it as a substring.
    ("feral swine", "swine"),
    ("bovine", "bovine"),
    ("canine", "canine"),
    ("equine", "equine"),
    ("swine", "swine"),
    ("ovine", "ovine"),
    ("caprine", "caprine"),
    ("feline", "feline"),
]

# OMSA measure columns are named cases_<species> / susceptible_<species>; derive
# the species list from them so a schema change surfaces here rather than in a
# hardcoded copy. total_* and focus_count are excluded by the prefix test.
OMSA_SPECIES_COLUMNS = [
    (col[len("cases_") :], f"susceptible_{col[len('cases_'):]}", col)
    for col in OMSA_MEASURE_COLS.values()
    if col.startswith("cases_")
]

# OMSA country label (normalized) -> ISO 3166-1 alpha-3.
COUNTRY_ISO3 = {
    "belice": "BLZ",
    "belize": "BLZ",
    "costa rica": "CRI",
    "el salvador": "SLV",
    "eua": "USA",
    "estados unidos": "USA",
    "usa": "USA",
    "united states": "USA",
    "guatemala": "GTM",
    "honduras": "HND",
    "mexico": "MEX",
    "nicaragua": "NIC",
    "panama": "PAN",
}

# Approximate geographic centroids of the Mexican states, used to place the
# APHIS Mexico weekly-status rows (which carry only a state name). These are
# rough state centers, NOT precise INEGI polygon centroids — rows placed with
# them carry geo_level='state_centroid' so consumers can tell them apart from
# true outbreak coordinates.
MX_STATE_CENTROIDS = {
    "aguascalientes": (21.96, -102.29),
    "baja california": (30.20, -115.20),
    "baja california sur": (25.50, -111.80),
    "campeche": (18.90, -90.40),
    "chiapas": (16.50, -92.50),
    "chihuahua": (28.60, -106.20),
    "ciudad de mexico": (19.40, -99.14),
    "coahuila": (27.30, -102.00),
    "colima": (19.15, -104.00),
    "durango": (24.80, -104.80),
    "guanajuato": (20.90, -101.10),
    "guerrero": (17.60, -99.80),
    "hidalgo": (20.50, -98.80),
    "jalisco": (20.50, -103.60),
    "mexico": (19.35, -99.70),
    "michoacan": (19.30, -101.90),
    "morelos": (18.75, -99.10),
    "nayarit": (21.75, -104.90),
    "nuevo leon": (25.60, -99.90),
    "oaxaca": (16.90, -96.60),
    "puebla": (19.00, -97.90),
    "queretaro": (20.80, -99.90),
    "quintana roo": (19.60, -88.10),
    "san luis potosi": (22.60, -100.40),
    "sinaloa": (25.00, -107.50),
    "sonora": (29.60, -110.50),
    "tabasco": (18.00, -92.60),
    "tamaulipas": (24.30, -98.60),
    "tlaxcala": (19.45, -98.20),
    "veracruz": (19.20, -96.40),
    "yucatan": (20.70, -89.00),
    "zacatecas": (23.30, -102.70),
}

# Long-form state names APHIS may publish -> the centroid dictionary key.
MX_STATE_ALIASES = {
    "coahuila de zaragoza": "coahuila",
    "distrito federal": "ciudad de mexico",
    "cdmx": "ciudad de mexico",
    "estado de mexico": "mexico",
    "michoacan de ocampo": "michoacan",
    "veracruz de ignacio de la llave": "veracruz",
    "queretaro de arteaga": "queretaro",
}

# Which source is authoritative for each country, best first. The deduplicated
# view keeps exactly one source per country so cases are counted once.
#
# The US goes to APHIS: it is the official national reporting and is per-case,
# where OMSA lags it. Mexico and Central America go to OMSA: it covers Mexico
# nationwide with true outbreak coordinates, whereas the APHIS Mexico CSV is
# scoped to the border states (it exists to track proximity to the US) and so
# holds a fraction of the country's cases.
#
# Later entries are fallbacks, used only when the preferred source produced no
# rows for that country in this run — one failed scrape should not silently
# erase a country from the deduplicated set.
COUNTRY_SOURCE_PRIORITY = {
    "USA": ["aphis_us", "omsa"],
    "MEX": ["omsa", "aphis_mx"],
}
DEFAULT_SOURCE_PRIORITY = ["omsa", "aphis_us", "aphis_mx"]

EVENT_COLUMNS = [
    "event_uid",
    "source",
    "grain",
    "detection_type",
    "event_date",
    "date_type",
    "country",
    "country_iso3",
    "admin1",
    "admin2",
    "locality",
    "latitude",
    "longitude",
    "geo_level",
    "total_cases",
    "total_susceptible",
    "species_primary",
    "status",
    "age",
    "animal_id",
    "case_type",
    "animal_type",
    "approx_miles_from_us",
    "sterile_insect_dispersal",
    "source_event_id",
    "overlap_key",
    "duplicate_candidate",
    "is_authoritative",
    "source_url",
    "retrieved_at",
]

SPECIES_COLUMNS = [
    "event_uid",
    "source",
    "country_iso3",
    "admin1",
    "admin2",
    "event_date",
    "species",
    "case_count",
    "susceptible_count",
]

COUNT_COLUMNS = ["total_cases", "total_susceptible"]


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _norm(value) -> str:
    """Lowercase, strip accents and footnote markers, collapse whitespace."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return " ".join(text.replace("*", "").strip().lower().split())


def _map_species(value, unmapped: set) -> str:
    """Map a free-text species value onto SPECIES_VOCAB, recording misses."""
    key = _norm(value)
    if not key:
        return "unknown"
    # APHIS mixes singular and plural ("Goat" and "Goats"), so try both.
    candidates = [key, key[:-1]] if key.endswith("s") else [key]
    for candidate in candidates:
        if candidate in SPECIES_ALIASES:
            return SPECIES_ALIASES[candidate]
        if candidate in SPECIES_VOCAB:
            return candidate
    for fragment, term in SPECIES_SUBSTRING_RULES:
        if fragment in key:
            return term
    unmapped.add(str(value))
    return "other"


def _country_iso3(value, fallback=None) -> str:
    """Map a country label to ISO3, falling back to the source abbreviation."""
    iso = COUNTRY_ISO3.get(_norm(value))
    if iso:
        return iso
    if fallback and len(str(fallback).strip()) == 3:
        return str(fallback).strip().upper()
    return "UNK"


def _clean_status(value):
    """Strip the footnote asterisks APHIS carries in its status column."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).replace("*", "").strip()
    return text or None


def _event_uids(frame: pd.DataFrame, source: str, id_column=None) -> pd.Series:
    """Build stable, unique event ids.

    Uses the source's own identifier when it has one; otherwise hashes the
    dimension tuple. Either way, rows that still collide (APHIS Mexico publishes
    genuinely identical rows for separate animals) get an occurrence suffix so
    ``event_uid`` stays a key.
    """
    if id_column is not None and id_column in frame.columns:
        base = frame[id_column].astype("string").fillna("")
    else:
        base = pd.Series([""] * len(frame), index=frame.index, dtype="string")

    dim_columns = [
        c
        for c in ["event_date", "country_iso3", "admin1", "admin2", "locality", "species_primary", "age"]
        if c in frame.columns
    ]
    digests = (
        frame[dim_columns]
        .astype("string")
        .fillna("")
        .agg("|".join, axis=1)
        .map(lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:12])
    )
    base = base.where(base.str.len() > 0, digests)

    uids = source + ":" + base
    occurrence = uids.groupby(uids).cumcount()
    return uids.where(occurrence == 0, uids + "#" + occurrence.astype("string"))


def _finalize(frame: pd.DataFrame) -> pd.DataFrame:
    """Add any missing schema columns and order them consistently."""
    for column in EVENT_COLUMNS:
        if column not in frame.columns:
            # Explicit object dtype: an all-NA column of unknown dtype makes the
            # later concat guess (and warn) about the merged column type.
            frame[column] = pd.Series(pd.NA, index=frame.index, dtype="object")
    return frame[EVENT_COLUMNS]


# ---------------------------------------------------------------------------
# Per-source normalizers: each returns (events, species_long, diagnostics)
# ---------------------------------------------------------------------------


def _normalize_omsa(focos: pd.DataFrame) -> tuple:
    """Normalize the OMSA focus line list (one row per outbreak)."""
    if focos is None or focos.empty:
        return _finalize(pd.DataFrame()), pd.DataFrame(columns=SPECIES_COLUMNS), {}

    src = focos.copy()
    events = pd.DataFrame(index=src.index)
    events["source"] = "omsa"
    events["grain"] = "focus"
    events["detection_type"] = "animal"
    events["event_date"] = src["start_date"]
    events["date_type"] = "focus_start"
    events["country"] = src["country"]
    events["country_iso3"] = [
        _country_iso3(c, a) for c, a in zip(src["country"], src.get("country_abbr", pd.Series(dtype=object)))
    ]
    events["admin1"] = src["province"]
    events["admin2"] = pd.NA
    events["locality"] = src["locality"]
    events["latitude"] = pd.to_numeric(src["latitude"], errors="coerce")
    events["longitude"] = pd.to_numeric(src["longitude"], errors="coerce")
    # OMSA reports true outbreak coordinates; 0/0 means "not geolocated".
    has_point = events["latitude"].notna() & events["longitude"].notna()
    has_point &= (events["latitude"] != 0) | (events["longitude"] != 0)
    events.loc[~has_point, ["latitude", "longitude"]] = pd.NA
    events["geo_level"] = pd.Series("point", index=src.index).where(has_point, "none")
    events["total_cases"] = pd.to_numeric(src["total_cases"], errors="coerce").astype("Int64")
    events["total_susceptible"] = pd.to_numeric(src["total_susceptible"], errors="coerce").astype("Int64")
    events["source_event_id"] = src["focus_id"].astype("string")
    events["source_url"] = NWS_OMSA_VIEW
    events["retrieved_at"] = src.get("retrieved_at", pd.NA)

    # Melt the per-species measure pairs into the long table, keeping only rows
    # that carry a count (an all-zero species is not an observation).
    species_frames = []
    for species, susceptible_col, cases_col in OMSA_SPECIES_COLUMNS:
        cases = pd.to_numeric(src.get(cases_col), errors="coerce").astype("Int64")
        susceptible = pd.to_numeric(src.get(susceptible_col), errors="coerce").astype("Int64")
        keep = cases.fillna(0).gt(0) | susceptible.fillna(0).gt(0)
        if not keep.any():
            continue
        species_frames.append(
            pd.DataFrame(
                {
                    "row": src.index[keep],
                    "species": species,
                    "case_count": cases[keep].values,
                    "susceptible_count": susceptible[keep].values,
                }
            )
        )

    if species_frames:
        long = pd.concat(species_frames, ignore_index=True)
    else:
        long = pd.DataFrame(columns=["row", "species", "case_count", "susceptible_count"])

    # species_primary: the single species with cases where there is exactly one,
    # otherwise "multiple" (or "unknown" for a focus with no confirmed cases).
    with_cases = long[long["case_count"].fillna(0) > 0]
    counts_per_row = with_cases.groupby("row")["species"].agg(["nunique", "first"])
    primary = pd.Series("unknown", index=src.index, dtype=object)
    if not counts_per_row.empty:
        single = counts_per_row[counts_per_row["nunique"] == 1]
        primary.loc[single.index] = single["first"]
        multiple = counts_per_row[counts_per_row["nunique"] > 1]
        primary.loc[multiple.index] = "multiple"
    events["species_primary"] = primary

    events["event_uid"] = _event_uids(events, "omsa", "source_event_id")

    if not long.empty:
        long = long.assign(
            event_uid=events["event_uid"].reindex(long["row"]).values,
            source="omsa",
            country_iso3=events["country_iso3"].reindex(long["row"]).values,
            admin1=events["admin1"].reindex(long["row"]).values,
            admin2=events["admin2"].reindex(long["row"]).values,
            event_date=events["event_date"].reindex(long["row"]).values,
        )[SPECIES_COLUMNS]
    else:
        long = pd.DataFrame(columns=SPECIES_COLUMNS)

    diagnostics = {"omsa_events": len(events), "omsa_species_rows": len(long)}
    return _finalize(events), long, diagnostics


def _normalize_aphis_us(cases: pd.DataFrame, county: pd.DataFrame) -> tuple:
    """Normalize the APHIS US line list, geocoded from the county summary."""
    if cases is None or cases.empty:
        return _finalize(pd.DataFrame()), pd.DataFrame(columns=SPECIES_COLUMNS), {}

    src = cases.copy()
    unmapped_species = set()

    events = pd.DataFrame(index=src.index)
    events["source"] = "aphis_us"
    events["grain"] = "case"
    events["event_date"] = src["confirmed_date"]
    events["date_type"] = "confirmed"
    events["country"] = "United States"
    events["country_iso3"] = "USA"
    events["admin1"] = src.get("state")
    events["admin2"] = src.get("county")
    events["locality"] = pd.NA
    events["case_type"] = src.get("case_type")
    events["animal_type"] = src.get("animal_type")
    events["animal_id"] = src.get("animal_id")
    events["status"] = src.get("status").map(_clean_status) if "status" in src else pd.NA

    # Fly-trap detections are surveillance hits, not infested animals; keep them
    # but let consumers filter on detection_type.
    type_text = (
        events["case_type"].map(_norm).fillna("") + " " + events["animal_type"].map(_norm).fillna("")
    )
    is_fly = type_text.str.contains("fly")
    events["detection_type"] = pd.Series("animal", index=src.index).where(~is_fly, "fly_trap")
    species = src.get("species", pd.Series(index=src.index, dtype=object)).map(
        lambda v: _map_species(v, unmapped_species)
    )
    events["species_primary"] = species.where(~is_fly, "fly")
    events["total_cases"] = pd.Series(1, index=src.index, dtype="Int64")
    events["total_susceptible"] = pd.Series(pd.NA, index=src.index, dtype="Int64")
    events["source_event_id"] = src.get("animal_id")
    events["source_url"] = NWS_DASHBOARD_VIEW
    events["retrieved_at"] = src.get("retrieved_at", pd.NA)

    # Geocode by joining the county summary's generated centroids.
    events["latitude"] = pd.NA
    events["longitude"] = pd.NA
    events["geo_level"] = "none"
    matched = 0
    if county is not None and not county.empty and {"state", "county"} <= set(county.columns):
        lookup = county.copy()
        lookup["_key"] = lookup["state"].map(_norm) + "|" + lookup["county"].map(_norm)
        lookup = lookup.drop_duplicates("_key").set_index("_key")
        keys = events["admin1"].map(_norm) + "|" + events["admin2"].map(_norm)
        events["latitude"] = pd.to_numeric(keys.map(lookup["latitude"]), errors="coerce")
        events["longitude"] = pd.to_numeric(keys.map(lookup["longitude"]), errors="coerce")
        has_point = events["latitude"].notna() & events["longitude"].notna()
        events["geo_level"] = pd.Series("county_centroid", index=src.index).where(has_point, "none")
        matched = int(has_point.sum())

    events["event_uid"] = _event_uids(events, "aphis_us", "source_event_id")

    long = pd.DataFrame(
        {
            "event_uid": events["event_uid"],
            "source": "aphis_us",
            "country_iso3": events["country_iso3"],
            "admin1": events["admin1"],
            "admin2": events["admin2"],
            "event_date": events["event_date"],
            "species": events["species_primary"],
            "case_count": pd.Series(1, index=src.index, dtype="Int64"),
            "susceptible_count": pd.Series(pd.NA, index=src.index, dtype="Int64"),
        }
    )[SPECIES_COLUMNS]

    diagnostics = {
        "aphis_us_events": len(events),
        "aphis_us_geocoded": matched,
        "aphis_us_unmapped_species": sorted(unmapped_species),
    }
    return _finalize(events), long, diagnostics


def _normalize_aphis_mx(weekly: pd.DataFrame) -> tuple:
    """Normalize the APHIS Mexico weekly status CSV (state-level geocoding)."""
    if weekly is None or weekly.empty:
        return _finalize(pd.DataFrame()), pd.DataFrame(columns=SPECIES_COLUMNS), {}

    src = weekly.copy()
    unmapped_species = set()

    events = pd.DataFrame(index=src.index)
    events["source"] = "aphis_mx"
    events["grain"] = "case"
    events["detection_type"] = "animal"
    events["event_date"] = src["report_date"]
    events["date_type"] = "report"
    events["country"] = "Mexico"
    events["country_iso3"] = "MEX"
    events["admin1"] = src.get("state")
    events["admin2"] = pd.NA
    events["locality"] = pd.NA
    events["species_primary"] = src.get("species", pd.Series(index=src.index, dtype=object)).map(
        lambda v: _map_species(v, unmapped_species)
    )
    events["age"] = src.get("age")
    events["status"] = src.get("status").map(_clean_status) if "status" in src else pd.NA
    events["sterile_insect_dispersal"] = src.get("sterile_insect_dispersal")
    events["approx_miles_from_us"] = pd.to_numeric(
        src.get("approx_miles_from_us"), errors="coerce"
    )
    events["total_cases"] = pd.Series(1, index=src.index, dtype="Int64")
    events["total_susceptible"] = pd.Series(pd.NA, index=src.index, dtype="Int64")
    events["source_url"] = NWS_STATUS_PAGE
    events["retrieved_at"] = src.get("retrieved_at", pd.NA)

    # Only a state name is published, so fall back to an approximate state
    # centroid — flagged as such in geo_level.
    unmapped_states = set()

    def _centroid(state):
        key = _norm(state)
        key = MX_STATE_ALIASES.get(key, key)
        if key in MX_STATE_CENTROIDS:
            return MX_STATE_CENTROIDS[key]
        if key:
            unmapped_states.add(str(state))
        return (None, None)

    coordinates = events["admin1"].map(_centroid)
    events["latitude"] = pd.to_numeric([c[0] for c in coordinates], errors="coerce")
    events["longitude"] = pd.to_numeric([c[1] for c in coordinates], errors="coerce")
    has_point = events["latitude"].notna()
    events["geo_level"] = pd.Series("state_centroid", index=src.index).where(has_point, "none")

    events["event_uid"] = _event_uids(events, "aphis_mx")

    long = pd.DataFrame(
        {
            "event_uid": events["event_uid"],
            "source": "aphis_mx",
            "country_iso3": events["country_iso3"],
            "admin1": events["admin1"],
            "admin2": events["admin2"],
            "event_date": events["event_date"],
            "species": events["species_primary"],
            "case_count": pd.Series(1, index=src.index, dtype="Int64"),
            "susceptible_count": pd.Series(pd.NA, index=src.index, dtype="Int64"),
        }
    )[SPECIES_COLUMNS]

    diagnostics = {
        "aphis_mx_events": len(events),
        "aphis_mx_geocoded": int(has_point.sum()),
        "aphis_mx_unmapped_species": sorted(unmapped_species),
        "aphis_mx_unmapped_states": sorted(unmapped_states),
    }
    return _finalize(events), long, diagnostics


def _flag_overlaps(events: pd.DataFrame) -> pd.DataFrame:
    """Flag events another source also reports for the same place and month.

    Deliberately coarse: the sources use different date semantics (focus start
    vs confirmation vs report date), so month-level agreement on country+admin1
    is the strongest claim the data supports. It marks candidates for review, it
    does not assert that two rows are the same event.
    """
    month = pd.to_datetime(events["event_date"], errors="coerce").dt.strftime("%Y-%m")
    events["overlap_key"] = (
        events["country_iso3"].astype("string").fillna("UNK")
        + "|"
        + events["admin1"].map(_norm)
        + "|"
        + month.fillna("")
    )
    sources_per_key = events.groupby("overlap_key")["source"].transform("nunique")
    events["duplicate_candidate"] = sources_per_key > 1
    return events


def _mark_authoritative(events: pd.DataFrame) -> tuple:
    """Pick one source per country and flag its rows as ``is_authoritative``.

    Returns ``(events, chosen)`` where ``chosen`` maps country_iso3 to the
    source that won it. Rows outside the winning source stay in the table — they
    are simply excluded from the deduplicated outputs.
    """
    available = events.groupby("country_iso3")["source"].agg(set)
    chosen = {}
    for country, sources in available.items():
        priority = COUNTRY_SOURCE_PRIORITY.get(country, DEFAULT_SOURCE_PRIORITY)
        # Fall back through the priority list, then to whatever is present, so
        # every country in the merged table survives into the deduplicated view.
        chosen[country] = next(
            (source for source in priority if source in sources), sorted(sources)[0]
        )
    events["is_authoritative"] = events["source"] == events["country_iso3"].map(chosen)
    return events, chosen


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------

UNIFIED_ASSET_KEY = AssetKey(["screwworm", "nws_unified_events"])

EVENTS_DESCRIPTION = (
    "Unified New World Screwworm event dataset merging USDA APHIS Mexico weekly "
    "status (source=aphis_mx), the APHIS US Public Reporting dashboard "
    "(source=aphis_us), and the OMSA/WOAH regional Power BI report (source=omsa). "
    "One row per reported event at each source's native grain: grain='focus' is an "
    "OMSA outbreak (total_cases/total_susceptible carry its counts), grain='case' is "
    "a single APHIS confirmed animal or fly-trap detection (total_cases=1). "
    "Coordinates are true outbreak points (geo_level='point'), county centroids "
    "(geo_level='county_centroid'), or approximate Mexican state centroids "
    "(geo_level='state_centroid'). OMSA also covers Mexico and the US, so its rows "
    "overlap the APHIS sources: nothing is de-duplicated and duplicate_candidate "
    "flags country+state+month reported by more than one source. Summing "
    "total_cases across all sources double-counts — filter to is_authoritative=true "
    "(or use the nws_unified_events_deduped dataset) to count each case once."
)
DEDUPED_DESCRIPTION = (
    "Deduplicated New World Screwworm events: nws_unified_events filtered to "
    "is_authoritative=true, which keeps exactly one source per country so every "
    "case is counted once. The USA comes from USDA APHIS (source=aphis_us, official "
    "per-case national reporting); Mexico and Central America come from OMSA/WOAH "
    "(source=omsa), which covers Mexico nationwide with true outbreak coordinates, "
    "whereas the APHIS Mexico CSV is scoped to the border states. Rows excluded here "
    "are not wrong — they are the same outbreaks as reported by a second publisher, "
    "and remain in nws_unified_events. total_cases is safe to sum across this "
    "dataset. Note the mixed grain: US rows are individual cases, all other rows are "
    "outbreak foci."
)
SPECIES_DESCRIPTION = (
    "Unified New World Screwworm case counts by species, keyed to "
    "nws_unified_events via event_uid: one row per (event, species) with "
    "case_count and susceptible_count. OMSA per-species outbreak columns are "
    "melted here (species with no cases and no susceptible animals are omitted); "
    "APHIS case rows contribute a single species row with case_count=1. Species "
    "use a controlled vocabulary shared across all three sources. The same "
    "cross-source overlap caveat as nws_unified_events applies — filter to "
    "is_authoritative=true, or use nws_unified_event_species_deduped."
)
DEDUPED_SPECIES_DESCRIPTION = (
    "Deduplicated New World Screwworm case counts by species: "
    "nws_unified_event_species restricted to the authoritative source for each "
    "country (APHIS for the USA, OMSA for Mexico and Central America), so case_count "
    "is safe to sum. Keyed to nws_unified_events_deduped via event_uid."
)


@asset(
    group_name="pathogens",
    key_prefix="screwworm",
    name="nws_unified_events",
    required_resource_keys={"s3", "slack"},
    ins={
        "omsa_focos": AssetIn(key=AssetKey(["screwworm", "nws_omsa_centroamerica"])),
        "us_dashboard": AssetIn(key=AssetKey(["screwworm", "nws_aphis_us"])),
        "mx_weekly": AssetIn(key=AssetKey(["screwworm", "nws_aphis_mx_weekly"])),
    },
    check_specs=[
        AssetCheckSpec(
            name="unified_species_vocab_covered",
            asset=UNIFIED_ASSET_KEY,
            description="Every source species value maps onto the controlled vocabulary.",
        ),
        AssetCheckSpec(
            name="unified_mx_states_geocoded",
            asset=UNIFIED_ASSET_KEY,
            description="Every APHIS Mexico state name resolves to a centroid.",
        ),
        AssetCheckSpec(
            name="unified_totals_reconcile",
            asset=UNIFIED_ASSET_KEY,
            description="Per-species case counts sum to each event's total_cases.",
        ),
        AssetCheckSpec(
            name="unified_required_fields",
            asset=UNIFIED_ASSET_KEY,
            description="event_uid is unique and the key dimensions are populated.",
        ),
        AssetCheckSpec(
            name="unified_dedupe_one_source_per_country",
            asset=UNIFIED_ASSET_KEY,
            description="The deduplicated view keeps one source per country and loses no country.",
        ),
    ],
)
def nws_unified_events(context, omsa_focos, us_dashboard, mx_weekly):
    """Merge the three NWS sources into one event dataset and one species table.

    Writes the full merged view (``nws_unified_events``,
    ``nws_unified_events_points``, ``nws_unified_event_species``) and the
    deduplicated view (the same three, ``*_deduped``), which keeps one
    authoritative source per country so cases are counted once. Then yields the
    data-quality checks over the merged frames.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    retrieved_at = datetime.now(pytz.timezone("America/New_York")).isoformat()

    us_cases = us_dashboard.get("cases") if isinstance(us_dashboard, dict) else None
    us_county = us_dashboard.get("county") if isinstance(us_dashboard, dict) else None

    omsa_events, omsa_species, omsa_diag = _normalize_omsa(omsa_focos)
    us_events, us_species, us_diag = _normalize_aphis_us(us_cases, us_county)
    mx_events, mx_species, mx_diag = _normalize_aphis_mx(mx_weekly)

    diagnostics = {**omsa_diag, **us_diag, **mx_diag}

    events = pd.concat([omsa_events, us_events, mx_events], ignore_index=True)
    species = pd.concat([omsa_species, us_species, mx_species], ignore_index=True)

    if events.empty:
        raise ValueError("No screwworm events produced; all three upstream sources were empty.")

    for column in COUNT_COLUMNS:
        events[column] = pd.to_numeric(events[column], errors="coerce").astype("Int64")
    for column in ["case_count", "susceptible_count"]:
        species[column] = pd.to_numeric(species[column], errors="coerce").astype("Int64")

    events = _flag_overlaps(events)
    events, authority = _mark_authoritative(events)
    events = events.sort_values(
        ["event_date", "source"], ascending=[False, True], na_position="last"
    ).reset_index(drop=True)
    events["ingested_at"] = retrieved_at
    # Carry the flag onto the species rows so either table can stand alone.
    species["is_authoritative"] = species["event_uid"].map(
        events.set_index("event_uid")["is_authoritative"]
    )
    species["ingested_at"] = retrieved_at

    logger.info(
        f"Unified NWS events: {len(events)} rows "
        f"({events['source'].value_counts().to_dict()}), "
        f"{len(species)} species rows, "
        f"{int(events['duplicate_candidate'].sum())} duplicate candidates, "
        f"latest event_date {events['event_date'].max()}"
    )
    logger.info(f"Deduplication authority by country: {authority}")

    def _store(frame, identifier, description, formats):
        metadata = store_assets.objectMetadata(
            name=identifier, description=description, source_url=NWS_OMSA_VIEW
        )
        store_assets.store_dataframe_to_s3(
            frame,
            SCREWWORM_OUTPUT_PATH,
            identifier,
            s3_resource,
            metadata=metadata,
            latestdatasetpath=SCREWWORM_LATEST_PATH,
            enable_latest_path=True,
            formats=list(formats),
        )

    def _store_view(event_frame, species_frame, suffix, event_description, species_description):
        """Write the events, points and species tables for one view."""
        _store(event_frame, f"nws_unified_events{suffix}", event_description, ["csv", "json"])
        _store(
            species_frame,
            f"nws_unified_event_species{suffix}",
            species_description,
            ["csv", "json"],
        )
        geo = event_frame.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
        point_frame = gpd.GeoDataFrame(
            geo,
            geometry=gpd.points_from_xy(geo["longitude"], geo["latitude"]),
            crs="EPSG:4326",
        )
        _store(
            point_frame,
            f"nws_unified_events_points{suffix}",
            f"{event_description} Emitted here as EPSG:4326 points; check geo_level for the "
            "precision of each point.",
            ["geojson", "csv"],
        )
        return {
            f"nws_unified_events{suffix}": len(event_frame),
            f"nws_unified_events_points{suffix}": len(point_frame),
            f"nws_unified_event_species{suffix}": len(species_frame),
        }

    counts = _store_view(events, species, "", EVENTS_DESCRIPTION, SPECIES_DESCRIPTION)

    # Deduplicated view: one authoritative source per country, so total_cases is
    # safe to sum. The excluded rows stay in the full view above.
    deduped = events[events["is_authoritative"]].reset_index(drop=True)
    deduped_species = species[species["is_authoritative"].fillna(False)].reset_index(drop=True)
    counts.update(
        _store_view(
            deduped,
            deduped_species,
            "_deduped",
            DEDUPED_DESCRIPTION,
            DEDUPED_SPECIES_DESCRIPTION,
        )
    )
    logger.info(
        f"Deduplicated NWS events: {len(deduped)} rows "
        f"({deduped['source'].value_counts().to_dict()}), "
        f"{int(deduped['total_cases'].sum())} cases across "
        f"{deduped['country_iso3'].nunique()} countries"
    )
    logger.info(f"Unified NWS stored datasets: {counts}")

    yield Output(
        {
            "events": events,
            "species": species,
            "deduped": deduped,
            "deduped_species": deduped_species,
            "authority": authority,
            "retrieved_at": retrieved_at,
        },
        metadata={
            **counts,
            "retrieved_at": retrieved_at,
            "duplicate_candidates": int(events["duplicate_candidate"].sum()),
            "sources": ", ".join(f"{k}={v}" for k, v in events["source"].value_counts().items()),
            "dedupe_authority": ", ".join(f"{k}={v}" for k, v in sorted(authority.items())),
            "deduped_total_cases": int(deduped["total_cases"].sum()),
        },
    )
    for result in _quality_checks(context, events, species, deduped, authority, diagnostics):
        yield result


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------


def _slack_alert(context, message: str) -> None:
    """Post a check failure to the failures channel; never break the run over it."""
    import os

    try:
        channel = os.environ.get("SLACK_CHANNEL_FAILURES", "workflows-failures")
        context.resources.slack.get_client().chat_postMessage(channel=channel, text=message)
    except Exception as e:  # noqa: BLE001 - alerting is best-effort
        get_dagster_logger().error(f"Failed to send Slack unified-screwworm alert: {e}")


def _quality_checks(
    context,
    events: pd.DataFrame,
    species: pd.DataFrame,
    deduped: pd.DataFrame,
    authority: dict,
    diagnostics: dict,
):
    """Yield the five AssetCheckResults over the merged and deduplicated frames."""
    # 1. Species vocabulary coverage — a new upstream species falls to "other".
    unmapped_species = sorted(
        set(diagnostics.get("aphis_us_unmapped_species", []))
        | set(diagnostics.get("aphis_mx_unmapped_species", []))
    )
    if unmapped_species:
        message = (
            "⚠️ Unified screwworm: species values not in SPECIES_ALIASES "
            f"(mapped to 'other'): {unmapped_species}. Add them to "
            "screwworm_unified.SPECIES_ALIASES."
        )
        _slack_alert(context, message)
    yield AssetCheckResult(
        check_name="unified_species_vocab_covered",
        passed=not unmapped_species,
        severity=AssetCheckSeverity.WARN,
        description=(
            "All source species values mapped to the controlled vocabulary."
            if not unmapped_species
            else message
        ),
        metadata={"unmapped_species": unmapped_species},
    )

    # 2. Mexican state centroid coverage.
    unmapped_states = sorted(diagnostics.get("aphis_mx_unmapped_states", []))
    if unmapped_states:
        message = (
            "⚠️ Unified screwworm: APHIS Mexico states with no centroid "
            f"(left ungeocoded): {unmapped_states}. Add them to "
            "screwworm_unified.MX_STATE_CENTROIDS or MX_STATE_ALIASES."
        )
        _slack_alert(context, message)
    yield AssetCheckResult(
        check_name="unified_mx_states_geocoded",
        passed=not unmapped_states,
        severity=AssetCheckSeverity.WARN,
        description=(
            "All APHIS Mexico states resolved to a centroid."
            if not unmapped_states
            else message
        ),
        metadata={
            "unmapped_states": unmapped_states,
            "geocoded_rows": diagnostics.get("aphis_mx_geocoded", 0),
        },
    )

    # 3. Long/wide reconciliation. Species rows only exist where a count is
    # non-zero, so their sum must equal the event's total_cases.
    per_event = species.groupby("event_uid")["case_count"].sum(min_count=1)
    totals = events.set_index("event_uid")["total_cases"]
    comparison = pd.DataFrame({"long": per_event, "wide": totals}).fillna(0)
    mismatched = comparison[comparison["long"] != comparison["wide"]]
    mismatch_count = len(mismatched)
    if mismatch_count:
        message = (
            f"❌ Unified screwworm: {mismatch_count} events where per-species case "
            f"counts do not sum to total_cases (e.g. {mismatched.head(5).to_dict('index')}). "
            "The wide and long tables disagree."
        )
        _slack_alert(context, message)
    yield AssetCheckResult(
        check_name="unified_totals_reconcile",
        passed=not mismatch_count,
        severity=AssetCheckSeverity.ERROR,
        description=(
            f"Per-species case counts reconcile with total_cases across {len(comparison)} events."
            if not mismatch_count
            else message
        ),
        metadata={
            "mismatched_events": mismatch_count,
            "total_cases_wide": int(totals.fillna(0).sum()),
            "total_cases_long": int(per_event.fillna(0).sum()),
        },
    )

    # 4. Key integrity of the merged table.
    duplicate_uids = int(events["event_uid"].duplicated().sum())
    missing = {
        column: int(events[column].isna().sum() + (events[column].astype("string") == "").sum())
        for column in ["event_uid", "source", "country_iso3", "event_date"]
    }
    unknown_countries = int((events["country_iso3"] == "UNK").sum())
    problems = duplicate_uids or any(missing.values()) or unknown_countries
    if problems:
        message = (
            "❌ Unified screwworm: required-field problems — "
            f"duplicate event_uid: {duplicate_uids}, null/empty by column: {missing}, "
            f"unmapped countries (UNK): {unknown_countries}."
        )
        _slack_alert(context, message)
    yield AssetCheckResult(
        check_name="unified_required_fields",
        passed=not problems,
        severity=AssetCheckSeverity.ERROR,
        description=(
            f"event_uid unique across {len(events)} events; key dimensions populated."
            if not problems
            else message
        ),
        metadata={
            "duplicate_event_uids": duplicate_uids,
            "unknown_country_rows": unknown_countries,
            **{f"null_{k}": v for k, v in missing.items()},
        },
    )

    # 5. The deduplicated view must hold exactly one source per country and must
    # not silently lose a country the merged table covers.
    sources_per_country = deduped.groupby("country_iso3")["source"].nunique()
    multi_source = sorted(sources_per_country[sources_per_country > 1].index)
    lost_countries = sorted(set(events["country_iso3"]) - set(deduped["country_iso3"]))
    # A fallback fired if any country was won by a source that is not its first
    # preference — worth surfacing, since it means a source came back empty.
    fallbacks = {
        country: source
        for country, source in authority.items()
        if source
        != COUNTRY_SOURCE_PRIORITY.get(country, DEFAULT_SOURCE_PRIORITY)[0]
    }
    problems = bool(multi_source or lost_countries)
    if problems or fallbacks:
        message = (
            "❌ Unified screwworm dedupe: "
            f"countries with more than one source: {multi_source}, "
            f"countries missing from the deduplicated view: {lost_countries}, "
            f"countries won by a fallback source (preferred source returned no rows): "
            f"{fallbacks}."
        )
        _slack_alert(context, message)
    yield AssetCheckResult(
        check_name="unified_dedupe_one_source_per_country",
        passed=not problems,
        severity=AssetCheckSeverity.ERROR if problems else AssetCheckSeverity.WARN,
        description=(
            f"Deduplicated view keeps one source per country across "
            f"{deduped['country_iso3'].nunique()} countries "
            f"({len(deduped)} of {len(events)} events, "
            f"{int(deduped['total_cases'].sum())} cases)."
            if not problems
            else message
        ),
        metadata={
            "deduped_events": len(deduped),
            "deduped_total_cases": int(deduped["total_cases"].sum()),
            "countries_with_multiple_sources": multi_source,
            "countries_missing_from_dedupe": lost_countries,
            "fallback_authorities": fallbacks,
            "authority": authority,
        },
    )


nws_unified_job = define_asset_job(
    "nws_unified_job",
    selection=[UNIFIED_ASSET_KEY],
)


# The three source pulls run at 6:00, 6:15 and 6:30pm Eastern; merge at 6:45.
@schedule(
    job=nws_unified_job,
    cron_schedule="45 18 * * *",
    name="nws_unified_schedule",
    execution_timezone="America/New_York",
)
def nws_unified_schedule(context):
    return RunRequest()
