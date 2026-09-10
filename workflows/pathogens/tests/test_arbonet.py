"""Unit tests for the pure transforms in pathogens.assets.arbonet.

The asset functions need S3 and Dagster resources; the transforms they
call do not. These tests load ``arbonet.py`` directly (the ``pathogens``
package's ``__init__`` pulls in the full Definitions, including Slack and
S3 resources) with ``resilient_core.utils.store_assets`` stubbed, so they
run with only pandas and dagster installed and never touch the network.

The county FIPS handling matters downstream: the NWS dashboard joins these
rows to its county vector tiles on the 5-digit FIPS (STATE + COUNTY), so a
lost leading zero or a mis-labelled Connecticut planning region would
silently drop counties from the human-case map.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

_ASSETS = Path(__file__).resolve().parents[1] / "src" / "pathogens" / "assets"

# Stub the storage helper the module imports at load time; nothing under
# test calls it.
if "resilient_core.utils.store_assets" not in sys.modules:
    _rc = types.ModuleType("resilient_core")
    _utils = types.ModuleType("resilient_core.utils")
    _store = types.ModuleType("resilient_core.utils.store_assets")
    _utils.store_assets = _store
    _rc.utils = _utils
    sys.modules.setdefault("resilient_core", _rc)
    sys.modules.setdefault("resilient_core.utils", _utils)
    sys.modules["resilient_core.utils.store_assets"] = _store

_spec = importlib.util.spec_from_file_location("_arbonet_under_test", _ASSETS / "arbonet.py")
arbonet = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arbonet)


# ---------------------------------------------------------------------------
# Suppressed-count bins
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label,expected", [
    ("6", (6, 6)),
    ("1 to 4", (1, 4)),
    ("1 TO 4", (1, 4)),
    ("50+", (50, None)),
    ("", (None, None)),
    (None, (None, None)),
    (float("nan"), (None, None)),
    ("suppressed", (None, None)),
])
def test_parse_binned_count(label, expected):
    assert arbonet.parse_binned_count(label) == expected


# ---------------------------------------------------------------------------
# West Nile virus — current season
# ---------------------------------------------------------------------------

WNV_CURRENT_CSV = (
    '"County","Activity","Total human disease cases","Neuroinvasive disease cases","Presumptive viremic blood donors"\n'
    "1039,Human infections,1,1,0\n"          # leading zero lost upstream -> must be restored
    "06037,Human infections and non-human activity,11,11,2\n"
    "09130,Non-human activity,0,0,0\n"       # Connecticut planning region (2022+)
    "09009,Human infections,2,1,0\n"         # legacy Connecticut county
)


def test_wnv_current_restores_fips_and_labels_schemes():
    df = arbonet.wnv_current_to_df(WNV_CURRENT_CSV, as_of="2026-09-07")
    assert df["county_fips"].tolist() == ["01039", "06037", "09009", "09130"]
    assert df.set_index("county_fips")["state_abbr"].to_dict() == {
        "01039": "AL", "06037": "CA", "09009": "CT", "09130": "CT",
    }
    schemes = df.set_index("county_fips")["fips_scheme"].to_dict()
    assert schemes["09130"] == "planning_region"
    assert schemes["09009"] == "county"
    assert schemes["06037"] == "county"


def test_wnv_current_counts_and_provenance():
    df = arbonet.wnv_current_to_df(WNV_CURRENT_CSV, as_of="2026-09-07")
    la = df.set_index("county_fips").loc["06037"]
    assert la["total_human_cases"] == 11
    assert la["neuroinvasive_cases"] == 11
    assert la["presumptive_viremic_blood_donors"] == 2
    assert la["activity"] == "Human infections and non-human activity"
    assert bool(df["provisional"].all())
    assert (df["as_of"] == "2026-09-07").all()
    # A non-human-only row carries zero human counts; its information is the class.
    ct = df.set_index("county_fips").loc["09130"]
    assert ct["total_human_cases"] == 0 and ct["activity"] == "Non-human activity"


# ---------------------------------------------------------------------------
# West Nile virus — county-year panel + CDC incidence join
# ---------------------------------------------------------------------------

WNV_YEARLY_CSV = (
    '"Year","County","Activity","Reported human cases","Neuroinvasive disease cases","Identified by Blood Donor Screening","Notes"\n'
    "2023,06037,Human infections,100,70,5,\n"
    "2024,06037,Human infections,50,30,1,\n"
    "2025,06037,Human infections,20,15,0,\n"
    "2024,9130,Human infections,1,1,0,\n"
)

WNV_INCIDENCE_CSV = (
    '"Type","Year","County","Population","Incidence","Legend","Notes"\n'
    "Neuroinvasive disease cases,1999-2025,06037,9873700,0.64,0.5 to 0.99,\n"
    "All disease cases,1999-2025,06037,9873700,0.90,0.5 to 0.99,\n"   # wrong Type: must be ignored
)


def test_wnv_county_yearly_flags_provisional_year_and_joins_incidence():
    df = arbonet.wnv_county_yearly_to_df(WNV_YEARLY_CSV, WNV_INCIDENCE_CSV)
    la = df[df["county_fips"] == "06037"].set_index("year")
    assert la.loc[2025, "provisional"] and not la.loc[2024, "provisional"]
    assert la.loc[2023, "human_cases"] == 100 and la.loc[2023, "neuroinvasive_cases"] == 70
    # CDC's precomputed cumulative incidence rides on every county-year row,
    # taken from the neuroinvasive row of the incidence file only.
    assert (la["cumulative_neuroinvasive_incidence_per_100k"] == 0.64).all()
    assert (la["cumulative_population"] == 9873700).all()
    assert (la["cumulative_period"] == "1999-2025").all()
    ct = df[df["county_fips"] == "09130"].iloc[0]
    assert ct["fips_scheme"] == "planning_region" and ct["state_abbr"] == "CT"
    assert pd.isna(ct["cumulative_neuroinvasive_incidence_per_100k"])


def test_wnv_county_yearly_without_incidence_file():
    df = arbonet.wnv_county_yearly_to_df(WNV_YEARLY_CSV, "")
    assert "cumulative_neuroinvasive_incidence_per_100k" not in df.columns
    assert len(df) == 4


# ---------------------------------------------------------------------------
# West Nile virus — state cumulative
# ---------------------------------------------------------------------------

WNV_STATE_HIST_CSV = (
    '"Type","Year","State","Reported Cases","Legend categories"\n'
    "All disease cases,1999-2025,CA,8279,x\n"
    "Neuroinvasive disease cases,1999-2025,CA,5069,x\n"
    "All disease cases,2024,CA,120,x\n"          # per-year breakdown row: not the cumulative
    "All disease cases,1999-2025,TX,6681,x\n"
    "Neuroinvasive disease cases,1999-2025,TX,4265,x\n"
)

WNV_STATE_CURRENT_CSV = (
    '"State","Reported Cases","Legend"\n'
    "CA,18,x\n"
    "TX,23,x\n"
)


def test_wnv_state_cumulative_picks_the_cumulative_rows_by_year_label():
    df = arbonet.wnv_state_cumulative_to_df(WNV_STATE_HIST_CSV, WNV_STATE_CURRENT_CSV, as_of="2026-09-07")
    ca = df.set_index("state_abbr").loc["CA"]
    assert ca["total_human_cases"] == 8279, "the 2024 breakdown row must not shadow the cumulative"
    assert ca["total_neuroinvasive_cases"] == 5069
    assert ca["current_season_reported_cases"] == 18
    assert ca["cumulative_year_range"] == "1999-2025"
    assert bool(ca["includes_current_season"])


def test_wnv_state_cumulative_refuses_ambiguous_year_labels():
    ambiguous = WNV_STATE_HIST_CSV + "All disease cases,1999-2026,CA,9000,x\n"
    with pytest.raises(ValueError, match="exactly one cumulative-period Year label"):
        arbonet.wnv_state_cumulative_to_df(ambiguous, "", as_of="2026-09-07")


def test_wnv_state_cumulative_without_current_season():
    df = arbonet.wnv_state_cumulative_to_df(WNV_STATE_HIST_CSV, "", as_of="2026-09-07")
    assert df["current_season_reported_cases"].isna().all()
    assert not df["includes_current_season"].any()


# ---------------------------------------------------------------------------
# Dengue — current season, suppressed bins and travel status
# ---------------------------------------------------------------------------

DENGUE_COUNTY_CSV = (
    '"Year","Travel status","County","Count","Legend","Notes"\n'
    '2026,"All","06037","1 to 4","1 to 4","CA, NA"\n'
    '2026,"Travel associated","06037","1 to 4","1 to 4","CA, NA"\n'
    '2026,"All","12057","66","50 to 99","FL, Hillsborough County"\n'
    '2026,"Locally acquired","12057","58","50 to 99","FL, Hillsborough County"\n'
    '2026,"All","4013","7","5 to 49","AZ, NA"\n'
)


def test_dengue_county_bins_and_travel_status():
    df = arbonet.dengue_county_current_to_df(DENGUE_COUNTY_CSV, as_of="2026-09-07")
    assert sorted(df["county_fips"].unique()) == ["04013", "06037", "12057"]
    la = df[(df["county_fips"] == "06037") & (df["travel_status"] == "all")].iloc[0]
    assert la["count_label"] == "1 to 4"
    assert (la["count_min"], la["count_max"], la["count_midpoint"]) == (1, 4, 2.5)
    assert la["state_abbr"] == "CA"
    hb = df[(df["county_fips"] == "12057") & (df["travel_status"] == "locally_acquired")].iloc[0]
    assert (hb["count_min"], hb["count_max"], hb["count_midpoint"]) == (58, 58, 58.0)
    assert set(df["travel_status"]) == {"all", "travel_associated", "locally_acquired"}
    assert bool(df["provisional"].all())


def test_dengue_jurisdiction_and_epi_curve():
    jur = arbonet.dengue_jurisdiction_current_to_df(
        '"Year","Travel status","Jurisdiction","Count","Legend","Notes"\n'
        '2026,"All","AS","430","x",\n'
        '2026,"Locally acquired","AS","428","x",\n',
        as_of="2026-09-07",
    )
    assert jur.set_index("travel_status").loc["locally_acquired", "state_abbr"] == "AS"
    assert jur["count_min"].tolist() == [430, 428]

    epi = arbonet.dengue_epi_curve_to_df(
        '"Year","Travel status","Week","Reported cases"\n'
        "2026,All,1,12\n"
        "2026,Locally acquired,1,3\n",
        as_of="2026-09-07",
    )
    assert epi["reported_cases"].sum() == 15
    assert epi["week"].tolist() == [1, 1]
