"""Tests for the SBIWTP plant-flow assets built from one factory.

The effluent assets existed before the factory; anything downstream (the
deficit asset, the model data, the S3 layout) keys on their names and paths,
so the factory must reproduce them exactly while adding the influent set.
"""

import pandas as pd
from dagster import AssetKey

from tijuana.assets import ibwc_flows
from tijuana.assets.ibwc_flows import (
    EFFLUENT,
    INFLUENT,
    build_plant_flow_assets,
    parse_effluent_csv,
    parse_flow_csv,
)

# SYNTHETIC_FIXTURE: the portal's export layout — three preamble rows, a
# header row, readings, and a trailing disclaimer.
EXPORT = """#Bulk Export - Points as recorded,,,
,SBIWTP,,
,South Bay International Wastewater Treatment Plant,,
,Flow.Plant-Influent-Flow-MGD,Grade Code,Qualifiers
Timestamp (UTC-08:00),Value (M US Gal/d),,
2026-08-14 00:00:00,35.489,-1,
2026-08-15 00:00:00,35.713,-1,
"Data are being provided with the understanding they are provisional.",,,
"""


def test_export_parses_to_timestamp_and_value_columns():
    df = parse_flow_csv(EXPORT)

    assert len(df) == 2
    assert df.columns[0] == "Timestamp (UTC-08:00)"
    assert df["Value (M US Gal/d)"].tolist() == [35.489, 35.713]
    assert df.index.name is None                     # timestamp stays a column


def test_the_old_parser_name_still_works():
    pd.testing.assert_frame_equal(parse_effluent_csv(EXPORT), parse_flow_csv(EXPORT))


def test_effluent_keys_paths_and_names_are_unchanged():
    assert ibwc_flows.effluent_flow_today.key == AssetKey(["ibwc", "effluent_flow_today"])
    assert ibwc_flows.effluent_flow_current_year.key == AssetKey(["ibwc", "effluent_flow_current_year"])
    assert ibwc_flows.effluent_flow_yearly.key == AssetKey(["ibwc", "effluent_flow_yearly"])
    assert ibwc_flows.effluent_flow_current_schedule.name == "effluent_flow_current"
    assert ibwc_flows.effluent_flow_yearly_schedule.name == "effluent_flow_yearly"
    assert ibwc_flows.effluent_flow_current_job.name == "effluent_flow_current"
    assert ibwc_flows.effluent_flow_yearly_job.name == "effluent_flow_yearly_all"
    assert EFFLUENT.s3_output_path == "tijuana/effluent_flow/output"
    assert EFFLUENT.s3_raw_path == "tijuana/effluent_flow/raw"
    assert EFFLUENT.s3_latest_path == "tijuana/effluent_flow"
    assert ibwc_flows.s3_latest_path == "tijuana/effluent_flow"
    assert "Flow.Plant-Effluent-Flow-MGD%40SBIWTP" in ibwc_flows.EFFLUENT_TODAY_URL
    assert "${YEAR}-01-01" in ibwc_flows.EFFLUENT_YEAR_TEMPLATE


def test_influent_assets_mirror_the_effluent_set():
    assert ibwc_flows.influent_flow_today.key == AssetKey(["ibwc", "influent_flow_today"])
    assert ibwc_flows.influent_flow_current_year.key == AssetKey(["ibwc", "influent_flow_current_year"])
    assert ibwc_flows.influent_flow_yearly.key == AssetKey(["ibwc", "influent_flow_yearly"])
    assert ibwc_flows.influent_flow_current_schedule.name == "influent_flow_current"
    assert ibwc_flows.influent_flow_yearly_schedule.name == "influent_flow_yearly"
    assert INFLUENT.s3_output_path == "tijuana/influent_flow/output"
    assert "Flow.Plant-Influent-Flow-MGD%40SBIWTP" in INFLUENT.today_url
    assert "StartTime=2025-01-01 00:00:00" in INFLUENT.year_url(2025)


def test_freshness_checks_target_their_own_today_asset():
    eff = ibwc_flows.effluent_flow_freshness_check
    inf = ibwc_flows.influent_flow_freshness_check

    assert [c.asset_key for c in eff.check_specs] == [AssetKey(["ibwc", "effluent_flow_today"])]
    assert [c.name for c in eff.check_specs] == ["effluent_flow_freshness_check"]
    assert [c.asset_key for c in inf.check_specs] == [AssetKey(["ibwc", "influent_flow_today"])]
    assert [c.name for c in inf.check_specs] == ["influent_flow_freshness_check"]


def test_factory_builds_a_complete_set():
    built = build_plant_flow_assets(INFLUENT)

    assert set(built) == {
        "today", "current_year", "yearly", "freshness_check",
        "current_job", "yearly_job", "current_schedule", "yearly_schedule",
    }
    assert built["yearly"].partitions_def is ibwc_flows.yearly_partitions
