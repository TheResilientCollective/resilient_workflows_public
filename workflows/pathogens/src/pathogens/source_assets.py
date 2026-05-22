from dagster import SourceAsset, AssetKey

# From epidemiology code location
sd_mpox_source = SourceAsset(key=AssetKey(["sandiego", "sd_mpox"]))

all_source_assets = [sd_mpox_source]
