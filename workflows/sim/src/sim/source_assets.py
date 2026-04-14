"""Cross-code-location asset dependencies for the sim project.

These SourceAssets declare the upstream asset keys produced by the
`epidemiology` and `pathogens` code locations so that Dagster can resolve
the lineage across code locations.
"""

from dagster import AssetKey, SourceAsset

# Produced by the `epidemiology` code location
_epidemiology_keys = [
    AssetKey(["sandiego", "sandiego_epidemiology_hyper_extraction"]),
]

# Produced by the `pathogens` code location
_mpox_keys = [
    AssetKey(["mpox", "mpox_aggregated"]),
]

all_source_assets = [SourceAsset(key=k) for k in _epidemiology_keys + _mpox_keys]
