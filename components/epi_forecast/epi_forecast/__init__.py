"""
Epidemiology Forecast Component

A reusable Dagster component for epidemiology forecasting pipelines.

This component can be configured via:
1. Python code using EpiForecastComponentConfig
2. YAML configuration using defs.yaml files (Dagster Components)

## Python Usage

```python
from epi_forecast import (
    create_epi_forecast_definitions,
    EpiForecastComponentConfig,
    SimulatorConfig,
    S3MonitorConfig,
    DiseaseConfig,
)

config = EpiForecastComponentConfig(
    name="sandiego_ili",
    jurisdiction="SanDiego",
    jurisdiction_display="San Diego County",
    s3_output_base_path="pathogens/sandiego/epidemiology",
    public_bucket="public-data",
    simulator=SimulatorConfig(simulator_id=1, output_bucket="resilientseasonal"),
    s3_monitor=S3MonitorConfig(monitor_path="api_run/", monitor_bucket="resilientseasonal"),
    diseases=[
        DiseaseConfig(name="COVID", display_name="COVID-19", input_csv_suffix="COVID"),
    ],
)

defs = create_epi_forecast_definitions(config)
```

## YAML Configuration (Dagster Components)

Create a `defs.yaml` file:

```yaml
type: epi_forecast.EpiForecastComponent
attributes:
  name: sandiego_ili
  jurisdiction: SanDiego
  jurisdiction_display: San Diego County
  s3_output_base_path: pathogens/sandiego/epidemiology
  public_bucket: "{{ env.PUBLIC_BUCKET }}"
  simulator:
    simulator_id: 1
    output_bucket: resilientseasonal
  s3_monitor:
    monitor_path: api_run/
    monitor_bucket: resilientseasonal
  diseases:
    - name: COVID
      display_name: COVID-19
      input_csv_suffix: COVID
```

Then scaffold using the dg CLI:
    dg scaffold defs epi_forecast.EpiForecastComponent --name my_forecast
"""

from .definitions import create_epi_forecast_definitions
from .config import (
    EpiForecastComponentConfig,
    SimulatorConfig,
    S3MonitorConfig,
    DiseaseConfig,
    TemplateConfig,
    DataSourceConfig,
    PublishingConfig,
    create_sandiego_ili_config,
)
from .component import (
    EpiForecastComponent,
    EpiForecastYAMLConfig,
    load_component_from_yaml,
)

__all__ = [
    # Core factory function
    "create_epi_forecast_definitions",
    # Python configuration models
    "EpiForecastComponentConfig",
    "SimulatorConfig",
    "S3MonitorConfig",
    "DiseaseConfig",
    "TemplateConfig",
    "DataSourceConfig",
    "PublishingConfig",
    # Convenience functions
    "create_sandiego_ili_config",
    # Dagster Component (YAML-based)
    "EpiForecastComponent",
    "EpiForecastYAMLConfig",
    "load_component_from_yaml",
]
