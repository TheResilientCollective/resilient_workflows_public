"""
Assets for the Epidemiology Forecast Component.

These assets are created dynamically based on the component configuration.
"""

from .forecast_processing import create_forecast_processing_assets

__all__ = [
    "create_forecast_processing_assets",
]
