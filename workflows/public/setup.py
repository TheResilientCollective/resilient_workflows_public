from setuptools import find_packages, setup

setup(
    name="sheild",
    packages=find_packages(exclude=["sheild_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagstermill",
"papermill-origami>=0.0.8",
        "geocoder"
        "pandas",
        "geopandas",
        "geopy",
        "shapely",
"yfiles-jupyter-graphs",
        "ipywidgets",
        "matplotlib",
        "plotly",
        "notebook",
        "minio",
"seaborn",
        "scikit-learn",
        "tableauscraper",
        "arcgis",
        "arcgis2geojson",
        "keplergl",
        "scrapy",
        "beautifulsoup4",
        "urllib",
        "openpyxl",
    "geopy"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
