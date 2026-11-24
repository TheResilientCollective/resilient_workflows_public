"""resilient_core's epi schemas and Tableau reader must work without Dagster.

External projects (e.g. epimodel_harness) install ``resilient_core[tableau]`` only.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

RESILIENT_CORE_DIR = Path(__file__).resolve().parents[1]

# Imports the light modules, asserts no heavy dependency was pulled in, and
# validates a small statistical-extension frame.
CHECK_SCRIPT = """
import sys
import pandas as pd
from resilient_core.utils.resilient_epi_schemas import StatisticalExtensionSchema
from resilient_core.utils.tableau_workbook import (
    TableauWorkbookProcessor,
    convert_tableau_timestamps_to_datetime,
)

heavy = [m for m in ("dagster", "geopandas", "minio", "awswrangler", "pyairtable")
         if m in sys.modules]
assert not heavy, f"heavy modules imported: {heavy}"

df = pd.DataFrame({
    "Jurisdiction": ["SanDiego", "SanDiego"],
    "date": ["2025-01-04", "2025-01-11"],
    "disease": ["influenza", "influenza"],
    "metric": ["cases", "cases"],
    "observation_type": ["actual", "actual"],
    "count": [12, 15],
    "lower_ci": [10.0, 12.0],
    "upper_ci": [14.0, 18.0],
})
validated = StatisticalExtensionSchema.validate(df)
assert len(validated) == 2
TableauWorkbookProcessor()
print("ok")
"""


def test_light_modules_import_without_dagster():
    """In the workspace env (Dagster installed), importing must not load it."""
    result = subprocess.run(
        [sys.executable, "-c", CHECK_SCRIPT], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


@pytest.mark.skipif(
    os.environ.get("RESILIENT_CORE_ISOLATED_TEST") != "1" or shutil.which("uv") is None,
    reason="slow: builds an isolated env; set RESILIENT_CORE_ISOLATED_TEST=1 to run",
)
def test_tableau_extra_installs_without_dagster():
    """With only resilient_core[tableau] installed, Dagster is absent and the check passes."""
    script = "import importlib.util as u; assert u.find_spec('dagster') is None\n" + CHECK_SCRIPT
    result = subprocess.run(
        [
            "uv", "run", "--isolated", "--no-project",
            "--with", f"{RESILIENT_CORE_DIR}[tableau]",
            "python", "-c", script,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("ok")
