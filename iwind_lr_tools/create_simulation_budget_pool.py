"""
While temporary file + symbolic linking method is able to track root file changes automatically,
some strange bugs may occur, so this alternative implementation is made.

The implementation may break some tests but should be transparent for Runner and run_batch API users.
"""

import os
from warnings import warn

WATER_BUDGET_POOL_ENV_NAME = "WATER_BUDGET_POOL"

if WATER_BUDGET_POOL_ENV_NAME not in os.environ:
    warn(f"Create environment variable {WATER_BUDGET_POOL_ENV_NAME} to prevent extra cost")
    

BUDGET_POOL = os.environ[WATER_BUDGET_POOL_ENV_NAME]

