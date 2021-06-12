"""
Import some useful names into develoment session (Jupyter/IPython)
The module requires `requirements-dev.txt` (`pip install -r requirements-dev.txt`) to run.
This module is assumed to be very volatile so that other module should not depends on it.
If some code snippet is interesting, copy it but not import it.
"""

import iwind_lr_tools
from pathlib import Path
import json
from datetime import timedelta, datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from iwind_lr_tools import create_simulation, run_simulation, dumps
from .collector import *

# from .extract_non_modified_files import extract_non_modified_files


interested_keys = ["ROP", "LOP", "LDP", "RDP", "PO4"]

class Period:
    def __init__(self, date_begin, date_end):
        self.date_begin = date_begin
        self.date_end = date_end

    def limit(self, df, date_key='date'):
        return df[(df[date_key] > self.date_begin) & (df[date_key] < self.date_end)]

def zscore(x):
    return (x - x.mean()) / x.std()


