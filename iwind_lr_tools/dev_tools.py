# import some useful names into develoment session (Jupyter/IPython)
# The module requires `requirements-dev.txt` (`pip install -r requirements-dev.txt`) to run.

import iwind_lr_tools
from pathlib import Path
import json
from datetime import timedelta, datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class ModelXML:
    def __init__(self, xml_path: str):
        import xml.etree.ElementTree as ET
        from dateutil.parser import parse

        self.tree = ET.parse(xml_path)
        self.root = self.tree.getroot()

        tag = self.tree.find("ReferenceTime")
        self.ref_date = parse(tag.text)

    def enhance_date(self, df: pd.DataFrame, key:str, date_key="date"):
        df[date_key] = df[key].map(lambda x: self.ref_date + timedelta(days=x))

def get_model(root):
    root_parent = Path(root) / ".."
    glob_model_list = list(root_parent.glob("*.model"))
    assert len(glob_model_list) == 1, "model file is missing or ambiguous (>=2)"
    model_p = glob_model_list[0]
    return ModelXML(model_p)

interested_keys = ["ROP", "LOP", "LDP", "RDP", "PO4"]

class Period:
    def __init__(self, date_begin, date_end):
        self.date_begin = date_begin
        self.date_end = date_end

    def limit(self, df, date_key='date'):
        return df[(df[date_key] > self.date_begin) & (df[date_key] < self.date_end)]

def zscore(x):
    return (x - x.mean()) / x.std()
