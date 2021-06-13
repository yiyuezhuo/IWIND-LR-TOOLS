"""
Collect interested files
"""

from pathlib import Path
import pandas as pd
from datetime import timedelta

from .io import aser_inp, efdc_inp, qbal_out, qser_inp, wqpsc_inp, WQWCTS_out

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

inp_map = {
    "aser.inp": aser_inp,
    "efdc.inp": efdc_inp,
    "qser.inp": qser_inp,
    "wqpsc.inp": wqpsc_inp,
}

out_map = {
    "qbal.out": qbal_out,
    "WQWCTS.out": WQWCTS_out
}

has_df_map_list = ["efdc.inp", "qser.inp"]

inp_out_map = {}
inp_out_map.update(inp_map)
inp_out_map.update(out_map)

def parse_map(root, map):
    return {k:v.parse(root / k) for k, v in map.items()}

def parse_inp(root):
    return parse_map(root, inp_map)

def parse_out(root):
    return parse_map(root, out_map)

def parse_all(root):
    return parse_map(root, inp_out_map)

def get_df_map_map(data_map):
    df_map_map = {}
    for key in has_df_map_list:
        df_map_map[key] = inp_out_map[key].get_df_map(data_map[key])
    return df_map_map

def get_all(root):
    data_map = parse_all(root)
    df_map_map = get_df_map_map(data_map)
    return data_map, df_map_map
