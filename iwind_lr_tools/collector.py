"""
Collect interested files
"""

from iwind_lr_tools.io.common import DataFrameNode, FlowNode, ConcentrationNode
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

from .io import aser_inp, efdc_inp, qbal_out, qser_inp, wqpsc_inp, WQWCTS_out, wq3dwc_inp, conc_adjust_inp

class ModelXML:
    def __init__(self, xml_path: str):
        import xml.etree.ElementTree as ET
        from dateutil.parser import parse

        self.tree = ET.parse(xml_path)
        self.root = self.tree.getroot()

        tag = self.tree.find("ReferenceTime")
        self.ref_date = parse(tag.text)

    def to_date(self, arr):
        return [self.ref_date + timedelta(x) for x in arr]

    def get_date(self, df: pd.DataFrame, key:str):
        return df[key].map(lambda x: self.ref_date + timedelta(days=x))

    def enhance_date(self, df: pd.DataFrame, key:str, date_key="date"):
        df[date_key] = self.get_date(df, key)

    def to_time(self, date: datetime):
        delta  = date - self.ref_date
        return delta.total_seconds() / (60*60*24)

def get_model(root):
    root_parent = Path(root) / ".."
    glob_model_list = list(root_parent.glob("*.model"))
    assert len(glob_model_list) == 1, "model file is missing or ambiguous (>=2)"
    model_p = glob_model_list[0]
    return ModelXML(model_p)

inp_map = {
    "aser.inp": aser_inp, # weather time series
    "efdc.inp": efdc_inp, # general master input
    "qser.inp": qser_inp, # flow time series
    "wqpsc.inp": wqpsc_inp, # concentration of inflow time series
    "wq3dwc.inp": wq3dwc_inp, # concentration of pollutant master input
    "conc_adjust.inp": conc_adjust_inp # concentration adjust matrix
}

out_map = {
    "qbal.out": qbal_out, # general stats
    "WQWCTS.out": WQWCTS_out # concenteration of outflow time series
}

has_df_map_list = ["efdc.inp", "qser.inp", "wqpsc.inp", "wq3dwc.inp"]
node_cls_map = {
    "efdc.inp": DataFrameNode,
    "qser.inp": FlowNode,
    "wqpsc.inp": ConcentrationNode,
    "wq3dwc.inp": DataFrameNode
}

# dependences and extra_length_map
dep_map = {
    "wq3dwc.inp": (["efdc.inp"], wq3dwc_inp.parse_dep),
    "conc_adjust.inp": (["efdc.inp"], conc_adjust_inp.parse_dep)
}

inp_out_map = {}
inp_out_map.update(inp_map)
inp_out_map.update(out_map)

"""
def parse_map(root, map):
    return {k:v.parse(root / k) for k, v in map.items()}

def parse_inp(root):
    return parse_map(root, inp_map)

def parse_out(root):
    return parse_map(root, out_map)

def parse_all(root):
    return parse_map(root, inp_out_map)
"""

def parse_with_resolve(root, io_module_map, max_loop=1000):
    data_map = {}
    remain_list = list(io_module_map.keys())
    for _ in range(max_loop):
        if len(remain_list) == 0:
            return data_map
        
        for key in remain_list:
            new_remain_list = []
            module = io_module_map[key]
            if key not in dep_map:
                data_map[key] = module.parse(root / key)
            else:
                dep_list, extra_length_map_callback = dep_map[key]
                for dep in dep_list:
                    if dep not in dep_list:
                        break
                else:
                    kwargs = extra_length_map_callback(data_map)
                    data_map[key] = module.parse(root / key, **kwargs)
                    continue
                new_remain_list.append(key)
        remain_list = new_remain_list
    
    raise ValueError("max_loop reached, is resolve graph too large or circular reference appear?")

def parse_all(root):
    return parse_with_resolve(root, inp_out_map)

def parse_out(root):
    return parse_with_resolve(root, out_map)


def get_df_node_map_map_and_df_map_map(data_map):
    df_node_map_map = {}
    df_map_map = {}
    for key in has_df_map_list:
        df_node_map = node_cls_map[key].get_df_node_map(data_map[key])
        df_map = {k: v.get_df() for k, v in df_node_map.items()}
        df_node_map_map[key] = df_node_map
        df_map_map[key] = df_map
    return df_node_map_map, df_map_map

def get_all(root):
    data_map = parse_all(root)
    df_node_map_map, df_map_map = get_df_node_map_map_and_df_map_map(data_map)
    return data_map, df_node_map_map, df_map_map

