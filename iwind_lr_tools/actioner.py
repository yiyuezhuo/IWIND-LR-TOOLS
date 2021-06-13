
import pandas as pd
from typing import List

from .collector import inp_out_map
from .io import qser_inp
from .io.common import Node, FlowNode

class Actioner:
    def __init__(self, data_map, df_map_map):
        self.data_map = data_map
        self.df_map_map = df_map_map

    def get_simulation_length(self):
        return self.df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0]

    def set_simulation_length(self, value):
        self.df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0] = value

    def sync_df_map_map(self, keys=None):
        if keys is None:
            keys = self.df_map_map.keys()
        for key in keys:
            # df_map_map[key] = inp_out_map[key].get_df_map(data_map[key])
            self.df_map_map[key].clear()
            self.df_map_map[key].update(inp_out_map[key].get_df_map(self.data_map[key]))

    def _set_flow(self, C08: pd.DataFrame, C09: pd.DataFrame, qser_node_list: List[Node]):
        C08_ref = self.df_map_map["efdc.inp"]["C08"]
        C09_ref = self.df_map_map["efdc.inp"]["C09"]
        qser_node_list_ref = self.data_map["qser.inp"]

        assert C08 is not C08_ref
        assert C09 is not C09_ref
        assert qser_node_list is not qser_node_list_ref

        self.df_map_map["efdc.inp"]["C08"] = C08
        self.df_map_map["efdc.inp"]["C09"] = C09

        qser_node_list_ref.clear()
        qser_node_list_ref.extend(qser_node_list)

        self.sync_df_map_map(["qser.inp"])
        
        n = C08.shape[0]
        C07 = self.df_map_map["efdc.inp"]["C07"]
        C07["NQSIJ"].iloc[0] = n
        C07["NQSER"].iloc[0] = n

    def set_flow(self, C08:pd.DataFrame, C09:pd.DataFrame, flow_node_list: List[FlowNode]):
        node_list = self.data_map["qser.inp"][:1] + flow_node_list
        return self._set_flow(C08, C09, node_list)
