
import pandas as pd
from typing import List

from .collector import inp_out_map, get_df_node_map_map_and_df_map_map
from .io import qser_inp
from .io.common import ConcentrationNode, Node, FlowNode, CommentNode

class Actioner:
    def __init__(self, data_map:dict, df_node_map_map:dict, df_map_map:dict):
        self.data_map = data_map
        self.df_node_map_map = df_node_map_map
        self.df_map_map = df_map_map

    def get_simulation_length(self):
        return self.df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0]

    def set_simulation_length(self, value):
        self.df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0] = value

    """
    def sync_from_data_map(self, keys=None):
        if keys is None:
            keys = self.df_map_map.keys()
        for key in keys:
            # df_map_map[key] = inp_out_map[key].get_df_map(data_map[key])
            self.df_map_map[key].clear()
            self.df_map_map[key].update(inp_out_map[key].get_df_map(self.data_map[key]))
    """
    def sync_from_data_map(self):
        _df_node_map_map, _df_map_map = get_df_node_map_map_and_df_map_map(self.data_map)
        self.df_node_map_map.clear()
        self.df_node_map_map.update(_df_node_map_map)
        self.df_map_map.clear()
        self.df_map_map.update(_df_map_map)

    """
    def _set_flow(self, C08: pd.DataFrame, C09: pd.DataFrame, qser_node_list: List[Node]):
        C08_ref = self.df_map_map["efdc.inp"]["C08"]
        C09_ref = self.df_map_map["efdc.inp"]["C09"]
        qser_node_list_ref = self.data_map["qser.inp"]

        assert C08 is not C08_ref
        assert C09 is not C09_ref
        assert qser_node_list is not qser_node_list_ref

        # self.df_map_map["efdc.inp"]["C08"] = C08
        # self.df_map_map["efdc.inp"]["C09"] = C09
        self.df_node_map_map["efdc.inp"]["C08"].set_df(C08)
        self.df_node_map_map["efdc.inp"]["C09"].set_df(C09)

        qser_node_list_ref.clear()
        qser_node_list_ref.extend(qser_node_list)

        # self.sync_df_map_map(["qser.inp"])
        self.sync_from_data_map()
        
        n = C08.shape[0]
        C07 = self.df_map_map["efdc.inp"]["C07"]
        C07["NQSIJ"].iloc[0] = n
        C07["NQSER"].iloc[0] = n

    def set_flow(self, C08:pd.DataFrame, C09:pd.DataFrame, flow_node_list: List[FlowNode]):
        node_list = self.data_map["qser.inp"][:1] + flow_node_list
        return self._set_flow(C08, C09, node_list)
    """

    def select_flow_hard(self, idx_list: List[int]):
        """
        Keep some flow and drop otherthing. Ex:
        Before: [f0, f1, f2, f3, f4, f5]
        flow_map([3,4,5])
        After: [f3, f4, f5]
        """
        old2new = {old_idx: new_idx for new_idx, old_idx in enumerate(idx_list)}
        # new2old is expressd by idx_list itself

        C07 = self.df_map_map["efdc.inp"]["C07"]
        C08 = self.df_map_map["efdc.inp"]["C08"].iloc[idx_list].copy()
        C09 = self.df_map_map["efdc.inp"]["C09"].iloc[idx_list].copy()

        C07["NQSIJ"].iloc[0] = C07["NQSER"].iloc[0] = len(idx_list)

        C08["NQSERQ"] = C08["NQSERQ"].map(lambda x:old2new[x - 1] + 1)

        self.df_node_map_map["efdc.inp"]["C08"].set_df(C08)
        self.df_node_map_map["efdc.inp"]["C09"].set_df(C09)

        old_node_list = self.data_map["qser.inp"]
        old_df_node_list = FlowNode.get_df_node_list(old_node_list)
        df_node_list = [old_df_node_list[idx] for idx in idx_list]

        assert isinstance(old_node_list[0], CommentNode)
        assert len(old_df_node_list) == len(old_node_list) -1

        node_list = [old_node_list[0]] + df_node_list

        self.data_map["qser.inp"].clear()
        self.data_map["qser.inp"].extend(node_list)

        old_node_list = self.data_map["wqpsc.inp"]
        old_df_node_list = ConcentrationNode.get_df_node_list(old_node_list)
        df_node_list = [old_df_node_list[idx] for idx in idx_list if idx < len(old_df_node_list)]
        # outflow may point to a non-existed node, which denotes that its concentration is dependent on its location.

        assert len(old_node_list) == len(old_node_list)
        
        node_list = df_node_list

        self.data_map["wqpsc.inp"].clear()
        self.data_map["wqpsc.inp"].extend(node_list)

        self.sync_from_data_map()

    def select_flow_soft(self, idx_list: List[int]):
        """
        Adjust efdc.inp only, leave useless data in qser.inp and wqpsc.inp alone.
        """
        C07 = self.df_map_map["efdc.inp"]["C07"]
        C08 = self.df_map_map["efdc.inp"]["C08"].iloc[idx_list].copy()
        C09 = self.df_map_map["efdc.inp"]["C09"].iloc[idx_list].copy()

        C07["NQSIJ"].iloc[0] = C07["NQSER"].iloc[0] = len(idx_list)

        # C08["NQSERQ"] = C08["NQSERQ"].map(lambda x:old2new[x - 1] + 1)

        self.df_node_map_map["efdc.inp"]["C08"].set_df(C08)
        self.df_node_map_map["efdc.inp"]["C09"].set_df(C09)

        self.sync_from_data_map()


    def select_flow(self, idx_list: List[int]):
        # return self.select_flow_soft()
        return self.select_flow_hard()

    def get_flow_comp(self, selected_idx):
        selected_idx_set = set(selected_idx)
        remain_idx_list = [idx for idx in range(self.count_flow()) if idx not in selected_idx_set]
        return remain_idx_list

    def drop_flow_soft(self, selected_idx):
        return self.select_flow_soft(self.get_flow_comp(selected_idx))

    def drop_flow_hard(self, selected_idx):
        return self.select_flow_hard(self.get_flow_comp(selected_idx))

    def drop_flow(self, selected_idx):
        return self.drop_flow_hard(selected_idx)

    def count_flow(self):
        # return len(self.df_map_map["qser.inp"]) # soft drop will not work correctly
        return len(self.list_flow_name())

    def list_flow_name(self):
        # return self.df_map_map["qser.inp"].keys() # soft drop will not work correctly
        node_list = FlowNode.get_df_node_list(self.data_map["qser.inp"])
        name_list = []
        for idx_1plus in self.df_map_map["efdc.inp"]["C08"]["NQSERQ"]:
            idx = idx_1plus - 1
            name_list.append(node_list[idx].get_name())
        return name_list


    def get_flow_node_list(self):
        return FlowNode.get_df_node_list(self.data_map["qser.inp"])

    def get_concenteration_node_list(self):
        return ConcentrationNode.get_df_node_list(self.data_map["wqpsc.inp"])



