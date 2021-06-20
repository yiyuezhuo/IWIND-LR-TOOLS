
from multiprocessing.dummy import Value
import pandas as pd
from typing import List
from warnings import warn
from copy import deepcopy

from .collector import inp_out_map, get_df_node_map_map_and_df_map_map
from .io import qser_inp
from .io.common import ConcentrationNode, Node, FlowNode, CommentNode, FlowAdjustMatrixNode


class Actioner:
    def __init__(self, data_map:dict, df_node_map_map:dict, df_map_map:dict):
        self.data_map = data_map
        self.df_node_map_map = df_node_map_map
        self.df_map_map = df_map_map

    def get_simulation_length(self):
        return self.df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0]

    def set_simulation_length(self, value):
        self.df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0] = value

    def enable_restart(self):
        C02 = self.df_map_map["efdc.inp"]["C02"]
        C02.loc[0, "ISRESTI"] = 1

    def is_restarting(self):
        C02 = self.df_map_map["efdc.inp"]["C02"]
        return True if C02.loc[0, "ISRESTI"] == 1 else False

    def set_simulation_begin_time(self, value):
        C03 = self.df_map_map["efdc.inp"]["C03"]
        C03.loc[0, "TBEGIN"] = value

    def get_simulation_begin_time(self):
        C03 = self.df_map_map["efdc.inp"]["C03"]
        return C03.loc[0, "TBEGIN"]

    def sync_from_data_map(self):
        _df_node_map_map, _df_map_map = get_df_node_map_map_and_df_map_map(self.data_map)
        self.df_node_map_map.clear()
        self.df_node_map_map.update(_df_node_map_map)
        self.df_map_map.clear()
        self.df_map_map.update(_df_map_map)

    def select_flow_hard(self, idx_list: List[int]):
        old2new = {old_idx: new_idx for new_idx, old_idx in enumerate(idx_list)}

        # Modify efdc.inp
        C07 = self.df_map_map["efdc.inp"]["C07"]
        C08 = self.df_map_map["efdc.inp"]["C08"]
        C09 = self.df_map_map["efdc.inp"]["C09"]

        assert C07["NQSIJ"].iloc[0] == C07["NQSER"].iloc[0]
        C07["NQSIJ"].iloc[0] = C07["NQSER"].iloc[0] = len(idx_list)

        C08_selected = C08.iloc[idx_list].copy()
        C09_selected = C09.iloc[idx_list].copy()

        C08_selected["NQSERQ"] = C08_selected["NQSERQ"].map(lambda x:old2new[x - 1] + 1)

        self.df_node_map_map["efdc.inp"]["C08"].set_df(C08_selected)
        self.df_node_map_map["efdc.inp"]["C09"].set_df(C09_selected)

        # Modify qser.inp

        old_node_list = self.data_map["qser.inp"]
        old_df_node_list = FlowNode.get_df_node_list(old_node_list)
        df_node_list = [old_df_node_list[idx] for idx in idx_list]

        assert isinstance(old_node_list[0], CommentNode)
        assert len(old_df_node_list) == len(old_node_list) -1

        node_list = [old_node_list[0]] + df_node_list

        self.data_map["qser.inp"].clear()
        self.data_map["qser.inp"].extend(node_list)

        # Modify wqpsc.inp

        old_node_list = self.data_map["wqpsc.inp"]
        old_df_node_list = ConcentrationNode.get_df_node_list(old_node_list)
        df_node_list = [old_df_node_list[idx] for idx in idx_list if idx < len(old_df_node_list)]
        # outflow may point to a non-existed node, which denotes that its concentration is dependent on its location.

        assert len(old_node_list) == len(old_node_list)
        
        node_list = df_node_list

        self.data_map["wqpsc.inp"].clear()
        self.data_map["wqpsc.inp"].extend(node_list)

        # Modify wq3dwc.inp

        C34_1 = self.df_map_map["wq3dwc.inp"]["C34_1"]
        C34_2 = self.df_map_map["wq3dwc.inp"]["C34_2"]

        unmapped_idx_set = set(range(C08.shape[0])) - set(range(C34_2.shape[0]))
        idx_list_wq = [idx for idx in idx_list if idx not in unmapped_idx_set]
        old2new_wq = {old_idx: new_idx for new_idx, old_idx in enumerate(idx_list_wq)}

        assert C34_1["IWQPS"].iloc[0] == C34_1["NPSTMSR"].iloc[0]
        C34_1["IWQPS"].iloc[0] = C34_1["NPSTMSR"].iloc[0] = len(idx_list_wq)

        C34_2_selected = C34_2.iloc[idx_list_wq].copy()
        C34_2_selected["N"] = C34_2_selected["N"].map(lambda x:old2new_wq[x - 1] + 1)
        
        self.df_node_map_map["wq3dwc.inp"]["C34_2"].set_df(C34_2_selected)

        # Modify conc_adjust_inp

        for node in self.data_map["conc_adjust.inp"]:
            if isinstance(node, FlowAdjustMatrixNode):
                df = node.get_df()
                df = df.iloc[idx_list_wq]
                node.set_df(df)

        self.sync_from_data_map()

    def select_flow_soft(self, idx_list: List[int]):
        """
        Adjust efdc.inp only, leave useless data in qser.inp and wqpsc.inp alone.
        """

        warn("soft selecting is correct implemented at this time.")

        # Modify efdc.inp
        C07 = self.df_map_map["efdc.inp"]["C07"]
        C08 = self.df_map_map["efdc.inp"]["C08"]
        C09 = self.df_map_map["efdc.inp"]["C09"]

        assert C07["NQSIJ"].iloc[0] == C07["NQSER"].iloc[0]
        C07["NQSIJ"].iloc[0] = C07["NQSER"].iloc[0] = len(idx_list)

        C08_selected = C08.iloc[idx_list].copy()
        C09_selected = C09.iloc[idx_list].copy()

        self.df_node_map_map["efdc.inp"]["C08"].set_df(C08_selected)
        self.df_node_map_map["efdc.inp"]["C09"].set_df(C09_selected)

        # Modify wq3dwc.inp

        C34_1 = self.df_map_map["wq3dwc.inp"]["C34_1"]
        C34_2 = self.df_map_map["wq3dwc.inp"]["C34_2"]

        unmapped_idx_set = set(range(C08.shape[0])) - set(range(C34_2.shape[0]))
        idx_list_wq = [idx for idx in idx_list if idx not in unmapped_idx_set]

        assert C34_1["IWQPS"].iloc[0] == C34_1["NPSTMSR"].iloc[0]
        C34_1["IWQPS"].iloc[0] = C34_1["NPSTMSR"].iloc[0] = len(idx_list_wq)

        C34_2_selected = C34_2.iloc[idx_list_wq].copy()
        
        self.df_node_map_map["wq3dwc.inp"]["C34_2"].set_df(C34_2_selected)

        self.sync_from_data_map()

    def select_flow_flow0(self, idx_list: List[int]):
        drop_idx_list = self.get_flow_comp(idx_list)

        for drop_idx in drop_idx_list:
            self.df_map_map["qser.inp"]
            df = self.get_flow_node_list()[drop_idx].get_df()
            df["flow"] = 0

    def select_flow_qfactor0(self, idx_list: List[int]):
        drop_idx_list = self.get_flow_comp(idx_list)

        C08 = self.df_map_map["efdc.inp"]["C08"]
        for drop_idx in drop_idx_list:
            C08.loc[C08.index[drop_idx], "Qfactor"] = 0

    def select_flow(self, idx_list: List[int], mode="hard"):
        # return self.select_flow_soft()
        if mode == "hard":
            return self.select_flow_hard(idx_list)
        elif mode == "soft":
            raise ValueError("Soft selecting is not correctly implemented at this time")
        elif mode == "flow":
            return self.select_flow_flow0(idx_list)
        elif mode == "qfactor":
            return self.select_flow_qfactor0(idx_list)
        else:
            raise ValueError(f"Unknown mode: {mode}, valid mode is hard, flow and qfactor")

    def get_flow_comp(self, selected_idx):
        selected_idx_set = set(selected_idx)
        remain_idx_list = [idx for idx in range(self.count_flow()) if idx not in selected_idx_set]
        return remain_idx_list

    def drop_flow(self, drop_idx_list: List[int], mode="hard"):
        selected_idx = self.get_flow_comp(drop_idx_list)
        return self.select_flow(selected_idx, mode=mode)

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

    def to_data(self):
        return (self.data_map, self.df_node_map_map, self.df_map_map)

    def copy(self):
        # TODO: find a way to speed it up while still be correct.
        return deepcopy(self)

    def set_flow_range(self, flow_key, value, time_begin, time_end):
        df = self.df_map_map["qser.inp"][flow_key]

        if time_begin is None:
            time_begin = 0
        if time_end is None:
            time_end = df.shape[0] // 2

        # TODO: support float set, but that may damage current abstraction
        assert isinstance(time_begin, int), "set_flow_range support int only now"
        assert isinstance(time_end, int), "set_flow_range support int only now"

        idx_begin = 2 * time_begin
        idx_end = 2 * time_end
        
        index = df.index[idx_begin: idx_end]
        df.loc[index, "flow"] = value

    def set_flow_range_to_0(self, flow_key, time_begin, time_end):
        self.set_flow_range(flow_key, 0, time_begin, time_end)

    def set_flow_range_from_actioner(self, flow_key, actioner, time_begin, time_end):
        df = self.df_map_map["qser.inp"][flow_key]
        df_target = actioner.df_map_map["qser.inp"][flow_key]

        if time_begin is None:
            time_begin = 0
        if time_end is None:
            time_end = df.shape[0] // 2

        idx_begin = 2 * time_begin
        idx_end = 2 * time_end

        index = df.index[idx_begin: idx_end]
        df.loc[index, "flow"] = df_target.loc[index, "flow"]
