"""
Abstract parser for "master input files" (say, very heterogeneity) efdc.inp and wq3dwc.inp
"""

from iwind_lr_tools.io.utils import path_to_lines
from os import path
import pandas as pd
from copy import deepcopy
from collections import OrderedDict

from .common import DataFrameNode, CommentNode


def generate_parse(card_info_dsl, forward_lookup_map, length_map_init):

    headers_map = OrderedDict()
    for row in card_info_dsl.split("\n"):
        if len(row) == 0:
            continue
        rl = row.split()
        headers_map[rl[0]] = rl[1:]
    
    @path_to_lines
    def parse(lines, *, extra_length_map=None):
        length_map = deepcopy(length_map_init)
        if extra_length_map is not None:
            length_map.update(extra_length_map)

        it_lines = (line.strip() for line in lines)

        df_remain = []
        comment_remain = []

        """
        length_map = {
            "C02": 1,
            "C03": 1,
            "C04": 1,
            "C05": 1,
            "C06": 1,
            "C07": 1,
            
            "C13": 1,
            
            "C15": 1,
            
            "C17": 1
        }
        """


        node_list = []
        for key, fields in headers_map.items():
            if length_map[key] == 0:
                df = pd.DataFrame([], columns=fields)
                df_node = DataFrameNode.from_dataframe(df)
                node_list.append(df_node)
                continue
            for line in it_lines:
                if len(line) == 0 or line[0] in {"#", "C"}:
                    comment_remain.append(line)
                    continue
                df_remain.append(line)
                if len(df_remain) == length_map[key]:
                    
                    df_node = DataFrameNode.from_str_list(df_remain)
                    comment_node = CommentNode.from_str_list(comment_remain)
                    df_remain = []
                    comment_remain = []
                    node_list.append(comment_node)
                    node_list.append(df_node)
                    
                    df_node.set_header(fields)
                    df_node.set_name(key)
                    df = df_node.get_df()
                    if key in forward_lookup_map:
                        for field, set_cards in forward_lookup_map[key].items():
                            value = df[field].iloc[0]
                            for set_card in set_cards:
                                length_map[set_card] = value
                    
                    break
            else:
                ValueError("Unexpected reaching end of file")
        assert len(df_remain) == 0 and len(comment_remain) == 0
        # collect trailing comment (if any)
        lines_trailing = list(it_lines)
        if len(lines_trailing) > 0:
            commend_node = CommentNode.from_str_list(lines_trailing)
            node_list.append(commend_node)

        return node_list

    return parse
