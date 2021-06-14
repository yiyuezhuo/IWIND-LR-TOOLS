from collections import OrderedDict
import pandas as pd
# import numpy as np
# from pathlib import Path
# from io import StringIO
from typing import List

from .common import ConcentrationNode, Node
from .utils import iter_strip, path_to_lines

# wqpsc_header_txt = "TIME	CHC	CHD	CHG	ROC	LOC	LDC	RDC	ROP	LOP	LDP	RDP	PO4	RON	LON	LDN	RDN	NH4	NO3	COD	DO	TAM	FCB	REA	MP1	MP2	TEM	DEP	ELE	DYE	NLC	PLC	PMC	PMD	PMG	BMC	PRC	FNC	FIC	TDC	DBN	DBP	SDI"
wqpsc_header_txt1 = "TIME	CHC	CHD	CHG	ROC	LOC	LDC	RDC	ROP	LOP	LDP	RDP	PO4	RON	LON	LDN	RDN	NH4	NO3"
wqpsc_header_txt2 = "usable_si unusable_si chemistry_demand_oxygen dissolved_oxygen active_metal EPEC dissolved_se grain_se"
wqpsc_header_txt = wqpsc_header_txt1 + " " + wqpsc_header_txt2
wqpsc_header = wqpsc_header_txt.split()

def _build_node(lines):
    return ConcentrationNode.from_str_list(lines, names=wqpsc_header)


@path_to_lines
def parse(lines: List[str]):
    """
    assert len(lines) >= 1
    it_lines = iter_strip(lines)

    content_lines = [next(it_lines)]

    node_list = []
    for line in it_lines:
        if len(line.strip().split()) == 7:
            node = _build_node(content_lines)
            node_list.append(node)
            content_lines = [line]
        else:
            content_lines.append(line)
    if len(content_lines) > 0:
        node_list.append(_build_node(content_lines))

    return node_list
    """

    len_lines = len(lines)
    i = 0

    node_list = []
    
    while i < len_lines:
        line = lines[i]
        l = line.strip()
        if len(l) == 0:
            i += 1
            continue
        ls = l.split("\t")
        nlines = int(ls[0])
        content_lines = lines[i: i + nlines + 1]
        node = _build_node(content_lines)
        node_list.append(node)

        i = i + nlines + 1
        
    return node_list


# get_df_node_list, get_df_node_map, get_df_map = ConcentrationNode.get_helpers()


"""
def get_df_node_list(node_list: List[Node]):
    return [node for node in node_list if isinstance(node, ConcentrationNode)]

def get_df_node_map(node_list: List[Node]):
    # get a "view" for node list to help navigation and select desired object.
    flow_node_map = {node.get_name(): node for node in get_df_node_list(node_list)}
    return flow_node_map

def get_df_map(node_list: List[Node]):
    return {k: flow_node.get_df() for k, flow_node in get_df_node_map(node_list).items()}
"""
