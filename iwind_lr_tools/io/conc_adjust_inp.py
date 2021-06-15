
from iwind_lr_tools.io.utils import path_to_lines
from typing import List
import pandas as pd

from .utils import path_to_lines, iter_strip
from .common import Node, FlowAdjustMatrixNode, DataFrameNode, CommentNode

header_s = "Bc Bd Bg ROC LOC LDOC RDC ROP LOP LDOP RDP PO4t RPON LON LDON RDN NH4 NO3 SU SA COD DO TAM FCB DSE PSE"
header = header_s.split()

def fetch_non_comment_line(lines, node_list):
    comment_remain = []

    for line in iter_strip(lines):
        if len(line)==0 or line[0] in {"#", "C"}:
            comment_remain.append(line)
        else:
            if len(comment_remain) > 0:
                node_list.append(CommentNode(comment_remain))
                comment_remain = []
            yield line

@path_to_lines
def parse(lines, *, nrows):
    node_list = []
    it = fetch_non_comment_line(lines, node_list)
    num_matrix_str: str = next(it)
    assert num_matrix_str.isdigit()
    num_matrix = int(num_matrix_str)
    node = DataFrameNode(pd.DataFrame({"num_matrix": [num_matrix]}))
    node_list.append(node)

    for _ in range(num_matrix):
        matrix_node_lines = []
        for _ in range(nrows + 1):
            matrix_node_lines.append(next(it))
        node = FlowAdjustMatrixNode(matrix_node_lines, names=header)
        node_list.append(node)
    for _ in it:
        raise ValueError("Unexpected extra data")
    return node_list

def parse_dep(data_map):
    C07 = DataFrameNode.get_df_map(data_map["efdc.inp"])["C07"]
    NQSIJ = C07["NQSIJ"].iloc[0]
    NQSER = C07["NQSER"].iloc[0]
    assert NQSIJ == NQSER
    nrows = NQSIJ
    return dict(nrows=nrows)

