
from collections import OrderedDict
import pandas as pd
from typing import List

from .utils import path_to_lines
from .common import Node, DataFrameNode, CommentNode


card_info_dsl = """
C02  ISRESTI   ISDRY  ISIMTMP  ISIMWQ  ISIMDYE  TEMO  RKDYE  IASWRAD  SWRATNF   REVCHC  DABEDT  TBEDIT HTBED1 HTBED2    KBHM
C03  NTC  NTSPTC  TBEGIN
C04  IC  JC   LVC ISMASK KC ZBRADJ HMIN    HADJ  HDRY  HWET  BELADJ
C05     K        DZC
C06 AHO  AHD  AVO    ABO    AVMN  ABMN  VISMUD AVBCON ZBRWALL
C07 NWSER NASER  NTSER  NQSIJ  NQSER  NQCTL  NQCTLT  NQWR  NQWRSR
C08 IQS  JQS   QSSE NQSMFF NQSERQ NT-  ND-  Qfactor
C09  TEM     DYE
C10 IQCTLU JQCTLU IQCTLD JQCTLD NQCTYP NQCTLQ NQCMUL NQC_U NQC_D BQC_U  BQC_D	CREST	SEEP
C11 IWRU JWRU KWRU IWRD JCWRD KWRD  QWRE NQW_RQ NQWR_U NQWR_D BQWR_U BQWR_D	WD_BEGIN  WD_END
C12 TEMP  DYEC
C13 ISPD NPD  NPDRT   NWPD ISLRPD ILRPD1 ILRPD2 JLRPD1  JLRPD2 IPLRPD
C14   RI   RJ   RK
C15 ISTMSR  MLTMSR  NBTMSR  NSTMSR     NWTMSR
C16 ILTS JLTS  MTSP MTSC MTSA MTSUE MTSUT MTSU MTSQE MTSQ  CLTS
C17   WID IRELH   RAINCVT   EVAPCVT  SOLRCVT  CLDCVT   TASER   TWSER    WSADJ    WNDD    STANAME
"""

headers_map = OrderedDict()
for row in card_info_dsl.split("\n"):
    if len(row) == 0:
        continue
    rl = row.split()
    headers_map[rl[0]] = rl[1:]

forward_lookup_map = {
    "C07": {
        "NQSIJ": ["C08", "C09"],
        "NQCTL": ["C10"],
        "NQWR": ["C11", "C12"]
    },
    "C13": {
        "NPD": ["C14"]
    },
    "C15": {
        "MLTMSR": ["C16"]
    }
}


@path_to_lines
def parse(lines):
    it_lines = (line.strip() for line in lines)

    df_remain = []
    comment_remain = []

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

def get_df_node_map(node_list: List[Node]):
    # get a "view" for node list to help navigation and select desired object.
    dataframe_node_list = [node for node in node_list if isinstance(node, DataFrameNode)]
    df_node_map = {k: node for k, node in zip(headers_map, dataframe_node_list)}
    return df_node_map

def get_df_map(node_list: List[Node]):
    return {k: node.get_df() for k, node in get_df_node_map(node_list).items()}
