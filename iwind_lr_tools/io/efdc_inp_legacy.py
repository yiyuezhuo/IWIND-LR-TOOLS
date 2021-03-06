from pathlib import Path
from enum import Enum
import pandas as pd
from io import StringIO
from typing import List

from .utils import path_to_text
from .common import Node, DataFrameNode, CommentNode, _read_csv_from_row_list #, NodeListSuit
from collections import OrderedDict

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

    
class ParserState(Enum):
    begin = 0
    comment = 1
    dataframe = 2
    

def pre_parse(text:str):
    state = ParserState.begin
    
    obj_list = []
    comment_building = []
    dataframe_building = []
    for row in text.split("\n"):
        r = row.strip()
        if len(r) == 0 or r[0] == "#" or r[0] == "C":
            if state == ParserState.dataframe:
                df = DataFrameNode.from_str_list(dataframe_building)
                obj_list.append(df)
                dataframe_building = []
            state = ParserState.comment
            comment_building.append(row)
        else:
            if state == ParserState.comment:
                obj_list.append(CommentNode.from_str_list(comment_building))
                comment_building = []
            state = ParserState.dataframe
            dataframe_building.append(r)
    if len(comment_building) > 0:
        obj_list.append(comment_building)
    if len(dataframe_building) > 0:
        obj_list.append(_read_csv_from_row_list(dataframe_building))
        
    return obj_list

def post_parse(node_list:List[Node]):
    # apply headers and fill "header only" table nodes
    it_node = iter(node_list)
    it_header = iter(headers_map.items())


    def check(df, key, empty_card_name_list):
        if df[key].iloc[0] > 0:
            for expect_card_name in empty_card_name_list:
                # node = next(it_node)
                for node in it_node:
                    if isinstance(node, CommentNode):
                        yield node
                    elif isinstance(node, DataFrameNode):
                        break
                    else:
                        raise ValueError(f"Unexpected Node type {type(node)}: {node}")
                card_name, header = next(it_header)
                assert expect_card_name == card_name
                node.set_header(header)
                yield node
        else:
            for expect_card_name in empty_card_name_list:
                card_name, header = next(it_header)
                assert expect_card_name == card_name
                node = DataFrameNode(pd.DataFrame({}, columns=header))
                yield node

    for node in it_node:
        if isinstance(node, CommentNode):
            yield node
        elif isinstance(node, DataFrameNode):
            card_name, header = next(it_header)
            node.set_header(header)
            yield node
            df = node.get_df()
            if card_name == "C07":
                yield from check(df, "NQSIJ", ["C08", "C09"])
                yield from check(df, "NQCTL", ["C10"])
                yield from check(df, "NQWR", ["C11", "C12"])
            elif card_name == "C13":
                yield from check(df, "NPD", ["C14"])
            elif card_name == "C15":
                yield from check(df, "MLTMSR", ["C16"])


@path_to_text
def parse(text:str):
    node_list = pre_parse(text)
    return list(post_parse(node_list))


def get_df_node_map(node_list: List[Node]):
    # get a "view" for node list to help navigation and select desired object.
    dataframe_node_list = [node for node in node_list if isinstance(node, DataFrameNode)]
    df_node_map = {k: node for k, node in zip(headers_map, dataframe_node_list)}
    return df_node_map

def get_df_map(node_list: List[Node]):
    return {k: node.get_df() for k, node in get_df_node_map(node_list).items()}

"""
class EfdcSuit(NodeListSuit):

    def _get_df_node_map(self):
        return get_df_node_map(self.node_list)

    def _get_df_map(self):
        return get_df_map(self.node_list)

    @property
    def simulation_length(self):
        return self._df_map["C03"]["NTC"].iloc[0]

    @simulation_length.setter
    def simulation_length(self, val):
        self._df_map["C03"]["NTC"].iloc[0] = val
"""
    
