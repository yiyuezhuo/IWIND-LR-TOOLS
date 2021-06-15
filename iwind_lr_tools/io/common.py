import pandas as pd
from typing import List
from io import StringIO

def df_to_str(df, index=False, header=False, **kwargs):
    buf = StringIO()
    df.to_csv(buf, index=index, header=header, line_terminator="\n", **kwargs)
    buf.seek(0)
    return buf.read()


class Node:
    @staticmethod
    def from_str_list(str_list: List[str]):
        raise NotImplementedError
        
    def to_str_list(self) -> List[str]:
        raise NotImplementedError

    @staticmethod
    def from_str(_str: str):
        return Node.from_str_list(_str.split("\n"))
        
    def to_str(self) -> str:
        return "\n".join(self.to_str_list())
    
    def __str__(self):
        return f"{self.__class__}:\n {self.obj.__str__()}" 
    
    def __repr__(self):
        return f"{self.__class__}:\n {self.obj.__repr__()}"
    


class CommentNode(Node):
    def __init__(self, comment_list: List[str]):
        self.obj = comment_list
        
    @staticmethod
    def from_str_list(str_list: List[str]):
        return CommentNode(str_list)
    
    def to_str_list(self) -> List[str]:
        return self.obj

def _read_csv_from_row_list(row_list):
    return pd.read_csv(StringIO("\n".join(row_list)), header=None, delim_whitespace=True)

class AbstractDataFrameNode(Node):
    @classmethod
    def get_df_node_list(cls, node_list: List[Node]):
        return [node for node in node_list if isinstance(node, cls)]

    @classmethod
    def get_df_node_map(cls, node_list: List[Node]):
        # get a "view" for node list to help navigation and select desired object.
        flow_node_map = {node.get_name(): node for node in cls.get_df_node_list(node_list)}
        return flow_node_map

    @classmethod
    def get_df_map(cls, node_list: List[Node]):
        return {k: flow_node.get_df() for k, flow_node in cls.get_df_node_map(node_list).items()}

    @classmethod
    def get_helpers(cls):
        return cls.get_df_node_list, cls.get_df_node_map, cls.get_df_map


class DataFrameNode(AbstractDataFrameNode):
    def __init__(self, df, name=None):
        self.obj = df
        self._name = name
    
    @staticmethod
    def from_str_list(str_list: List[str]):
        """
        if len(str_list) == 1 and str_list[0] == "":
            return DataFrameNode(None) # "empty" dataframe
        """
        return DataFrameNode(_read_csv_from_row_list(str_list))

    @staticmethod
    def from_dataframe(df: pd.DataFrame):
        return DataFrameNode(df)
    
    def to_str(self):
        """
        if self.obj is None:
            return ""
        """
        df = self.obj
        """
        buf = StringIO()
        df.to_csv(buf, header=False, sep=" ", index=False, line_terminator="\n")
        buf.seek(0)
        text = buf.read()
        """
        text = df_to_str(df, sep=" ")
        if len(text) > 0:
            assert text[-1] == "\n"
            text = text[:-1]
        return text

    def set_header(self, header):
        self.obj.columns = header

    def get_df(self) -> pd.DataFrame:
        return self.obj

    def set_df(self, df):
        self.obj = df

    def set_name(self, name:str):
        self._name = name

    def get_name(self):
        return self._name

class FlowNode(AbstractDataFrameNode): # qser main data
    def __init__(self, lines):
        
        self.spec = lines[0].strip().split()
        assert len(self.spec) == 8, f"wrong flow spec, {self.spec}"
        self.depth_line = lines[1].strip()
        assert int(self.depth_line.strip()) == 1, f"This version assume target has only 1 depth, but found {self.depth_line}"
        
        self.table_data = lines[2:]
        buf = StringIO('\n'.join(self.table_data))
        self.df = pd.read_csv(buf, header=None, names=["time", "flow"], delim_whitespace=True)
        
        self.length = len(lines) - 2
        self.somewhat_length = int(self.spec[1]) # 0 or something like 5856
        
        # if self.somewhat_length != 0:
        #     assert self.somewhat_length == self.length
            
        self.obj = (self.spec, self.depth_line, self.df)
            
    @staticmethod
    def from_str_list(str_list: List[str]):
        return FlowNode(str_list)
    
    def to_str(self):
        # TODO: Is leftpad 2 tabs necessary?
        df = self.df
        """
        buf = StringIO()
        df.to_csv(buf, index=False, sep="\t", header=False, line_terminator="\n")
        buf.seek(0)
        s = buf.read()
        """
        s = df_to_str(df, sep="\t")
        return "\n".join(["\t".join(self.spec), self.depth_line, s])
        
    
    def get_df(self):
        return self.df

    def get_name(self):
        return self.spec[-1]

class ConcentrationNode(AbstractDataFrameNode):
    """
    While we can extract some common parts from `FlowNode` and `ConcentrationNode`,
    the benefit to do it is too small so the code is just copied and modified.
    """
    def __init__(self, lines, *, names):
        self.spec = lines[0].strip().split()
        assert len(self.spec) == 7, f"wrong concentration spec, {self.spec}"

        self.table_data = lines[1:]

        buf = StringIO('\n'.join(self.table_data))
        self.df = pd.read_csv(buf, header=None, names=names, delim_whitespace=True)
        
        self.length = len(lines) - 1

        self.obj = (self.spec, self.df)

    @staticmethod
    def from_str_list(str_list: List[str], *, names):
        return ConcentrationNode(str_list, names=names)
    
    def to_str(self):
        # TODO: Is leftpad 2 tabs necessary?
        df = self.df
        s = df_to_str(df, sep="\t")
        return "\n".join(["\t".join(self.spec), s])
        
    def get_df(self):
        return self.df

    def get_name(self):
        return self.spec[-1]


class FlowAdjustMatrixNode(Node):
    def __init__(self, lines, *, names):
        time_line = lines[0].strip().split()
        assert len(time_line) == 1
        self.time = float(time_line[0])
        
        self.table_data = lines[1:]
        buf = StringIO('\n'.join(self.table_data))
        self.df = pd.read_csv(buf, header=None, names=names, delim_whitespace=True)
        self.obj = (self.time, self.df)

    def get_df(self):
        return self.df

    @staticmethod
    def from_str_list(str_list: List[str], *, names):
        return FlowAdjustMatrixNode(str_list, names=names)

    def to_str(self):
        df = self.df
        s = df_to_str(df, sep="\t")
        return "\n".join([f"{self.time}", s])

    def set_df(self, df):
        self.df = df


def dumps(node_list: List[Node]):
    """
    All "projection" files should follow List[Node] format to prevent fragile code somewhat.
    """
    return "\n".join(node.to_str() for node in node_list)

"""
class NodeListSuit:
    def __init__(self, node_list: List[Node]):
        self._node_list = node_list
        self.sync_with_node_list()

    def sync_with_node_list(self):
        if not hasattr(self, "_df_node_map"):
            self._df_node_map = {}
        if not hasattr(self, "_df_map"):
            self._df_node_map = {}
        
        self.df_node_map.clear()
        self.df_node_map.update(self._get_df_node_map())
        self.df_map.clear()
        self.df_map.update(self._get_df_map())

    @property
    def node_list(self):
        return self._node_list

    @node_list.setter
    def node_list(self, val):
        self._node_list.clear()
        self._node_list.extend(val)
        self.sync_with_node_list()

    def sync_with_node_list():
        pass

    def dumps(self):
        return dumps(self.node_list)

    def _get_df_node_map(self):
        raise NotImplementedError

    def _get_df_map(self):
        raise NotImplementedError
"""

