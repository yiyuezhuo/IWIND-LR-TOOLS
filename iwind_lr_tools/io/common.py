import pandas as pd
from typing import List
from io import StringIO


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

class DataFrameNode(Node):
    def __init__(self, df):
        self.obj = df
    
    @staticmethod
    def from_str_list(str_list: List[str]):
        """
        if len(str_list) == 1 and str_list[0] == "":
            return DataFrameNode(None) # "empty" dataframe
        """
        return DataFrameNode(_read_csv_from_row_list(str_list))
    
    def to_str(self):
        """
        if self.obj is None:
            return ""
        """
        buf = StringIO()
        self.obj.to_csv(buf, header=False, sep=" ", index=False)
        buf.seek(0)
        text = buf.read()
        if len(text) > 0:
            assert text[-1] == "\n"
            text = text[:-1]
        return text[:-1]

    def set_header(self, header):
        self.obj.columns = header

    def get_df(self) -> pd.DataFrame:
        return self.obj

class FlowNode(Node): # qser main data
    def __init__(self, lines):
        
        self.spec = lines[0].strip().split()
        assert len(self.spec) == 8, f"wrong spec, {self.spec}"
        self.depth_line = lines[1].strip()
        assert int(self.depth_line.strip()) == 1, f"This version assume target has only 1 depth, but found {self.depth_line}"
        
        self.table_data = lines[2:]
        buf = StringIO('\n'.join(self.table_data))
        self.df = pd.read_csv(buf, header=None, names=["time", "flow"], delim_whitespace=True)
        
        self.length = len(lines) - 2
        self.explicit_length = int(self.spec[1]) # 0 or something like 5856
        
        if self.explicit_length != 0:
            assert self.explicit_length == self.length
            
        self.obj = (self.spec, self.depth_line, self.df)
            
    @staticmethod
    def from_str_list(str_list: List[str]):
        return FlowNode(str_list)
    
    def to_str(self):
        # TODO: Is leftpad 2 tabs necessary?
        buf = StringIO()
        self.df.to_csv(buf, index=False, sep="\t", header=False)
        buf.seek(0)
        return "\n".join(["\t".join(self.spec), self.depth_line, buf.read()])
    
    def get_df(self):
        return self.df


def dumps(node_list: List[Node]):
    """
    All "projection" files should follow List[Node] format to prevent fragile code somewhat.
    """
    return "\n".join(node.to_str() for node in node_list)
