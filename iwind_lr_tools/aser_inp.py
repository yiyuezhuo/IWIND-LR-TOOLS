
from io import StringIO
import pandas as pd

from .utils import path_to_lines

aser_header_txt = "time	pressure	temperature	humidity	rain	evaporate	sun	cloud"
aser_header = aser_header_txt.split("\t")   

def parse_aser_py(p):
    return pd.read_csv(p, comment="#", skipfooter=1, delim_whitespace=True, header=None, names=aser_header, engine="python")

@path_to_lines
def parse_aser_c(lines):
    df = pd.read_csv(StringIO("\n".join(lines[:-1])), comment="#", sep="\t", header=None, names=aser_header)
    return df

@path_to_lines
def parse_aser_c2(lines):
    df = pd.read_csv(StringIO("\n".join(lines[:-1])), comment="#", delim_whitespace=True, header=None, names=aser_header)
    return df
