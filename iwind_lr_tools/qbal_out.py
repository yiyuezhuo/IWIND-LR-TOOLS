from iwind_lr_tools.utils import path_to_lines
import pandas as pd
from typing import List
from warnings import warn
from io import StringIO

qbac_header_text_raw = "                                                 jday elev(m) qin(million-m3)qou(million-m3)  qctlo(million-m3)  qin(m) qou(m) qctlo(m) rain(m) eva(m)\n"

# "qin(million-m3)qou(million-m3)" -> "qin(million-m3) qou(million-m3)"
qbac_header_text = "jday elev(m) qin(million-m3) qou(million-m3)  qctlo(million-m3)  qin(m) qou(m) qctlo(m) rain(m) eva(m)"
qbac_header = qbac_header_text.split()

@path_to_lines
def parse(lines: List[str]):
    if lines[0] != qbac_header_text_raw: 
        warn(f"qbal header is not compatible.\n Given:\n{lines[0]}\n,Expected:\n{qbac_header_text_raw}\n, parser may still work but do it at your own risk.")
        # assert False
    buf = StringIO("\n".join(lines[1:]))
    return pd.read_csv(buf, header=None, delim_whitespace=True, names=qbac_header)
