from collections import OrderedDict
import pandas as pd
# import numpy as np
# from pathlib import Path
from io import StringIO

# wqpsc_header_txt = "TIME	CHC	CHD	CHG	ROC	LOC	LDC	RDC	ROP	LOP	LDP	RDP	PO4	RON	LON	LDN	RDN	NH4	NO3	COD	DO	TAM	FCB	REA	MP1	MP2	TEM	DEP	ELE	DYE	NLC	PLC	PMC	PMD	PMG	BMC	PRC	FNC	FIC	TDC	DBN	DBP	SDI"
wqpsc_header_txt1 = "TIME	CHC	CHD	CHG	ROC	LOC	LDC	RDC	ROP	LOP	LDP	RDP	PO4	RON	LON	LDN	RDN	NH4	NO3"
wqpsc_header_txt2 = "usable_si unusable_si chemistry_demand_oxygen dissolved_oxygen active_metal EPEC dissolved_se grain_se"
wqpsc_header_txt = wqpsc_header_txt1 + " " + wqpsc_header_txt2
wqpsc_header = wqpsc_header_txt.split()


def parse_wqpsc_inp(p):
    with open(p, encoding="utf8") as f:
        lines = f.readlines()

    len_lines = len(lines)
    i = 0
    
    rd = OrderedDict()

    while i < len_lines:
        line = lines[i]
        l = line.strip()
        if len(l) == 0:
            i += 1
            continue
        ls = l.split("\t")
        nlines = int(ls[0])
        name = ls[-1]
        # mat = np.loadtxt(StringIO("\n".join(lines[i+1:i + nlines + 1])))
        buf = StringIO("\n".join(lines[i+1:i + nlines + 1]))
        # df = pd.read_csv(buf, header=None, names=wqpsc_header, delim_whitespace=True) # or delim_whitespace=True
        df = pd.read_csv(buf, header=None, names=wqpsc_header, sep="\t") # or delim_whitespace=True
        df.reset_index(drop=True, inplace=True)

        i = i + nlines + 1
        rd[name] = df
        
    return rd
