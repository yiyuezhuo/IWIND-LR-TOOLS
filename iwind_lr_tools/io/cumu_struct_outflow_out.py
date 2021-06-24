import pandas as pd

def parse(p):
    return pd.read_csv(p, delim_whitespace=True)
