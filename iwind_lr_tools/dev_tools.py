"""
Import some useful names into develoment session (Jupyter/IPython)
The module requires `requirements-dev.txt` (`pip install -r requirements-dev.txt`) to run.
This module is assumed to be very volatile so that other module should not depends on it.
If some code snippet is interesting, copy it but not import it.
"""

import iwind_lr_tools
from pathlib import Path
import json
from datetime import timedelta, datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from copy import deepcopy
import pickle
import logging

from iwind_lr_tools import create_simulation, run_simulation, dumps, Actioner, Runner, run_batch, restart_batch, \
    restart_iterator, start_iterator
from .collector import get_all, get_model
# from .load_stats import get_aligned_dict, get_aligned_df, stats_load, drop_obsession_edge_int, append_WQWCTS_OUT, drop_obsession_WQCTS_out
from .load_stats import get_time_indexed_df, get_time_aligned_map, get_aligned_series_list, get_aligned_df, stats_load, Pedant
from .runner import fork

# from .extract_non_modified_files import extract_non_modified_files

logger = logging.getLogger()
logger.setLevel(logging.INFO)

interested_keys = ["ROP", "LOP", "LDP", "RDP", "PO4"]

class Period:
    def __init__(self, date_begin, date_end):
        self.date_begin = date_begin
        self.date_end = date_end

    def limit(self, df, date_key='date'):
        return df[(df[date_key] > self.date_begin) & (df[date_key] < self.date_end)]

def zscore(x):
    return (x - x.mean()) / x.std()

plt.rcParams["figure.figsize"] = [12, 6]
plt.rcParams['figure.facecolor'] = 'white'


def plot_two_y(x, y1, y2, x_label="x", y1_label=None, y2_label=None, alpha=0.9, markersize=6):
    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ax1.plot(x, y1, 'go-', alpha=alpha, markersize=markersize)
    ax2.plot(x, y2, 'bo-', alpha=alpha, markersize=markersize)

    ax1.set_xlabel(x_label)
    ax1.set_ylabel(y1_label, color='green')
    ax2.set_ylabel(y2_label, color='blue')

def infer_x_axis(df, x_label=None):
    if x_label is None and df.index.name == "date":
        x_label = "date"
        x = df.index
    else:
        if x_label is None:
            x_label = "date" if "date" in df else "time"
        x = df[x_label]
    return x, x_label


def plot_aligned_df_compare(df, key1, key2, alpha=0.9, x_label=None, markersize=6):
    x, x_label = infer_x_axis(df, x_label)
    
    y1 = df[key1]
    y2 = df[key2]

    plot_two_y(x, y1, y2, x_label=x_label, y1_label=key1, y2_label=key2, alpha=alpha, markersize=markersize)

def plot_aligned_df_parallel(df, keys=None, normed=False, x_label=None, cov=None, cov_kwargs=None, **kwargs):
    x, x_label = infer_x_axis(df, x_label)

    if keys is None:
        keys = df.columns
    
    for key in keys:
        y = zscore(df[key]) if normed else df[key]
        plt.plot(x, y, 'o-', label=key, **kwargs)

    if cov is not None:
        if cov_kwargs is None:
            cov_kwargs = {}
        plt.twinx().plot(x, df[cov], **cov_kwargs)
    
    plt.xlabel(x_label)
    plt.legend()

def show_full_df(df):
    from IPython.display import display, HTML
    display(HTML(df.to_html()))

def first(iterable):
    return next(iter(iterable))
