
import pandas as pd
import numpy as np
from warnings import warn

from .actioner import Actioner


def get_time_indexed_df(df:pd.DataFrame, time_key:str, dt:pd.Timestamp):
    df = df.set_index(dt + pd.TimedeltaIndex(df[time_key], unit="D")).resample("H").nearest()
    df.index = df.index.rename("date")
    return df

def get_time_aligned_map(data_map, df_node_map_map, df_map_map, out_map=None, dt:pd.Timestamp=None):
    """
    Align time
    """
    if dt is None:
        warn("While it is possible to use a default dt value to create a relative timestamp, it is recommented to explicity specify one.")
        dt = pd.to_datetime("1989-6-4")

    rd = {}

    aser_inp = get_time_indexed_df(data_map["aser.inp"], "time", dt)
    rd["aser.inp"] = aser_inp

    if out_map is not None:
        qbal_out = get_time_indexed_df(out_map["qbal.out"], "jday", dt)
        # qbal_out = (qbal_out.shift(-1) - qbal_out).dropna()
        rd["qbal.out"] = qbal_out

        WQWCTS_OUT = {}
        for key, df in out_map["WQWCTS.OUT"].groupby(["I", "J", "K"]):
            WQWCTS_OUT[key] = get_time_indexed_df(df, "TIME", dt)
            # WQWCTS_OUT[key] = sdf.rolling(2).mean().shift(-1).dropna()
        rd["WQWCTS.OUT"] = WQWCTS_OUT

        cumu_struct_outflow_out = get_time_indexed_df(out_map["cumu_struct_outflow.out"], "Jday", dt)
        rd["cumu_struct_outflow.out"] = cumu_struct_outflow_out
    
    wqpsc_inp = {}
    for flow_name, df in df_map_map["wqpsc.inp"].items():
        wqpsc_inp[flow_name] = get_time_indexed_df(df.iloc[::2], "TIME", dt)
    rd["wqpsc.inp"] = wqpsc_inp
    
    qser_inp = {}
    for flow_name, df in df_map_map["qser.inp"].items():
        qser_inp[flow_name] = get_time_indexed_df(df.iloc[::2], "time", dt)
    rd["qser.inp"] = qser_inp

    return rd

def diff(ser:pd.Series) -> pd.Series:
    """
    For accumulative quantities
    t1 x1
    t2 x2
    t3 x3
    ->
    t1 x2-x1
    t2 x3-x2
    """
    return ser.diff(1).shift(-1).dropna()

def roll(ser:pd.Series) -> pd.Series:
    """
    For timestamp encoding, use middle value as approximation for that interval.
    t1 x1
    t2 x2
    t3 x3
    ->
    t1 (x1+x2)/2
    t2 (x2+x3)/2
    """
    return ser.rolling(2).mean().shift(-1).dropna()

def fluctuation_smooth(seq: pd.Series)->pd.Series:
    """
    The model sometime give [high, 0] sequence due to so called numerical problem when water is too "enough".
    However, the de-fluctuation "true" value may be interesting for decision alogrithom, 
    thus this function try to give an approximation by smoothing like: 

    0.2, 0.3, 0.8, 0.0, 1.2, 0.0, ...
    ->
    0.2, 0.3, 0.4, 0.5, 0.6, 0.7, ...

    Note: This pattern may fail for right 
    """
    seq = seq.copy()
    arr = seq.to_numpy()

    idx0 = np.where(arr == 0)[0]
    if len(idx0) == 0:
        return seq
    
    if idx0[0] == 0:
        idx0 = idx0[1:]
    idxp = idx0 - 1

    smoothed = (arr[idx0] + arr[idxp]) / 2

    arr[idx0] = arr[idxp] = smoothed
    
    return seq


def get_aligned_series_list(data_map, df_node_map_map, df_map_map, out_map=None, dt:pd.Timestamp=None,
                     wq_keys=None, aser_keys=None, smooth_wqpsc_inp=True):
    """
    Align time, unit and specify 'uniform' approximation for period
    """
    if wq_keys is None:
        wq_keys = ["ROP"]

    if aser_keys is None:
        aser_keys = ["rain"]

    aligned_map = get_time_aligned_map(data_map, df_node_map_map, df_map_map, out_map=out_map, dt=dt)

    ser_list = [aligned_map["aser.inp"]["time"]]

    if aser_keys is not None:
        for key in aser_keys:
            ser_list.append(aligned_map["aser.inp"][key])

    if out_map is not None:
        ser = - diff(aligned_map["qbal.out"]["qctlo(million-m3)"]) * 1_000_000 # million-m3 -> m3
        ser_list.append(ser.rename("flow_qctlo"))

        ser = roll(aligned_map["qbal.out"]["elev(m)"])
        ser_list.append(ser.rename("elev"))

        for ijk_key, df in aligned_map["WQWCTS.OUT"].items():
            for wq_key in wq_keys:
                key = f"{wq_key}_{ijk_key}"
                ser = roll(df[wq_key])
                ser_list.append(ser.rename(key))

        for key, ser in aligned_map["cumu_struct_outflow.out"].items():
            if key == "Jday":
                continue
            ser = diff(ser)
            ser_list.append(ser.rename("struct_" + ser.name))

    for flow_key, df in aligned_map["wqpsc.inp"].items():
        for wq_key in wq_keys:
            key = f"{wq_key}_{flow_key}"
            ser = df[wq_key]
            if smooth_wqpsc_inp:
                ser = fluctuation_smooth(ser)
            ser_list.append(ser.rename(key))

    for flow_key, df in aligned_map["qser.inp"].items():
        key = f"flow_{flow_key}"
        ser = df["flow"] * 3600 # m3/s -> m3/h
        ser_list.append(ser.rename(key))

    return ser_list

def get_aligned_df(data_map, df_node_map_map, df_map_map, out_map=None, *, dropna=True, **kwargs):
    ser_list = get_aligned_series_list(data_map, df_node_map_map, df_map_map, out_map=out_map, **kwargs)
    # TODO: keep only time overlap part? Set all index to the same name?
    df = pd.concat(ser_list, axis=1)
    if dropna:
        df = df.dropna()
    return df

def stats_load(df, df_limit, wq_key, flow_keys, qctlo_key="qctlo", pump_key="pump_outflow"):
    """
    df_ori should has un-modified qser.
    """
    
    dd = {}
    dd[f"load_{qctlo_key}"] = df[f"{wq_key}_{qctlo_key}"] * df[f"flow_{qctlo_key}"]
    for flow_key in flow_keys:
        diff = df_limit[f"flow_{flow_key}"] - df[f"flow_{flow_key}"]
        dd[f"load_{flow_key}"] = df[f"{wq_key}_{flow_key}"] * diff
    dd[f"load_{pump_key}"] = df[f"flow_{pump_key}"] * df[f"{wq_key}_{pump_key}"]

    rdf = pd.DataFrame(dd)
    rdf["load_flow"] = rdf[[f"load_{flow_key}" for flow_key in flow_keys]].sum(axis=1)
    rdf["load_total"] = rdf[f"load_{qctlo_key}"] + rdf["load_flow"] + rdf[f"load_{pump_key}"]
    return rdf

def append_df(df_left, df_right):
    """
    This function increase restarting error, as some info but not all can be given as exact value instead of interpoated value.
    But this function doesn't facilitate this info.
    """
    return df_left.append(df_right).resample("H").interpolate()


class Pedant:
    """
    A Pedant is a man who just want to attach everything with a timestamp.

    `postprocess` will add some specific data related columns to df which is used by `get_load_df`.
    """
    def __init__(self, dt: pd.Timestamp, *, wq_keys=None, aser_keys=None, smooth_wqpsc_inp=True, dropna=True,
                postprocess=None,
                df_limit=None, wq_key=None, flow_keys=None, qctlo_key="qctlo", pump_key="pump_outflow",
                total_begin_time=None):
        self.dt = dt

        self.wq_keys = wq_keys
        self.aser_keys = aser_keys
        self.smooth_wqpsc_inp = smooth_wqpsc_inp
        self.dropna = dropna

        self.postprocess = postprocess if postprocess is not None else lambda df:df
 
        self.df_limit = df_limit
        self.wq_key = wq_key
        self.flow_keys= flow_keys

        self.qctlo_key = qctlo_key
        self.pump_key = pump_key

        self.total_begin_time = int(total_begin_time) if total_begin_time is not None else None

    def get_df(self, actioner:Actioner, out_map=None):
        """
        There're three kinds of df:
        * Given actioner only
        * Given actioner and out_map
        * Given actioner, out_map and pedant has a non-trival postprocess

        Only The last one can be passed to get_load_df to get a valid load_df.
        """
        df = get_aligned_df(*actioner.to_data(), out_map, dt=self.dt, 
            wq_keys=self.wq_keys, aser_keys=self.aser_keys, smooth_wqpsc_inp=self.smooth_wqpsc_inp,
            dropna=self.dropna)
        df = self.postprocess(df)
        return df

    def get_df_load(self, df):
        assert self.get_load_df_ready()
        return stats_load(df, self.df_limit, self.wq_key, self.flow_keys, 
            qctlo_key=self.qctlo_key, pump_key=self.pump_key)

    def get_load_df_ready(self):
        return self.df_limit is not None and self.wq_key is not None and self.flow_keys is not None

    def __repr__(self):
        return f"Pedant(id={id(self)}, dt={self.dt}, postprocess={self.postprocess}, get_load_df_ready->{self.get_load_df_ready()}, total_begin_time={self.total_begin_time})"

    def hour_to_dt(self, hours):
        return self.dt + pd.DateOffset(days=self.total_begin_time) + pd.DateOffset(hours=int(hours))

    def dt_to_hour(self, dt):
        offset = dt - self.dt - pd.DateOffset(days=self.total_begin_time)
        return offset.hours
