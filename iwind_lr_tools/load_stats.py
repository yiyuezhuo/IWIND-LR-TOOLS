
import pandas as pd
import numpy as np

def fetch_time(df, key):
    """
    Identity, for time itself only?
    t1 x1
    t2 x2
    t3 x3
    ->
    t1 x1
    t2 x3
    t3 x3
    """
    return df[key].to_numpy()

def fetch_diff(df, key):
    """
    For accumulative quantities
    t1 x1
    t2 x2
    t3 x3
    ->
    t1 x2-x1
    t2 x3-x2
    """
    return df[key].diff(1).shift(-1).dropna().to_numpy()

def fetch_roll(df, key):
    """
    For timestamp encoding, use middle value as approximation for that interval.
    t1 x1
    t2 x2
    t3 x3
    ->
    t1 (x1+x2)/2
    t2 (x2+x3)/2
    """
    return df[key].rolling(2).mean().shift(-1).dropna().to_numpy()

def fetch_skip(df, key):
    """
    For sparse encoding but "uniform":
    t1 x1
    t2 x1
    t2 x2
    t3 x2
    ->
    t1 x1
    t2 x2
    """
    return df[key][::2].to_numpy()

def fluctuation_smooth(seq: np.ndarray):
    """
    The model sometime give [high, 0] sequence due to so called numerical problem when water is too "enough".
    However, the de-fluctuation "true" value may be interesting for decision alogrithom, 
    thus this function try to give an approximation by smoothing like: 

    0.2, 0.3, 0.8, 0.0, 1.2, 0.0, ...
    ->
    0.2, 0.3, 0.4, 0.5, 0.6, 0.7, ...

    Note: This pattern may fail for right 
    """
    idx0 = np.where(seq == 0)[0]
    if len(idx0) == 0:
        return
    
    if idx0[0] == 0:
        idx0 = idx0[1:]
    idxp = idx0 - 1

    smoothed = (seq[idx0] + seq[idxp]) / 2

    seq[idx0] = seq[idxp] = smoothed
    
    return seq


def get_aligned_dict(data_map, df_node_map_map, df_map_map, out_map=None, *, wq_keys=None, aser_keys=None,
                     smooth_wqpsc_inp=True):
    """
    out_map is the output of run and optional.
    """
    if wq_keys is None:
        wq_keys = ["ROP"]

    if aser_keys is None:
        aser_keys = ["rain"]
    
    aligned_dict = {}

    # aligned_dict["time"] = fetch_time(out_map["qbal.out"], "jday")
    aligned_dict["time"] = fetch_time(data_map["aser.inp"], "time")

    if out_map is not None:
        aligned_dict["flow_qctlo"] = - fetch_diff(out_map["qbal.out"], "qctlo(million-m3)") * 1_000_000 # million-m3 -> m3
        aligned_dict["elev"] = fetch_roll(out_map["qbal.out"], "elev(m)")

        gb = out_map["WQWCTS.out"].groupby(["I", "J", "K"])
        wq_ij_map = dict(tuple(gb))
        for wq_key in wq_keys:
            for ij_tuple in wq_ij_map:
                key = f"{wq_key}_{ij_tuple}"
                aligned_dict[key] = fetch_roll(wq_ij_map[ij_tuple], wq_key)

    for wq_key in wq_keys:
        for flow_key, df in df_map_map["wqpsc.inp"].items():
            key = f"{wq_key}_{flow_key}"
            seq = fetch_skip(df, wq_key)
            if smooth_wqpsc_inp:
                fluctuation_smooth(seq)
            aligned_dict[key] = seq

    for key, df in df_map_map["qser.inp"].items():
        key = f"flow_{key}"
        aligned_dict[key] = fetch_skip(df, "flow") * 3600 # m3/s -> m3/h
        
    for aser_key in aser_keys:
        aligned_dict[aser_key] = fetch_roll(data_map["aser.inp"], aser_key)

    return aligned_dict

def get_aligned_df(data_map, df_node_map_map, df_map_map, out_map=None, **kwargs):
    """
    This function itself may modify data passed in, so is not a pure "view".
    """
    aligned_dict = get_aligned_dict(data_map, df_node_map_map, df_map_map, out_map, **kwargs)

    min_length = min([len(arr) for arr in aligned_dict.values()])
    aligned_dict = {key: value[:min_length] for key, value in aligned_dict.items()}
    aligned_df = pd.DataFrame(aligned_dict)

    return aligned_df

def stats_load(df, df_ori, wq_key, flow_key_list, qctlo_key="qctlo", pump_key="pump_outflow"):
    """
    df_ori should has un-modified qser.
    """
    
    dd = {}
    dd[f"load_{qctlo_key}"] = df[f"{wq_key}_{qctlo_key}"] * df[f"flow_{qctlo_key}"]
    for flow_key in flow_key_list:
        diff = df_ori[f"flow_{flow_key}"] - df[f"flow_{flow_key}"]
        dd[f"load_{flow_key}"] = df[f"{wq_key}_{flow_key}"] * diff
    dd[f"load_{pump_key}"] = df[f"flow_{pump_key}"] * df[f"{wq_key}_{pump_key}"]

    rdf = pd.DataFrame(dd)
    rdf["load_flow"] = rdf[[f"load_{flow_key}" for flow_key in flow_key_list]].sum(axis=1)
    rdf["load_total"] = rdf[f"load_{qctlo_key}"] + rdf["load_flow"] + rdf[f"load_{pump_key}"]
    return rdf
