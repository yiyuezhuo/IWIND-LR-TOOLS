
import pandas as pd

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

def get_aligned_dict(data_map, df_node_map_map, df_map_map, out_map=None, *, wq_keys=None, aser_keys=None):
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
            aligned_dict[key] = fetch_skip(df, wq_key)

    for key, df in df_map_map["qser.inp"].items():
        key = f"flow_{key}"
        aligned_dict[key] = fetch_skip(df, "flow") * 3600 # m3/s -> m3/h
        
    for aser_key in aser_keys:
        aligned_dict[aser_key] = fetch_roll(data_map["aser.inp"], aser_key)

    return aligned_dict

def get_aligned_df(data_map, df_node_map_map, df_map_map, out_map=None, *, wq_keys=None, aser_keys=None):
    aligned_dict = get_aligned_dict(data_map, df_node_map_map, df_map_map, out_map=out_map, wq_keys=wq_keys, aser_keys=aser_keys)

    min_length = min([len(arr) for arr in aligned_dict.values()])
    aligned_dict = {key: value[:min_length] for key, value in aligned_dict.items()}
    aligned_df = pd.DataFrame(aligned_dict)

    return aligned_df

def get_load(aligned_df, ):
    pass


class LoadAccountant:
    def __init__(self):
        pass