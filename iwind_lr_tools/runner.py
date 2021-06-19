"""

"""

from typing import List
from pathlib import Path
from shutil import rmtree
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
import pandas as pd
import os

from .io.common import Node, dumps
from .utils import open_safe, run_simulation, mkdtemp_locked
from .create_simulation import create_simulation
from .collector import parse_out, dumpable_list


def get_default_pool_size():
    return cpu_count() // 2 # assumes x2 hyper-threads


class Runner:
    """
    Create a new environment, replace some *inp with the one proposed by optimizer and fetch result.
    """
    def __init__(self, src_root, dst_root=None):
        self.src_root = Path(src_root)

        if dst_root is None:
            dst_root = mkdtemp_locked() # create_simulation will "replace" it instantly, but is it thread-safe?

        self.dst_root = Path(dst_root)
        create_simulation(src_root, dst_root)

    def write(self, data_map:dict):
        # {"efdc.inp": efdc_node_list: List[Node], ....}
        for fname in dumpable_list:
            node_list = data_map[fname]
            if node_list is not None:
                with open_safe(self.dst_root / fname, "w", encoding="utf8") as f:
                    f.write(dumps(node_list))
    
    def run_simulation(self):
        run_simulation(self.dst_root, popen=False)

    def parse_out(self):
        return parse_out(self.dst_root)

    def run_strict(self, data_map:dict):
        # If efdc_node_list or qser_node_list takes None, the value will not be changed.
        self.write(data_map)
        self.run_simulation()
        return self.parse_out()
    
    def run(self, data_map:dict):
        data_map_filled = data_map_fill(data_map)
        return self.run_strict(data_map_filled)

    def cleanup(self):
        # user may want to keep those files
        rmtree(self.dst_root)

    def __repr__(self):
        return f"Ruuner, src_root: {self.src_root}, dst_root: {self.dst_root}"


class RunnerInplace(Runner):
    """
    For one who don't want environment isolation (not recommended).
    """
    def __init__(self, src_root):
        self.src_root = Path(src_root)
        self.dst_root = self.src_root
        # create_simulation(src_root, dst_root)

def work(process_args: dict):
    root:str = process_args["root"]
    data_map:dict = process_args["data_map"]
    dst_root = process_args["dst_root"]
    debug_list = process_args["debug_list"]
    idx:int = process_args["idx"]

    runner = Runner(root, dst_root)
    if debug_list is not None:
        debug_list[idx] = runner
    out = runner.run_strict(data_map)

    if debug_list is None:
        runner.cleanup()        
    
    return out

def run_batch(root, data_map_list, pool_size=0, debug_list=None, dst_root_list=None):
    if pool_size == 0:
        pool_size = get_default_pool_size()
    if dst_root_list is None:
        dst_root_list = [None for _ in data_map_list]

    if debug_list is not None:
        assert len(debug_list) == 0, "debug_list is not None or empty list, maybe mistakenly use a previous list?"
        debug_list.extend([None for _ in data_map_list])
    
    pool = Pool(pool_size)
    process_args_list = []
    for idx, (data_map, dst_root) in enumerate(zip(data_map_list, dst_root_list)):
        process_args = {"root":root, "data_map": data_map, "debug_list": debug_list, "dst_root": dst_root, "idx": idx} 
        process_args_list.append(process_args)
    return pool.map(work, process_args_list)

def work_restart(process_args: dict):
    # TODO: An valuable altnative implementation is to just rename three files rather than symbolic link
    runner: Runner = process_args["runner"]
    data_map:dict = process_args["data_map"]
    dst_root = runner.dst_root

    RESTART_INP = dst_root / "RESTART.INP"
    RESTART_OUT = dst_root / "RESTART.OUT"
    TEMPB_RST = dst_root / "TEMPB.RST"
    TEMPBRST_OUT = dst_root / "TEMPBRST.OUT"
    wqini_inp = dst_root / "wqini.inp"
    WQWCRST_OUT = dst_root  / "WQWCRST.OUT"

    if wqini_inp.exists():
        wqini_inp.unlink()

    RESTART_OUT.rename(RESTART_INP)
    TEMPBRST_OUT.rename(TEMPB_RST)
    WQWCRST_OUT.rename(wqini_inp)

    """
    RESTART_INP = dst_root / "RESTART.INP"
    if not RESTART_INP.is_symlink():
        if RESTART_INP.exists(): # prevent occatially copying file from original root.
            RESTART_INP.unlink()
        RESTART_OUT = dst_root / "RESTART.OUT"
        assert RESTART_OUT.exists(), "RESTART.INP or RESTART.OUT must be given"
        RESTART_INP.symlink_to(RESTART_OUT)

    TEMPB_RST = dst_root / "TEMPB.RST"
    if not TEMPB_RST.is_symlink():
        if TEMPB_RST.exists():
            TEMPB_RST.unlink()
        TEMPBRST_OUT = dst_root / "TEMPBRST.OUT"
        assert TEMPBRST_OUT.exists(), "TEMPB.RST or TEMPBRST.OUT must be given"
        TEMPB_RST.symlink_to(TEMPBRST_OUT)

    wqini_inp = dst_root / "wqini.inp"
    assert wqini_inp.is_symlink()
    WQWCRST_OUT = dst_root / "WQWCRST.OUT"
    assert WQWCRST_OUT.exists(), "To restart, wqini_inp should exsited and be linked"
    if Path(os.readlink(wqini_inp)) != WQWCRST_OUT:
        wqini_inp.unlink()
        wqini_inp.symlink_to(WQWCRST_OUT)

    import pdb;pdb.set_trace()
    """

    out = runner.run_strict(data_map)

    return out

def restart_batch(runner_list:List[Runner], data_map_list, pool_size=None):
    # runner_list can be obtained by `debug_list` in `run_batch`
    if pool_size is None:
        pool_size = get_default_pool_size()

    pool = Pool(pool_size)
    process_args_list = []
    for runner, data_map in zip(runner_list, data_map_list):
        process_args = {"runner": runner, "data_map": data_map} 
        process_args_list.append(process_args)
    
    return pool.map(work_restart, process_args_list)

def data_map_fill(data_map:dict):
    data_map_filled = {dumpable: None for dumpable in dumpable_list}
    data_map_filled.update(data_map)
    return data_map_filled

