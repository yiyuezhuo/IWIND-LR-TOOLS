"""

"""

from iwind_lr_tools.load_stats import get_aligned_df
from typing import List
from pathlib import Path
from shutil import rmtree
# from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
import pandas as pd
import os
from contextlib import contextmanager
from warnings import warn

from .fault_tolerant_pool import YPool as Pool
from .io.common import Node, dumps
from .utils import copy_locked, open_safe, run_simulation, mkdtemp_locked
from .create_simulation import create_simulation
from .collector import parse_out, dumpable_list
from .actioner import Actioner


def get_default_pool_size():
    return cpu_count() // 2 # assumes x2 hyper-threads

shell_end_anchor = "TIMING INFORMATION IN SECONDS"
shell_end_anchor_offset = len(shell_end_anchor)

def parse_shell_output(full_output):
    gi = full_output.index(shell_end_anchor)
    word_list = full_output[gi + shell_end_anchor_offset:].split()
    rd = {}
    stack = []
    it = iter(word_list)
    for word in it:
        if word == "=":
            key = " ".join(stack)
            stack = []
            value = next(it)
            rd[key] = float(value)
        else:
            stack.append(word)
    return rd

class Runner:
    """
    Create a new environment, replace some *inp with the one proposed by optimizer and fetch result.
    """

    def __init__(self, src_root, dst_root=None, without_create_simulation=False, verbose=True):
        self.shell_output_list = []
        self.shell_output_parsed_list = []

        if without_create_simulation:
            self.src_root = None
            self.dst_root = src_root
        else:
            self.src_root = Path(src_root)

            if dst_root is None:
                dst_root = mkdtemp_locked() # create_simulation will "replace" it instantly, but is it thread-safe?

            self.dst_root = Path(dst_root)
            create_simulation(src_root, dst_root)
        
        self.verbose = verbose

    def write(self, data_map:dict):
        # {"efdc.inp": efdc_node_list: List[Node], ....}
        for fname in dumpable_list:
            node_list = data_map[fname]
            if node_list is not None:
                with open_safe(self.dst_root / fname, "w", encoding="utf8") as f:
                    f.write(dumps(node_list))
    
    def run_simulation(self):
        return run_simulation(self.dst_root, popen=False)

    def parse_out(self):
        return parse_out(self.dst_root)

    def run_strict(self, data_map:dict):
        # If efdc_node_list or qser_node_list takes None, the value will not be changed.
        self.write(data_map)
        shell_output = self.run_simulation().decode()
        self.shell_output_list.append(shell_output)
        self.check_shell_output(shell_output)
        return self.parse_out()
    
    def run(self, data_map:dict):
        data_map_filled = data_map_fill(data_map)
        return self.run_strict(data_map_filled)

    def cleanup(self):
        # user may want to keep those files
        rmtree(self.dst_root)
        if self.verbose:
            print(f"cleanup {self.dst_root}")

    def check_shell_output(self, shell_output:str):
        # TODO: do some check to raise error as early as possible
        # The parsing will fail if model itself failed to complete.
        self.shell_output_parsed_list.append(parse_shell_output(shell_output))

    def __repr__(self):
        return f"Ruuner(src_root={self.src_root}, dst_root={self.dst_root})"


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


def run_batch(root, data_map_list, pool_size=0, debug_list=None, dst_root_list=None, sequential=False):
    """
    sequential argument is used to control parallel related factor to help debugging
    """
    data_map_list = [x if isinstance(x, dict) else x.data_map for x in data_map_list]

    if pool_size == 0:
        pool_size = get_default_pool_size()
    if dst_root_list is None:
        dst_root_list = [None for _ in data_map_list]

    if debug_list is not None:
        assert len(debug_list) == 0, "debug_list is not None or empty list, maybe mistakenly use a previous list?"
        debug_list.extend([None for _ in data_map_list])

    process_args_list = []
    for idx, (data_map, dst_root) in enumerate(zip(data_map_list, dst_root_list)):
        process_args = {"root":root, "data_map": data_map, "debug_list": debug_list, "dst_root": dst_root, "idx": idx} 
        process_args_list.append(process_args)
    
    if not sequential:
        pool = Pool(pool_size)
        return pool.map(work, process_args_list)
    else:
        return [work(process_arg) for process_arg in process_args_list]


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

    if RESTART_INP.exists():
        RESTART_INP.unlink()
    if TEMPB_RST.exists():
        TEMPB_RST.unlink()
    if wqini_inp.exists():
        wqini_inp.unlink()
    
    copy_locked(RESTART_OUT, RESTART_INP)
    copy_locked(TEMPBRST_OUT, TEMPB_RST)
    copy_locked(WQWCRST_OUT, wqini_inp)

    out = runner.run_strict(data_map)

    return out


def copy_restart_files(src, dst):
    src = Path(src)
    dst = Path(dst)
    copy_name_list = ["RESTART.OUT", "TEMPBRST.OUT", "WQWCRST.OUT"]
    for copy_name in copy_name_list:
        copy_locked(src / copy_name, dst / copy_name)
        assert (dst / copy_name).exists(), "Strange bug?"
        print("copied", src / copy_name, "=>", dst / copy_name)

def fork(runner_base: Runner, size:int) -> List[Runner]:
    """
    Fork a executed runner into many runners.
    """
    #copy_name_list = ["RESTART.OUT", "TEMPBRST.OUT", "WQWCRST.OUT"]
    runner_list = []
    for _ in range(size):
        runner = Runner(runner_base.dst_root)
        copy_restart_files(runner_base.dst_root, runner.dst_root)
        runner_list.append(runner)
        print(f"fork: {runner_base.dst_root} -> {runner.dst_root}")
    return runner_list

def restart_batch(runner_list:List[Runner], data_map_list, pool_size=None):
    # runner_list can be obtained by `debug_list` in `run_batch`
    data_map_list = [x if isinstance(x, dict) else x.data_map for x in data_map_list]

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

def restart_iterator(begin_day, end_day, runner_completed: Runner, actioner_frozen:Runner, 
                    step=7, yield_out_map=False, dropna=True, dt=None,
                    debug_list=None):
    """
    This function will not modify *qser* and other detailed information, 
    as they're expected to be encoded in actioner_frozen already.
    So this function will not yield actioner since the caller can still use action_frozen as usual.
    """
    actioner = actioner_frozen.copy()
    processing_begin_day = begin_day

    runner_list = fork(runner_completed, 1)
    actioner_list = [actioner]

    if debug_list is not None:
        debug_list.extend(runner_list)

    actioner.enable_restart()

    while processing_begin_day < end_day:
        simulation_length = min(end_day - processing_begin_day, step)

        actioner.set_simulation_begin_time(processing_begin_day)
        actioner.set_simulation_length(simulation_length)
        
        out_map, = restart_batch(runner_list, actioner_list, pool_size=1)

        if yield_out_map:
            yield out_map
        else:
            df = get_aligned_df(*actioner.to_data(), out_map, dropna=dropna, dt=dt)
            yield df

        processing_begin_day = processing_begin_day + simulation_length

    if debug_list is None:
        for runner in runner_list:
            runner.cleanup()

def restart_iterator_1day_plus(begin_day, end_day, runner_completed: Runner, actioner_frozen:Runner, 
                            step=7, yield_out_map=False, dropna=True, dt=None,
                            debug_list=None):
    actioner = actioner_frozen.copy()
    actioner_1day_plus = actioner_frozen.copy()

    actioner.enable_restart()
    actioner_1day_plus.enable_restart()

    processing_begin_day = begin_day

    runner_list = fork(runner_completed, 2)
    actioner_list = [actioner, actioner_1day_plus]

    if debug_list is not None:
        debug_list.extend(runner_list)

    while processing_begin_day < end_day:
        simulation_length = min(end_day - processing_begin_day, step)

        actioner.set_simulation_begin_time(processing_begin_day)
        actioner.set_simulation_length(simulation_length)

        actioner_1day_plus.set_simulation_begin_time(processing_begin_day)
        actioner_1day_plus.set_simulation_length(simulation_length + 1)

        copy_restart_files(runner_list[0].dst_root, runner_list[1].dst_root)
        
        out_map, out_map_1day_plus = restart_batch(runner_list, actioner_list, pool_size=2)

        if yield_out_map:
            yield out_map_1day_plus
        else:
            df = get_aligned_df(*actioner.to_data(), out_map, dropna=dropna, dt=dt)
            df_1day_plus = get_aligned_df(*actioner_1day_plus.to_data(), out_map_1day_plus, dropna=dropna, dt=dt)
            # yield df.append(df_1day_plus.iloc[-24:-23])
            # assert df.equals(df_1day_plus[:-24])
            if not df.equals(df_1day_plus[:-24]):
                warn(f"Internal consistency check failed: df - df_1day_plus[:-24] = {df - df_1day_plus[:-24]}")
            
            yield df_1day_plus[:-23]

        processing_begin_day = processing_begin_day + simulation_length

    if debug_list is None:
        for runner in runner_list:
            runner.cleanup()

def start_iterator(begin_day:int, end_day:int, root, actioner_frozen: Actioner,
                    step=7, yield_out_map=False, dropna=True, dt=None,
                    debug_list=None, debug_restart_list=None):
                    
    actioner = actioner_frozen.copy()
    actioner.set_simulation_begin_time(begin_day)
    actioner.set_simulation_length(step)
    # assert begin_day + step < end_day

    if debug_list is None:
        debug_list = []
    out_map, = run_batch(root, [actioner], debug_list=debug_list)
    runner_completed = debug_list[0]

    if yield_out_map:
        yield out_map
    else:
        yield get_aligned_df(*actioner.to_data(), out_map, dt=dt)

    yield from restart_iterator(begin_day + step, end_day, runner_completed, actioner_frozen,
                step=step, yield_out_map=yield_out_map, dropna=dropna, dt=dt,
                debug_list=debug_restart_list)

def start_iterator_1day_plus(begin_day, end_day, root, actioner_frozen:Actioner,
                    step=7, yield_out_map=False, dropna=True, dt=None,
                    debug_list=None, debug_restart_list=None):

    actioner = actioner_frozen.copy()
    actioner.set_simulation_length(begin_day)
    actioner.set_simulation_length(step)

    actioner_1day_plus = actioner_frozen.copy()
    actioner_1day_plus.set_simulation_length(begin_day)
    actioner_1day_plus.set_simulation_length(step+1)

    actioner_list = [actioner, actioner_1day_plus]

    if debug_list is None:
        debug_list = []
    out_map, out_map_1day_plus = run_batch(root, actioner_list, debug_list=debug_list)
    runner_completed = debug_list[0]

    if yield_out_map:
        yield out_map, out_map_1day_plus
    else:
        df = get_aligned_df(*actioner.to_data(), out_map, dt=dt)
        df_1day_plus = get_aligned_df(*actioner_1day_plus.to_data(), out_map_1day_plus, dt=dt)
        # assert df.equals(df_1day_plus[:-24])
        if not df.equals(df_1day_plus[:-24]):
            warn(f"Internal consistency check failed: df - df_1day_plus[:-24] = {df - df_1day_plus[:-24]}")
        
        yield df_1day_plus[:-23]
    
    yield from restart_iterator_1day_plus(begin_day + step, end_day, runner_completed, actioner_frozen,
                step=step, yield_out_map=yield_out_map, dropna=dropna, dt=dt,
                debug_list=debug_restart_list)

@contextmanager
def debug_env(debug_list=None, protect=None):
    if debug_list is None:
        _debug_list = []
    else:
        _debug_list = debug_list

    yield _debug_list

    if debug_list is None:
        protect_set = set(protect) if protect is not None else set()
        for idx, runner in enumerate(_debug_list):
            if idx not in protect_set:
                runner.cleanup()
