"""

"""

from typing import List
from pathlib import Path
from shutil import rmtree
# from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
import pandas as pd
import os
from contextlib import contextmanager
from warnings import warn
import logging
import numpy as np

from .fault_tolerant_pool import YPool as Pool
from .io.common import Node, dumps
from .utils import copy_locked, open_safe, run_simulation, mkdtemp_locked
from .create_simulation import create_simulation
from .collector import parse_out, dumpable_list
from .actioner import Actioner
from .load_stats import Pedant


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

    def __init__(self, src_root, dst_root=None, without_create_simulation=False):
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
        logging.debug(f"cleanup {self.dst_root}")

    def check_shell_output(self, shell_output:str):
        # TODO: do some check to raise error as early as possible
        # The parsing will fail if model itself failed to complete.
        self.shell_output_parsed_list.append(parse_shell_output(shell_output))

    def __repr__(self):
        return f"Ruuner(src_root={self.src_root}, dst_root={self.dst_root})"

    def debug(self):
        if len(self.shell_output_list) == 0:
            print("No shell output is catched")
        else:
            if len(self.shell_output_list) > 1:
                print("The size of collected shell output size > 1, show last shell output")
            print(self.shell_output_list[-1])


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

    out = runner.run_strict(data_map)

    return out


def copy_restart_files(src, dst=None):
    src = Path(src)
    dst = Path(dst) if dst is not None else src

    out_to_inp = {
        "RESTART.OUT": "RESTART.INP",
        "TEMPBRST.OUT": "TEMPB.RST",
        "WQWCRST.OUT": "wqini.inp"
    }

    for out_s, inp_s in out_to_inp.items():
        src_p = src / out_s
        dst_p = dst / inp_s
        if dst_p.exists():
            dst_p.unlink()
        copy_locked(src_p, dst_p)
        assert dst_p.exists(), "Strange bug?"

        logging.debug(f"copied {src_p} => {dst_p}")

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

        logging.debug(f"fork: {runner_base.dst_root} -> {runner.dst_root}")
    return runner_list

def check_is_restarting(data_map_or_actioner):
    if isinstance(data_map_or_actioner, Actioner):
        assert data_map_or_actioner.is_restarting()
    elif isinstance(data_map_or_actioner, dict):
        warn("Input is data_map instead of Actioner, is_restarting is not checked")

def restart_batch(runner_list:List[Runner], data_map_list, pool_size=None):
    # runner_list can be obtained by `debug_list` in `run_batch`
    assert len(runner_list) == len(data_map_list)

    for x in data_map_list:
        check_is_restarting(x)
    
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

def restart_list_iterator(begin_day, end_day, runner_completed:Runner, actioner_frozen_list: List[Actioner],
                        step=7, pedant: Pedant=None,
                        debug_list=None, return_out_map=False):
    # This function will not modify *qser* and other detailed information, 
    # as they're expected to be encoded in actioner_frozen already.
    # So this function will not yield actioner since the caller can still use action_frozen as usual.

    actioner_list = [actioner.copy() for actioner in actioner_frozen_list]
    processing_begin_day = begin_day

    runner_list = fork(runner_completed, len(actioner_frozen_list))

    if debug_list is not None:
        debug_list.extend(runner_list)

    for actioner in actioner_list:
        actioner.enable_restart()

    # TODO
    while processing_begin_day < end_day:
        simulation_length = min(end_day - processing_begin_day, step)

        for actioner in actioner_list:
            actioner.set_simulation_begin_time(processing_begin_day)
            actioner.set_simulation_length(simulation_length)
        
        out_map_list = restart_batch(runner_list, actioner_list, pool_size=len(actioner_list))

        for runner in runner_list:
            copy_restart_files(runner.dst_root)

        if return_out_map:
            yield out_map_list
        else:
            df_list = [pedant.get_df(actioner, out_map) for out_map in out_map_list]
            yield df_list

        processing_begin_day = processing_begin_day + simulation_length

    if debug_list is None:
        for runner in runner_list:
            runner.cleanup()

def restart_iterator(begin_day, end_day, runner_completed: Runner, actioner_frozen:Runner, 
                    step=7, pedant: Pedant=None,
                    debug_list=None, return_out_map=False):
    # This function is for backward compatibility. Favor restart_list_iterator in general.
    actioner_frozen_list = [actioner_frozen]
    for df_or_out_map_list in restart_list_iterator(begin_day, end_day, runner_completed, actioner_frozen_list,
                    step=step, pedant=pedant, debug_list=debug_list, return_out_map=return_out_map):
        yield df_or_out_map_list[0]
    
"""
def restart_iterator(begin_day, end_day, runner_completed: Runner, actioner_frozen:Runner, 
                    step=7, pedant: Pedant=None,
                    debug_list=None, return_out_map=False):
    # This function will not modify *qser* and other detailed information, 
    # as they're expected to be encoded in actioner_frozen already.
    # So this function will not yield actioner since the caller can still use action_frozen as usual.
    
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

        copy_restart_files(runner_list[0].dst_root)

        if return_out_map:
            yield out_map
        else:
            df = pedant.get_df(actioner, out_map)
            yield df

        processing_begin_day = processing_begin_day + simulation_length

    if debug_list is None:
        for runner in runner_list:
            runner.cleanup()
"""

def start_iterator(begin_day:int, end_day:int, root, actioner_frozen: Actioner,
                    step=7, pedant:Pedant=None, return_out_map=False,
                    debug_list=None, debug_restart_list=None):
                    
    actioner = actioner_frozen.copy()
    actioner.set_simulation_begin_time(begin_day)
    actioner.set_simulation_length(step)
    # assert begin_day + step < end_day

    if debug_list is None:
        debug_list = []
    out_map, = run_batch(root, [actioner], debug_list=debug_list)
    runner_completed = debug_list[0]

    if return_out_map:
        yield out_map
    else:
        yield pedant.get_df(actioner, out_map)

    yield from restart_iterator(begin_day + step, end_day, runner_completed, actioner_frozen,
                step=step, pedant=pedant,
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

def start_single(begin_day, end_day, root, actioner_frozen:Actioner, **kwargs):
    step = end_day - begin_day
    it = start_iterator(begin_day, end_day, root, actioner_frozen, step=step, **kwargs)
    return next(it)

def restart_single(begin_day, end_day, runner_completed, actioner_frozen:Actioner, **kwargs):
    step = end_day - begin_day
    it = restart_iterator(begin_day, end_day, runner_completed, actioner_frozen, step=step, **kwargs)
    return next(it)

def restart_list_single(begin_day, end_day, runner_completed, actioner_frozen:Actioner, **kwargs):
    step = end_day - begin_day
    it = restart_list_iterator(begin_day, end_day, runner_completed, actioner_frozen, step=step, **kwargs)
    return next(it)

class SimilarRestarter:
    def __init__(self, actioner_limit: Actioner, runner_completed, decided_length, end_hour, pedant, use_cache=True):
        self.actioner_limit = actioner_limit
        self.runner_completed = runner_completed
        self.decided_length = decided_length
        self.end_hour = end_hour
        assert end_hour % 24 ==0

        self.pedant = pedant

        total_begin_time = self.actioner_limit.get_simulation_begin_time()

        # whole begin_day, end_day
        self.begin_day = total_begin_time + self.decided_length // 24
        self.end_day = total_begin_time + end_hour // 24

        self.use_cache = use_cache

        self.decided_ddf_cached = None
        self.same_end_day_cached = None
        self.runner_guider_cached = None

    def restart(self, decided_ddf_list):
        assert len(decided_ddf_list) > 0

        total_begin_time = self.actioner_limit.get_simulation_begin_time()

        use_guider = True

        if len(decided_ddf_list) == 1:
            use_guider = False
        else:
            cut_ddf_arr = np.array([dddf[self.decided_length: self.end_hour].to_numpy() for dddf in decided_ddf_list]) # (num_simulation, time, inflow_and_pump)
            same_arr = np.cumprod(np.all(cut_ddf_arr[0:1, :, :] == cut_ddf_arr[1:, :, :], axis=(0, 2)))
            if same_arr[0] == 0:
                use_guider = False
            else:
                same_idx = np.where(same_arr == 1)[0][0]
                same_end_day = total_begin_time + (self.decided_length + same_idx) // 24
                if same_end_day == self.begin_day:
                    use_guider = False

        if not use_guider:
            actioner_list = []
            for decided_ddf in decided_ddf_list:
                actioner = self.actioner_limit.copy()
                actioner.set_flow_by_decision_df(decided_ddf)
                actioner_list.append(actioner)
            return restart_list_single(self.begin_day, self.end_day, self.runner_completed, actioner_list, pedant=self.pedant)
    
        if self.use_cache and self.same_idx_cached is not None and self.same_end_day_cached <= same_end_day:
            same_end_hour = (self.same_end_day_cached - total_begin_time)*24
            ref_arr = self.decided_ddf_cached[self.decided_length: same_end_hour].to_numpy()[np.newaxis, ...]
            
            cut_ddf_arr[:same_end_hour]

        actioner_guider = self.actioner_limit.copy()
        actioner_guider.set_flow_by_decision_df(decided_ddf_list[0])
        
        debug_list = []
        df_guider = restart_single(begin_day, end_day, self.runner_completed, actioner_guider,
            debug_list=debug_list, pedant=self.pedant,
        )
        runner_guider = debug_list[0]

        # TODO: WIP









