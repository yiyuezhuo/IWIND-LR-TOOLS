"""

"""

from typing import List
from pathlib import Path
from shutil import rmtree
# from multiprocessing.dummy import Pool
from .fault_tolerant_pool import YPool as Pool
from multiprocessing import cpu_count
import pandas as pd
import os

from .io.common import Node, dumps
from .utils import copy_locked, open_safe, run_simulation, mkdtemp_locked
from .create_simulation import create_simulation
from .collector import parse_out, dumpable_list


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

    def check_shell_output(self, shell_output:str):
        # TODO: do some check to raise error as early as possible
        # The parsing will fail if model itself failed to complete.
        self.shell_output_parsed_list.append(parse_shell_output(shell_output))

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

def run_batch(root, data_map_list, pool_size=0, debug_list=None, dst_root_list=None, sequential=False):
    """
    sequential argument is used to control parallel related factor to help debugging
    """
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

    if wqini_inp.exists():
        wqini_inp.unlink()

    RESTART_OUT.rename(RESTART_INP)
    TEMPBRST_OUT.rename(TEMPB_RST)
    WQWCRST_OUT.rename(wqini_inp)

    out = runner.run_strict(data_map)

    return out

def fork(runner_base: Runner, size:int) -> List[Runner]:
    """
    Fork a executed runner into many runners.
    """
    copy_name_list = ["RESTART.OUT", "TEMPBRST.OUT", "WQWCRST.OUT"]
    runner_list = []
    for _ in range(size):
        runner = Runner(runner_base.dst_root)
        for copy_name in copy_name_list:
            copy_locked(runner_base.dst_root / copy_name, runner.dst_root / copy_name)
            assert (runner.dst_root / copy_name).exists(), "Strange bug?"
        runner_list.append(runner)
    return runner_list

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

