"""

"""

from typing import List
from pathlib import Path
from shutil import rmtree
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count

from .io.common import Node, dumps
from .utils import open_safe, run_simulation, mkdtemp_locked
from .create_simulation import create_simulation
from .collector import parse_out, dumpable_list

"""
write_map = {
    "efdc.inp": "efdc_inp",
    "qser.inp": "qser_inp",
    "wqpsc.inp": "wqpsc_inp",
    "wq3dwc.inp": "wq3dwc_inp",
    "conc_adjust.inp": "conc_adjust_inp"
}
"""


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


class RunnerInplace(Runner):
    """
    For one who don't want environment isolation (not recommended).
    """
    def __init__(self, src_root):
        self.src_root = Path(src_root)
        self.dst_root = self.src_root
        # create_simulation(src_root, dst_root)

def work(process_args: dict):
    root = process_args["root"]
    data_map = process_args["data_map"]
    dst_root = process_args["dst_root"]
    debug_list = process_args["debug_list"]

    runner = Runner(root, dst_root)
    out = runner.run_strict(data_map)

    if debug_list is None:
        runner.cleanup()
    else:
        debug_list.append(runner)
    
    return out

def run_batch(root, data_map_list, pool_size=None, debug_list=None, dst_root_list=None):
    if pool_size is None:
        pool_size = cpu_count() // 2 # assumes x2 hyper-threads
    if dst_root_list is None:
        dst_root_list = [None for _ in data_map_list]
    
    pool = Pool(pool_size)
    process_args_list = []
    for data_map, dst_root in zip(data_map_list, dst_root_list):
        process_args = {"root":root, "data_map": data_map, "debug_list": debug_list, "dst_root": dst_root} 
        process_args_list.append(process_args)
    return pool.map(work, process_args_list)

def data_map_fill(data_map:dict):
    data_map_filled = {dumpable: None for dumpable in dumpable_list}
    data_map_filled.update(data_map)
    return data_map_filled
