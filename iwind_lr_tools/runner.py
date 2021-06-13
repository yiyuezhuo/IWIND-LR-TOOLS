"""

"""

from typing import List
from pathlib import Path
from shutil import rmtree

from .io.common import Node, dumps
from .utils import open_safe, run_simulation

from .create_simulation import create_simulation
from .collector import parse_out

import tempfile

class Runner:
    """
    Create a new environment, replace some *inp with the one proposed by optimizer and fetch result.
    """
    def __init__(self, src_root, dst_root=None):
        self.src_root = Path(src_root)

        if dst_root is None:
            dst_root = tempfile.mkdtemp() # create_simulation will "replace" it instantly, but is it thread-safe?

        self.dst_root = Path(dst_root)
        create_simulation(src_root, dst_root)

    def write(self, *, efdc_node_list: List[Node]=None, qser_node_list: List[Node]=None):
        if efdc_node_list is not None:
            with open_safe(self.dst_root / "efdc.inp", "w", encoding='utf8') as f:
                f.write(dumps(efdc_node_list))
        if qser_node_list is not None:
            with open_safe(self.dst_root / "qser.inp", "w", encoding='utf8') as f:
                f.write(dumps(qser_node_list))
    
    def run_simulation(self, popen=False):
        run_simulation(self.dst_root, popen=popen)

    def parse_out(self):
        return parse_out(self.dst_root)

    def run(self, *, efdc_node_list: List[Node], qser_node_list: List[Node], popen=False):
        # If efdc_node_list or qser_node_list takes None, the value will not be changed.
        self.write(efdc_node_list=efdc_node_list, qser_node_list=qser_node_list)
        self.run_simulation(popen=popen)
        return self.parse_out()

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
