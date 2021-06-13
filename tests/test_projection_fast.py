"""
Fast version will copy original root and copy a human-wrote "efdc.inp" (simulate only 3 days) file
to override the original one. Then symbolic the overided one.
Fast version is not guaranteed to be correct (though it will catch most possible errors) and 
is able to run (a new human- file may be required).
"""

import unittest
import os
import shutil
import tempfile
import contextlib
from pathlib import Path

from iwind_lr_tools import create_simulation, run_simulation, efdc_inp, qser_inp, dumps
from iwind_lr_tools.utils import open_safe
from iwind_lr_tools import Actioner, Runner
from iwind_lr_tools.collector import get_all

ori_root = os.environ["WATER_ROOT"]
efdc_fast = os.environ["WATER_EFDC_FAST"]
non_fast_mode = "WATER_STRICT" in os.environ

@contextlib.contextmanager
def create_environment(ori_root, verbose=True):
    with tempfile.TemporaryDirectory() as temp_path:
        root = Path(temp_path) / "root"
        target_root = Path(temp_path) / "target_root"
        # print(f"shutil.copy({ori_root}, {root})")
        # import pdb; pdb.set_trace()
        shutil.copytree(ori_root, root)
        if not non_fast_mode:
            shutil.copy(efdc_fast, root / "efdc.inp")
            if verbose:
                print(f"Override {efdc_fast} -> {root / 'efdc.inp'}")
        if verbose:
            print(f"Copy {ori_root} to {root}")
        create_simulation(root, target_root)
        if verbose:
            print(f"Copy {ori_root} to {root}")
        yield root, target_root


class TestProjection(unittest.TestCase):

    def file_eq(self, p1, p2, eq=True):
        with open(p1) as f:
            d1 = f.read()
        with open(p2) as f:
            d2 = f.read()
        if eq:
            self.assertEqual(d1, d2)
        else:
            self.assertNotEqual(d1, d2)

    def get_result(self, root, target_root):
        root_list = [root, target_root]
        process_list = [run_simulation(r, popen=True) for r in root_list]
        for p in process_list:
            p.wait()

    def test_create_simulation(self):
        with create_environment(ori_root) as (root, target_root):
            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out")

    def test_efdc_inp_projection(self):
        with create_environment(ori_root) as (root, target_root):
            efdc_path = target_root / "efdc.inp"
            efdc_node_list = efdc_inp.parse(efdc_path)
            with open_safe(efdc_path, "w", encoding="utf8") as f:
                f.write(dumps(efdc_node_list))

            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out")

    
    def test_qser_inp_projection(self):
        with create_environment(ori_root) as (root, target_root):
            qser_path = target_root / "qser.inp"
            qser_node_list = qser_inp.parse(qser_path)
            with open_safe(qser_path, "w", encoding="utf8") as f:
                f.write(dumps(qser_node_list))

            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out")


    def test_more_1_day_sanity_check(self):
        # test if test can catch difference if target simulation runs for 1 day more.
        with create_environment(ori_root) as (root, target_root):
            efdc_path = target_root / "efdc.inp"
            efdc_node_list = efdc_inp.parse(efdc_path)
            df_node_map = efdc_inp.get_df_node_map(efdc_node_list)
            df = df_node_map["C03"].get_df()
            df["NTC"].iloc[0] += 1
            print(df)
            with open_safe(efdc_path, "w", encoding="utf8") as f:
                f.write(dumps(efdc_node_list))

            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out", eq=False)


class TestEnvrionmentIsolation(unittest.TestCase):
    def test_environment_isolation(self):
        given_simulation_length = 1.1

        root = Path(ori_root)
        data_map, df_map_map = get_all(root)
        actioner = Actioner(data_map, df_map_map)
        actioner.set_simulation_length(given_simulation_length)

        self.assertEqual(actioner.get_simulation_length(), given_simulation_length)
        self.assertEqual(df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0], given_simulation_length)

        date_map = {p: p.stat().st_mtime for p in root.iterdir()}
        runner = Runner(root) # a temp path will be created
        runner.run(efdc_node_list=data_map["efdc.inp"], qser_node_list=None)
        for p in root.iterdir():
            self.assertEqual(p.stat().st_mtime, date_map[p])
        
        efdc_new_time = (runner.dst_root / "efdc.inp").stat().st_mtime
        efdc_old_time = (root / "efdc.inp").stat().st_mtime

        qser_new_time = (runner.dst_root / "qser.inp").stat().st_mtime
        qser_old_time = (root / "qser.inp").stat().st_mtime
        
        self.assertGreater(efdc_new_time, efdc_old_time)
        self.assertEqual(qser_new_time, qser_old_time)

        runner.write(efdc_node_list=data_map["efdc.inp"], qser_node_list=data_map["qser.inp"])

        qser_new_time = (runner.dst_root / "qser.inp").stat().st_mtime

        self.assertGreater(qser_new_time, qser_old_time)



        
