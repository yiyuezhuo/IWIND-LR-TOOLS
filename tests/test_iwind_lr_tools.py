"""
Fast version will copy original root and copy a human-wrote "efdc.inp" (simulate only 3 days) file
to override the original one. Then symbolic the overided one.
Fast version is not guaranteed to be correct (though it will catch most possible errors) and 
is able to run (a new human- file may be required).
"""

from iwind_lr_tools.io.common import DataFrameNode
import unittest
import os
import shutil
import tempfile
import contextlib
from pathlib import Path
from multiprocessing.dummy import Pool
from copy import deepcopy

from iwind_lr_tools import create_simulation, run_simulation, efdc_inp, qser_inp, wqpsc_inp, dumps
from iwind_lr_tools.utils import open_safe
from iwind_lr_tools import Actioner, Runner
from iwind_lr_tools.collector import get_all, inp_out_map

MIN_SIMULATION_TIME = 1.0

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

@contextlib.contextmanager
def create_environment_fast_only(ori_root, verbose=True):
    with tempfile.TemporaryDirectory() as temp_path:
        root = Path(temp_path) / "root"
        shutil.copytree(ori_root, root)
        if not non_fast_mode:
            shutil.copy(efdc_fast, root / "efdc.inp")
            if verbose:
                print(f"Override {efdc_fast} -> {root / 'efdc.inp'}")
        yield root


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

    def test_fast_too_small(self):
        root = Path(ori_root)
        actioner = Actioner(*get_all(root))
        actioner.set_simulation_length(MIN_SIMULATION_TIME)
        runner = Runner(root)
        out = runner.run(efdc=actioner.data_map["efdc.inp"], qser=None, wqpsc=None)
        
        self.assertGreater(out["qbal.out"].shape[0], 1)

        if not non_fast_mode:
            fast_length = DataFrameNode.get_df_map(efdc_inp.parse(efdc_fast))["C03"]["NTC"].iloc[0]
            self.assertGreaterEqual(fast_length, MIN_SIMULATION_TIME)


    def test_create_simulation(self):
        with create_environment(ori_root) as (root, target_root):
            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out")

    def _test_projection(self, inp_key):
        with create_environment(ori_root) as (root, target_root):
            inp_path = target_root / inp_key
            node_list = inp_out_map[inp_key].parse(inp_path)
            with open_safe(inp_path, "w", encoding="utf8") as f:
                f.write(dumps(node_list))

            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out")


    def test_projection_direct(self):
        check_list = ["efdc.inp", "qser.inp", "wqpsc.inp"]
        for name in check_list:
            self._test_projection(name)

    def test_projection_dep(self):
        with create_environment_fast_only(Path(ori_root)) as root:
            data = get_all(root)
            # actioner = Actioner(*data)
            # actioner.set_simulation_length(MIN_SIMULATION_TIME)
            data_map = data[0]

            check_list = {
                "efdc.inp": "efdc", 
                "qser.inp": "qser", 
                "wqpsc.inp": "wqpsc", 
                "wq3dwc.inp": "wq3dwc", 
                "conc_adjust.inp": "conc_adjust"
            }
            run_kwargs_list = [{}]
            for key, arg_key in check_list.items():
                run_kwargs_list.append({arg_key: data_map[key]})
            
            def work(run_kwargs):
                runner = Runner(root)
                print(f"{run_kwargs} -> {runner.dst_root}")
                return runner.run(**run_kwargs)
            pool = Pool(6)
            out_list = pool.map(work, run_kwargs_list)

            for key in out_list[0].keys():
                self.assertGreater(out_list[0][key].shape[0], 1)
                for i in range(1, len(check_list)):
                    self.assertTrue(out_list[0][key].equals(out_list[i][key]))


    def test_more_time_sanity_check(self):
        # test if test can catch difference if target simulation runs for 1 day more.
        with create_environment(ori_root) as (root, target_root):
            efdc_path = target_root / "efdc.inp"
            efdc_node_list = efdc_inp.parse(efdc_path)
            # df_node_map = efdc_inp.get_df_node_map(efdc_node_list)
            df_node_map = DataFrameNode.get_df_node_map(efdc_node_list)
            df = df_node_map["C03"].get_df()
            # df["NTC"].iloc[0] += 0.5
            df.loc[df.index[0], "NTC"] += MIN_SIMULATION_TIME
            print(df)
            with open_safe(efdc_path, "w", encoding="utf8") as f:
                f.write(dumps(efdc_node_list))

            self.get_result(root, target_root)
            self.file_eq(root / "WQWCTS.out", target_root / "WQWCTS.out", eq=False)

    def test_runner_wqpsc_node_list(self):
        root = Path(ori_root)
        actioner = Actioner(*get_all(root))
        actioner.set_simulation_length(MIN_SIMULATION_TIME)
        run_kwargs_list = [
            dict(efdc=actioner.data_map["efdc.inp"], qser=None, wqpsc=None),
            dict(efdc=actioner.data_map["efdc.inp"], qser=None, wqpsc=actioner.data_map["wqpsc.inp"])
        ]
        pool = Pool(2)
        def work(run_kwargs):
            runner = Runner(ori_root)
            out = runner.run(**run_kwargs)
            runner.cleanup()
            return out
        out_guard, out_new = pool.map(work, run_kwargs_list)
        
        self.assertTrue(out_guard["qbal.out"].equals(out_new["qbal.out"]))


class TestEnvrionmentIsolation(unittest.TestCase):
    def test_environment_isolation(self):
        given_simulation_length = MIN_SIMULATION_TIME

        root = Path(ori_root)
        data = data_map, df_node_map_map, df_map_map = get_all(root)
        actioner = Actioner(*data)
        actioner.set_simulation_length(given_simulation_length)

        self.assertEqual(actioner.get_simulation_length(), given_simulation_length)
        self.assertEqual(df_map_map["efdc.inp"]["C03"]["NTC"].iloc[0], given_simulation_length)

        date_map = {p: p.stat().st_mtime for p in root.iterdir()}
        runner = Runner(root) # a temp path will be created
        runner.run(efdc=data_map["efdc.inp"], qser=None, wqpsc=None)
        for p in root.iterdir():
            self.assertEqual(p.stat().st_mtime, date_map[p])
        
        efdc_new_time = (runner.dst_root / "efdc.inp").stat().st_mtime
        efdc_old_time = (root / "efdc.inp").stat().st_mtime

        qser_new_time = (runner.dst_root / "qser.inp").stat().st_mtime
        qser_old_time = (root / "qser.inp").stat().st_mtime
        
        self.assertGreater(efdc_new_time, efdc_old_time)
        self.assertEqual(qser_new_time, qser_old_time)

        runner.write(efdc=data_map["efdc.inp"], qser=data_map["qser.inp"],
                     wqpsc=None)

        qser_new_time = (runner.dst_root / "qser.inp").stat().st_mtime

        self.assertGreater(qser_new_time, qser_old_time)


class TestDropEquivalence(unittest.TestCase):
    def test_drop_equivalence(self):
        # return # soft/hard drop doesn't work correctly at this time.
        root = Path(ori_root)
        drop_idx = 0

        data_ori = data_map_ori, df_node_map_map_ori, df_map_map_ori = get_all(root)
        actioner_ori = Actioner(*data_ori)
        actioner_ori.set_simulation_length(MIN_SIMULATION_TIME)

        actioner_iden = deepcopy(actioner_ori)

        actioner_drop_hard = deepcopy(actioner_ori)
        actioner_drop_hard.drop_flow_hard([drop_idx])

        """
        actioner_drop_soft = deepcopy(actioner_ori)
        actioner_drop_soft.drop_flow_soft([4])
        actioner_drop_soft.list_flow_name()
        """

        actioner_q0 = deepcopy(actioner_ori)
        actioner_q0.df_map_map["efdc.inp"]["C08"].loc[actioner_q0.df_map_map["efdc.inp"]["C08"].index[drop_idx], "Qfactor"] = 0

        actioner_v0 = deepcopy(actioner_ori)
        actioner_v0.df_map_map["qser.inp"]
        df = actioner_v0.get_flow_node_list()[drop_idx].get_df()
        df["flow"] = 0

        # actioner_list = [actioner_iden, actioner_drop_hard, actioner_drop_soft, actioner_q0, actioner_v0]
        actioner_list = [actioner_iden, actioner_drop_hard, actioner_q0, actioner_v0]
        # label_list = ["iden", "drop_hard", "drop_soft", "q0", "v0"]
        label_list = ["iden", "drop_hard", "q0", "v0"]
        label2actioner = {label:actioner for label, actioner in zip(label_list, actioner_list)}

        def work(label):
            runner = Runner(root)#, test_root_root / label)
            actioner = label2actioner[label]
            data_map = actioner.data_map
            dst_out_map = runner.run_strict(
                efdc=data_map["efdc.inp"], qser=data_map["qser.inp"],
                wqpsc=data_map["wqpsc.inp"], wq3dwc=data_map["wq3dwc.inp"],
                conc_adjust=data_map["conc_adjust.inp"])
            return dst_out_map

        pool = Pool(len(label_list))

        dst_out_map_list = pool.map(work, label_list)

        w_list = [dst_out_map["WQWCTS.out"] for dst_out_map in dst_out_map_list]

        self.assertTrue(not w_list[0].equals(w_list[1]))
        for i in range(2, len(label_list)):
            self.assertTrue(w_list[1].equals(w_list[i]))
