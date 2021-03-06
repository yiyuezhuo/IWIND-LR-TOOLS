"""
Fast version will copy original root and copy a human-wrote "efdc.inp" (simulate only 3 days) file
to override the original one. Then symbolic the overided one.
Fast version is not guaranteed to be correct (though it will catch most possible errors) and 
is able to run (a new human- file may be required).

pytest refactored versopn
"""

import pytest

import os
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from contextlib import contextmanager
import shutil
from typing import List
import pandas as pd
import logging

from iwind_lr_tools import Actioner, Runner, run_batch, Runner, restart_batch, fork
# import iwind_lr_tools
from iwind_lr_tools.runner import data_map_fill #, append_out_map
from iwind_lr_tools.collector import dumpable_list, get_all
from iwind_lr_tools.fault_tolerant_pool import YPool

MIN_SIMULATION_TIME = 1.0 # It seeems that there's round in the model program, 0.9 -> 0, 1.5 -> 1 etc.

ori_root = os.environ["WATER_ROOT"]
efdc_fast = os.environ["WATER_EFDC_FAST"]
non_fast_mode = "WATER_STRICT" in os.environ

"""
@contextmanager
def create_environment_fast_only(ori_root):
    with TemporaryDirectory() as temp_path:
        root = Path(temp_path) / "root"
        shutil.copytree(ori_root, root)
        if not non_fast_mode:
            shutil.copy(efdc_fast, root / "efdc.inp")
            logging.info(f"Override {efdc_fast} -> {root / 'efdc.inp'}")
        yield root
"""

# debug purpose
@contextmanager
def create_environment_fast_only(ori_root):
    temp_path = mkdtemp()

    root = Path(temp_path) / "root"
    shutil.copytree(ori_root, root)
    if not non_fast_mode:
        shutil.copy(efdc_fast, root / "efdc.inp")
        
        logging.info(f"Override {efdc_fast} -> {root / 'efdc.inp'}")
    yield root

    # shutil.rmtree(temp_path)


@contextmanager
def debug_env():
    debug_list: List[Runner]  = []
    yield debug_list
    assert len(debug_list) > 0
    for runner in debug_list:
        runner.cleanup()


def file_eq(self, p1, p2, eq=True):
    with open(p1) as f:
        d1 = f.read()
    with open(p2) as f:
        d2 = f.read()
    if eq:
        self.assertEqual(d1, d2)
    else:
        self.assertNotEqual(d1, d2)

def name_suit(root=None):
    root = Path(ori_root) if root is None else Path(root)
    
    data = data_map, df_node_map_map, df_map_map = get_all(root)
    actioner = Actioner(*data)
    actioner.set_simulation_length(MIN_SIMULATION_TIME)
    return root, data, data_map, df_node_map_map, df_map_map, actioner


def test_fast_too_small():
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()

    # actioner_list = [actioner]
    # kwargs_list = [actioner.to_run_strict_kwargs() for actioner in actioner]
    with debug_env() as debug_list:
        out_map, = run_batch(root, [data_map], debug_list=debug_list)

        assert out_map["qbal.out"].shape[0] > 1

def compare_out_map(out_map1, out_map2, neq=False):
    assert set(out_map1) == set(out_map2)

    for key in out_map1.keys():
        assert out_map1[key].shape[0] > 1
        if not neq:
            assert out_map1[key].shape == out_map2[key].shape
            assert out_map1[key].equals(out_map2[key])
        else:
            if key == "cumu_struct_outflow.out":
                if not out_map1["qbal.out"]["qctlo(million-m3)"].equals(out_map2["qbal.out"]["qctlo(million-m3)"]):
                    assert not out_map1[key].equals(out_map2[key])
            else:
                assert not out_map1[key].equals(out_map2[key])

def compare_out_map_weak(out_map1, out_map2):
    assert set(out_map1) == set(out_map2)

    for key in out_map1.keys():
        assert out_map1[key].shape[0] > 1
        assert out_map1[key].shape == out_map2[key].shape

def test_projection():
    with create_environment_fast_only(ori_root) as root:
        root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit(root)

        data_map_list = [data_map_fill({})]
        data_map_list.extend([data_map_fill({dumpable: data_map[dumpable]}) for dumpable in dumpable_list])
        with debug_env() as debug_list:
            out_map_list = run_batch(root, data_map_list, debug_list=debug_list)

            for i in range(1, len(out_map_list)):
                compare_out_map(out_map_list[0], out_map_list[i])

def test_more_time_sanity_check():
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()

    actioner_1plus = actioner.copy()
    actioner_1plus.set_simulation_length(MIN_SIMULATION_TIME * 2)

    with debug_env() as debug_list:
        data_map_list = [actioner.data_map, actioner_1plus.data_map]
        out_map, out_map_1plus = run_batch(root, data_map_list, debug_list=debug_list)

        compare_out_map(out_map, out_map_1plus, neq=True)

# @pytest.mark.xfail()
def test_environment_isolation():
    # return # disable this test for all-copying method
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()
    
    data_map_list = [
        data_map_fill({"efdc.inp": data_map["efdc.inp"]}),
        data_map_fill({"efdc.inp": data_map["efdc.inp"], "qser.inp": data_map["qser.inp"]})
    ]

    with debug_env() as debug_list:
        run_batch(root, data_map_list, debug_list=debug_list)
        dst_root_list = [runner.dst_root for runner in debug_list]
        efdc_stat_list = []
        qser_stat_list = []

        dst_root_list = [root] + [runner.dst_root for runner in debug_list]
        for dst_root in dst_root_list:
            dst_root = Path(dst_root)
            efdc_stat = (dst_root / "efdc.inp").stat()
            qser_stat = (dst_root / "qser.inp").stat()
            efdc_stat_list.append(efdc_stat)
            qser_stat_list.append(qser_stat)

        assert efdc_stat_list[0].st_mtime < efdc_stat_list[1].st_mtime
        assert efdc_stat_list[0].st_mtime < efdc_stat_list[2].st_mtime

        assert qser_stat_list[0].st_mtime == qser_stat_list[1].st_mtime
        assert qser_stat_list[0].st_mtime < qser_stat_list[2].st_mtime

def test_select_flow():
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()

    mode_list = ["iden", "hard", "flow", "qfactor"]
    idx_list = [0, 2]

    actioner_list = [actioner.copy() for mode in mode_list]
    mode2actioner = {mode:actioner for mode, actioner in zip(mode_list, actioner_list)}

    for mode, actioner in zip(mode_list, actioner_list):
        if mode == "iden":
            continue
        actioner.select_flow(idx_list, mode=mode)

    data_map_list = list(map(lambda actioner: actioner.data_map, actioner_list))

    with debug_env() as debug_list:
        out_map_list = run_batch(root, data_map_list, debug_list=debug_list)

        for i in range(1, len(mode_list)):
            compare_out_map(out_map_list[0], out_map_list[i], neq=True)

        for i in range(2, len(mode_list)):
            compare_out_map(out_map_list[1], out_map_list[i])

def test_restart_too_strong():
    # TODO: Due to numerical and output format problem, this test is disabled at this time.
    """
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()

    actioner_cont = actioner.copy() # cont -> continues
    actioner_cont.set_simulation_length(2*MIN_SIMULATION_TIME)

    with debug_env() as debug_list:
        data_map_list = [data_map, actioner_cont.data_map]

        out_map_left, out_map_cont = run_batch(root, data_map_list, debug_list=debug_list)
        
        compare_out_map(out_map_left, out_map_cont, neq=True)

        C02 = df_map_map["efdc.inp"]["C02"]
        C03 = df_map_map["efdc.inp"]["C03"]
        C02.loc[0, "ISRESTI"] = 1
        C03.loc[0, "TBEGIN"] = C03.loc[0, "TBEGIN"] + MIN_SIMULATION_TIME

        out_map_right, = restart_batch(debug_list[:1], data_map_list[:1])
        out_map = append_out_map(out_map_left, out_map_right)

        compare_out_map(out_map, out_map_cont)
    """

def test_restart_fork():
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()

    with debug_env() as debug_list:
        run_batch(root, [actioner], pool_size=1, debug_list=debug_list)
        runner_base = debug_list[0]
        runner_list = fork(runner_base, 2)
        actioner_base = actioner
        actioner_list = []
        for _ in range(2):
            actioner = actioner_base.copy()
            actioner.set_simulation_begin_time(actioner_base.get_simulation_begin_time() + MIN_SIMULATION_TIME)
            actioner.enable_restart()
            actioner_list.append(actioner)
        out_map_list = restart_batch(runner_list, actioner_list, pool_size=2)
        # compare_out_map_weak(out_map_list[0], out_map_list[1])
        compare_out_map(out_map_list[0], out_map_list[1])

"""
@pytest.mark.xfail()
def test_start_iterator_1day_plus():
    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit()
    begin_day = actioner.get_simulation_begin_time()
    end_day = begin_day + 2 * MIN_SIMULATION_TIME
    actioner_frozen = actioner

    dt_dummy = pd.to_datetime("1911-10-10")
    days = actioner.get_simulation_begin_time()

    it = start_iterator_1day_plus(begin_day, end_day, root, actioner_frozen, step=1, dt=dt_dummy)
    df = next(it)
    assert df.index[0] == dt_dummy + pd.DateOffset(int(days))
    assert df.index[-1].hour == 23
    df = next(it)
    assert df.index[0] == dt_dummy + pd.DateOffset(int(days + 1))
    assert df.index[-1].hour == 23
    i = 0
    for _ in it:
        i += 1
    assert i == 0
"""

class Baka(Exception):
    pass

class Tsundere:
    def __init__(self, hp):
        self.hp = hp
    
    def __call__(self, arg):
        self.hp -= 1
        if self.hp > 0:
            raise Baka("Tsundere: Baka")
        return True

def test_ypool_tsundere():
    pool = YPool(3, quota=3)
    for hp in [2, 3]:
        # poor Tsundere
        func = Tsundere(hp)
        with pytest.warns(UserWarning):
            assert len(pool.map(func, [1])) >  0

    # strong Tsundere
    func = Tsundere(4)
    with pytest.raises(Exception): # For some reasons, `pytest.raises(Baka)` doesn't works.
        with pytest.warns(UserWarning):
            pool.map(func, [1])
