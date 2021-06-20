import os
from tqdm import tqdm
from pathlib import Path

from .test_iwind_lr_tools import compare_out_map, name_suit, debug_env, data_map_fill, run_batch,\
     create_environment_fast_only
from iwind_lr_tools import Runner


def run(dst_root: str):
    dst_root = Path(dst_root)
    runner = Runner(dst_root, without_create_simulation=True)
    out_map_ori = runner.run({})

    for i in tqdm(range(1000)):
        runner = Runner(dst_root, without_create_simulation=True)
        out_map = runner.run({}) # override zero files
        compare_out_map(out_map_ori, out_map)

if __name__ == "__main__":        

    import clize
    clize.run(run)