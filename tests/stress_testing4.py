import os
from tqdm import tqdm

from .test_iwind_lr_tools import compare_out_map, name_suit, debug_env, data_map_fill, run_batch,\
     create_environment_fast_only
from iwind_lr_tools import Runner

ori_root = os.environ["WATER_ROOT"]


if __name__ == "__main__":        

    root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit(ori_root)

    data_map_list = [actioner.data_map]

    with debug_env() as debug_list:
        out_map_ori, = run_batch(root, data_map_list, debug_list=debug_list, sequential=True)
        dst_root = debug_list[0].dst_root

        for i in tqdm(range(1000)):
            runner = Runner(dst_root, without_create_simulation=True)
            out_map = runner.run({}) # override zero files
            compare_out_map(out_map_ori, out_map)
