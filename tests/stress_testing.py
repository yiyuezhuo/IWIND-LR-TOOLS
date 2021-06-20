
import os
from tqdm import tqdm

from .test_iwind_lr_tools import compare_out_map, name_suit, debug_env, data_map_fill, run_batch,\
     create_environment_fast_only

ori_root = os.environ["WATER_ROOT"]

if __name__ == "__main__":
    with create_environment_fast_only(ori_root) as root:
        for i in tqdm(range(1000)):
            root, data, data_map, df_node_map_map, df_map_map, actioner = name_suit(root)

            data_map_list = [data_map_fill({}), data_map]

            with debug_env() as debug_list:
                out_map_list = run_batch(root, data_map_list, debug_list=debug_list)
                compare_out_map(out_map_list[0], out_map_list[1])
