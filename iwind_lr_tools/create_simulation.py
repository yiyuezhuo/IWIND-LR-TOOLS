"""
This module provide both function which should be called from REPL and a CLI based on `clize`
"""

from .utils import copy_locked, symlink_locked
from os import symlink
from pathlib import Path
# import shutil
# import datetime
# import time
import json
from typing import List
from warnings import warn
import logging


selectd_name_list_path_default = Path(__file__).parent / "non_modified_files.json"
if selectd_name_list_path_default.exists():
    with open(selectd_name_list_path_default) as f:
        selected_name_list_default = json.load(f)
else:
    selected_name_list_default = None
    warn("selectd_name_list_path_default doesn't existed, run `extract_non_modified_files` to create it")

def create_simulation(origin_root:str, target_root:str, selected_name_list=None):
    if selected_name_list is None:
        selected_name_list = selected_name_list_default

    origin_root = Path(origin_root)
    target_root: Path = Path(target_root)

    if target_root.exists():
        # target_root.rmdir()
        for f in target_root.iterdir():
            # shutil.rmtree(f)
            f.unlink()
        
        logging.debug(f"Deleted existed files in {target_root}")
    else:
        target_root.mkdir(exist_ok=False)

    link_list:List[Path] = list(origin_root.glob("*.exe")) + [origin_root / name for name in selected_name_list]

    for dst in link_list:
        src = target_root / dst.relative_to(origin_root)
        #"""
        # method 0 (x)
        #dst.symlink_to(src)
        # This race-condition shit waste my a lot of time. 
        #"""
        symlink_locked(src, dst)
        assert src.is_file(), "Created symlink failed?????"
        #"""
        #"""
        """
        # method 1 (x)
        if src.suffix == ".exe":
            dst.symlink_to(src)
        else:
            shutil.copy(src, dst)
        """
        
        # method 2 (x)
        # shutil.copy(src, dst)
        # copy_locked(dst, src)

    logging.info(f"Base Env symbol link {target_root} -> {origin_root}")
    

if __name__ == "__main__":
    import clize
    clize.run(create_simulation)
