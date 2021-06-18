"""
This module provide both function which should be called from REPL and a CLI based on clize
"""

from pathlib import Path
import shutil
import datetime
import time
import json
from typing import List
from warnings import warn


selectd_name_list_path_default = Path(__file__).parent / "non_modified_files.json"
if selectd_name_list_path_default.exists():
    with open(selectd_name_list_path_default) as f:
        selected_name_list_default = json.load(f)
else:
    selected_name_list_default = None
    warn("selectd_name_list_path_default doesn't existed, run `extract_non_modified_files` to create it")

def create_simulation(origin_root:str, target_root:str, selected_name_list=None, verbose:bool=True):
    if selected_name_list is None:
        selected_name_list = selected_name_list_default

    origin_root = Path(origin_root)
    target_root: Path = Path(target_root)

    if target_root.exists():
        # target_root.rmdir()
        for f in target_root.iterdir():
            shutil.rmtree(f)
        if verbose:
            print(f"Deleted existed files in {target_root}")
    else:
        target_root.mkdir(exist_ok=False)

    link_list:List[Path] = list(origin_root.glob("*.exe")) + [origin_root / name for name in selected_name_list]

    for src in link_list:
        dst = target_root / src.relative_to(origin_root)
        #"""
        # method 0 (x)
        dst.symlink_to(src)
        #"""
        """
        # method 1 (x)
        if src.suffix == ".exe":
            dst.symlink_to(src)
        else:
            shutil.copy(src, dst)
        """
        """
        # method 2 (x)
        shutil.copy(src, dst)
        """

    if verbose:
        print(f"Done: Symbolinking all files from {target_root} to {origin_root}")

"""
def _create_experiment(origin_root:str, target_root:str, verbose=True):
    # symbolic link all files from origin_root to target_root
    origin_root = Path(origin_root)
    target_root: Path = Path(target_root)

    if target_root.exists():
        # target_root.rmdir()
        shutil.rmtree(target_root)
        print(f"Deleted existed {target_root}")
    target_root.mkdir(exist_ok=False)

    for src in origin_root.iterdir():
        dst = target_root / src.relative_to(origin_root)
        dst.symlink_to(src)

    if verbose:
        print(f"Done: Symbolinking 'real' *.inp file and *.exe files from {target_root} to {origin_root}")
"""

    

if __name__ == "__main__":
    import clize
    clize.run(create_simulation)
