
# This module provide both function which should be called from REPL and a CLI based on clize

from pathlib import Path
import shutil
import datetime
import time
import json


# from .utils import get_exe_p

def create_simulation(origin_root:str, target_root:str, selectd_name_list_path:str="", verbose:bool=True):
    if selectd_name_list_path == "":
        selectd_name_list_path = Path(__file__).parent / "non_modified_files.json"
    with open(selectd_name_list_path) as f:
        selected_name_list = json.load(f)

    origin_root = Path(origin_root)
    target_root: Path = Path(target_root)

    if target_root.exists():
        # target_root.rmdir()
        shutil.rmtree(target_root)
        if verbose:
            print(f"Deleted existed {target_root}")
    target_root.mkdir(exist_ok=False)

    link_list = list(origin_root.glob("*.exe")) + [origin_root / name for name in selected_name_list]

    for src in link_list:
        dst = target_root / src.relative_to(origin_root)
        dst.symlink_to(src)

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
