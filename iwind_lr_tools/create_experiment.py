
# This module provide both function which should be called from REPL and a CLI based on clize

from pathlib import Path
import shutil

def create_experiment(origin_root:str, target_root:str, verbose=True):
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
        print(f"Done: Symbolinking all files from {target_root} to {origin_root}")

if __name__ == "__main__":
    import clize
    clize.run(create_experiment)
