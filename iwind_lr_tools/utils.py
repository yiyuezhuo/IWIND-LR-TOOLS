
from pathlib import Path
import subprocess
from threading import Lock
from tempfile import mkdtemp
import pandas as pd
import shutil
from warnings import warn
 
def path_to_lines(func):
    def _func(p):
        with open(p, encoding="utf8") as f:
            lines = f.readlines()
        return func(lines)
    return _func

def path_to_text(func):
    def _func(p):
        with open(p, encoding="utf8") as f:
            lines = f.read()
        return func(lines)
    return _func

def get_exe_p(root: str):
    root = Path(root)
    exe_p_list = list(root.glob("*.exe"))
    assert len(exe_p_list) == 1, "exe file missing or ambiguous, please leave exact 1."
    exe_p = exe_p_list[0]
    return exe_p

exe_version = (4, 17)

def check_exe_version_number(p: Path):
    try:
        major, minor = p.stem.split("ver").split(".")
        assert major >= exe_version[0]
        assert minor >= exe_version[1]
    except:
        warn(f"Checking version for {p} failed, expected version {exe_version}")


def run_simulation(root: str, popen=False):
    exe_p = get_exe_p(root)
    command = str(exe_p)
    cwd = str(root)
    if popen:
        return subprocess.Popen(command, cwd=cwd)
    # return subprocess.run(command, cwd=cwd)
    return subprocess.check_output(command, cwd=cwd)

def open_safe(path, mode, **kwargs):
    # Prevent symbolic link occasionally rewrite to the original file
    assert mode in {"w", "wb"}
    p = Path(path)
    if p.exists():
        p.unlink()
    return open(p, mode, **kwargs)

mkdtemp_lock = Lock()

def mkdtemp_locked():
    with mkdtemp_lock:
        return mkdtemp()

symlink_lock = Lock()

def symlink_locked(src:Path, dst:Path):
    with symlink_lock:
        src.symlink_to(dst)

copy_lock = Lock()

def copy_locked(src, dst):
    with copy_lock:
        shutil.copy(src, dst)

class YPool:
    def __self__(self, pool_size):
        pass