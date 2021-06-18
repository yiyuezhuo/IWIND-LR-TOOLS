
from pathlib import Path
import subprocess
from threading import Lock
from tempfile import mkdtemp
 
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

def run_simulation(root: str, popen=False):
    exe_p = get_exe_p(root)
    command = str(exe_p)
    cwd = str(root)
    if popen:
        return subprocess.Popen(command, cwd=cwd)
    return subprocess.run(command, cwd=cwd)

def open_safe(path, mode, **kwargs):
    # Prevent symbolic link occastionally rewrite to the orignal file
    assert mode in {"w", "wb"}
    p = Path(path)
    if p.exists():
        p.unlink()
    return open(p, mode, **kwargs)

lock = Lock()

def mkdtemp_locked():
    with lock:
        return mkdtemp()
