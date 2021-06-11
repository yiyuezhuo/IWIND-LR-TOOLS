
from pathlib import Path
import subprocess

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

def run_simulation(root: str):
    exe_p = get_exe_p(root)
    return subprocess.run(str(exe_p), cwd=str(root))
