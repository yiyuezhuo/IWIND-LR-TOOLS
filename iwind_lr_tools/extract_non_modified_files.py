
from pathlib import Path
import datetime
# import subprocess
import json
import time

from .utils import get_exe_p, run_simulation


def iter_date(p_list):
    for p in p_list:
        timestamp = p.stat().st_mtime
        yield datetime.datetime.fromtimestamp(timestamp)

def extract_non_modified_files(root):
    root = Path(root)
    now_date = datetime.datetime.fromtimestamp(time.time())

    p_list = sorted(root.iterdir())
    date_list = list(iter_date(p_list))

    assert all([date < now_date for date in date_list])

    """
    exe_p = get_exe_p(root)
    subprocess.run(str(exe_p), cwd=str(root))
    """
    run_simulation(root)

    date_list = list(iter_date(p_list))
    assert not all([date < now_date for date in date_list])

    selected_p_list = [p for p, date in zip(p_list, date_list) if date < now_date]
    selected_name_list = [p.name for p in selected_p_list if p.suffix != ".exe"]

    return selected_name_list


def run_extract_non_modified_files(root: str, target:str=""):
    if target == "":
        target = Path(__file__).parent / "non_modified_files.json"
    else:
        target = Path(target)
    
    selected_name_list = extract_non_modified_files(root)
    with open(target, "w") as f:
        json.dump(selected_name_list, f, indent=0)
    print(f"Created {str(target)}")


if __name__ == "__main__":
    import clize
    clize.run(run_extract_non_modified_files)
