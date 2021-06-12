
from pathlib import Path

def file_difference(root1, root2):
    return set(Path(root1).iterdir()) - set(Path(root2).iterdir())

def report_file_difference(root1, root2):
    print(f"set({root1}) - set({root2})")
    print(file_difference(root1, root2))

    print(f"set({root2}) - set({root1})")
    print(file_difference(root2, root1))

def report_compare(src_root, dst_root):
    src_root = Path(src_root)
    dst_root = Path(dst_root)
    neq_list = []

    for dp in dst_root.iterdir():
        sp = src_root / dp.name
        if not sp.exists():
            print(f"{dp} existed but {sp} doesn't existed")
            continue
        with open(sp, "rb") as sf:
            sd = sf.read()
        with open(dp, "rb") as _df:
            dd = _df.read()
        is_eq = sd == dd
        print(f"{dp} vs {sp}: {is_eq}")
        if not is_eq:
            neq_list.append(dp)
        print("neq_list: {neq_list}")