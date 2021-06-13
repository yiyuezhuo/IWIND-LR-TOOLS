"""
This test create two isolated environment
"""

if __name__ == "__main__":

    from subprocess import Popen
    import os
    import tempfile

    from iwind_lr_tools import create_simulation, run_simulation

    from .utils import report_file_difference, report_compare

    root_env_name = "WATER_ROOT"

    if root_env_name not in os.environ:
        print(f"Set environment {root_env_name} to run this test")
        exit()

    root = os.environ[root_env_name]

    with tempfile.TemporaryDirectory() as target_root:
        print(f"Created temp folder {target_root}")

        create_simulation(root, target_root)

        root_list = [root, target_root]
        process_list = [run_simulation(r, popen=True) for r in root_list]
        for p in process_list:
            p.wait()

        report_file_difference(root, target_root)
        report_compare(root, target_root)
        report_compare(target_root, root)

    
