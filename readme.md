
# Tools to create, manipulate and analyze IWIND-LR input and output file.

## DISCLAIMER

Since no detailed `*.inp` and `*.OUT` specification is given, the "projection" (convert `*.inp` into object, modify it in Python and write it back to `*.inp`) may be subject to unknown risk and future modification of `*.exe`.

Tested version: `EFDCLGT_LR_ver4.17.exe`.

## Install

In the directory which holds `setup.py`.

```shell
pip install -e .
```

## Usage

```python
import iwind_lr_tools
```

### 

## Development

In jupyter notebook / IPython:

```python
%load_ext autoreload
%autoreload 2

from iwind_lr_tools.dev_tools import *
```

CLI debug:

```shell
ipython -i -m iwind_lr_tools.some_cli_supported_module -- arg1 arg2
```

Changes in source will reflect on the session in real time.

## Test

### Environment variables:

* `WATER_ROOT`: An environment variable which points to a "reference" Input folder.
* `WATER_EFDC_FAST`: An environment variable which points to a "fast" efdc.inp file (ex: simulation time is 1 day rather than 100+) . This file will override `WATER_ROOT` version in a new directory.


```shell
pytest
```

Test To start pdb when error is raised:

```shell
pytest -k "test_runner_wqpsc_node_list" --pdb
```

To obtain coverage report:

```shell
coverage run -m pytest
coverage report -m
```

HTML version:

```shell
coverage html
```

Check `htmlcov/index.html`.

## Notes

* As someone pushed me to present "result" as early as possible, I gave up on writing more unit tests to consolidate the code.
* The test sometimes will randomly fail, though if you run it again it will pass. I have not investigated the cause.
* This module is designed to consider "resume" ability, however this means that some temp files are created but never be cleaned automatically. You can check and delete these files manually in `C:\Users\{your_windows_username}\AppData\Local\Temp\` or use Windows 10 cleanup to delete these temp files.
* This module requires administrator privilege to symbol link some file (ex: the exe file.) to reduce disk usage, but Windows requires AP to do it. It's so bad at this time, many other software like Julia also need this to do even normal things, while Windows 10 users still don't know what they are doing. It's not hard to modify code to use copying instead of  symbol linking.
* Don't forget to turn off Windows 10 realtime protection to speed it up. See task manager to check how the IO triggers windows real time protection almost reduces half performance.