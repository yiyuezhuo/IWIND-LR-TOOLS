
# Tools to create, manipuate and analyze IWIND-LR input and output file.

## DISCLAIMER

Since no detailed `*.inp` and `*.OUT` specification is given, the "projection" (convert `*.inp` into object, modify it in Python and write it back to `*.inp`) may be subject to unknown risk and future modification of `*.exe`.

Tested version: `EFDCLGT_LR_ver4.16.exe`.

## Install

In the directory which holds `setup.py`.

```shell
pip install -e .
```

## Usage

```python
import iwind_lr_tools
```

### CLI tools

* `python -m iwind_lr_tools.extract_non_modified_files input_root`: Run exe file and extract file names which are not modified during the running ("real" input files). A `non_modified_files.json` file result will be generated or updated. 
* `python -m iwind_lr_tools.create_simulation input_root_src input_root_dst`: Non-modifed files and exe file wil be symbolink to target directory, so a "isolated" simulation can be run.


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
