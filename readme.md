
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

## Test

### Enviroment variables:

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

* It's strange that qser's `nlines` can be `0` so I am forced to check number of cell for every line to determine the shape of a dataframe.
* The test sometimes will randomly fail, though if you run it again it will pass. I have not investigate the cause.
