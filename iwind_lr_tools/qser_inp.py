
from types import prepare_class
from .utils import path_to_lines
from typing import List

@path_to_lines
def parse(lines: List[str]):
    len_lines = len(lines)
    i = 0
    comment_lines = []
    while i < len_lines:
        line = lines[i]
        if line.strip()[0] == "#":
            comment_lines.append(line)
            i += 1
        else:
            break
    line_s = line.split()
    assert len(line_s) == 8
    if line_s[1] == 0:
        pass
    