
from typing import List

from .utils import path_to_lines
from .common import FlowNode, CommentNode


@path_to_lines
def parse(lines: List[str]):
    comment_lines = []

    it_lines = iter(lines)

    for line in it_lines:
        if line.strip()[0] == "#":
            comment_lines.append(line)
        else:
            break
    
    comment_node = CommentNode.from_str_list(comment_lines)
    assert len(line.strip().split()) == 8
    content_lines = [line]

    flow_node_list = []
    for line in it_lines:
        if len(line.strip().split()) == 8:
            flow_node = FlowNode(content_lines)
            flow_node_list.append(flow_node)
            content_lines = [line]
        else:
            content_lines.append(line)
    if len(content_lines) > 0:
        flow_node_list.append(FlowNode(content_lines))

    node_list = [comment_node] + flow_node_list

    return node_list
