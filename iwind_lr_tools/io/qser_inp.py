
from typing import List

from .utils import path_to_lines
from .common import Node, FlowNode, CommentNode #, NodeListSuit

def iter_strip(lines):
    for line in lines:
        yield line.strip()


@path_to_lines
def parse(lines: List[str]):
    comment_lines = []

    # it_lines = iter(lines)
    it_lines = iter_strip(lines)

    for line in it_lines:
        # if line.strip()[0] == "#":
        if line[0] == "#":
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

def get_flow_node_map(node_list: List[Node]):
    # get a "view" for node list to help navigation and select desired object.
    flow_node_list = [node for node in node_list if isinstance(node, FlowNode)]
    flow_node_map = {node.get_name(): node for node in flow_node_list}
    return flow_node_map

def get_df_map(node_list: List[Node]):
    return {k: flow_node.get_df() for k, flow_node in get_flow_node_map(node_list).items()}

"""
class QserSuit(NodeListSuit):
    def _get_df_node_map(self):
        return get_flow_node_map(self.node_list)

    def _get_df_map(self):
        return get_df_map(self.node_list)

    def f(self):
        pass
"""
