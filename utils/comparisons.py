from collections import Counter
from semql.core.ast import Operation, Merge, Filter, ProjectionRoot


def str_eq(tree1: Operation, tree2: Operation) -> bool:
    return tree1.print() == tree2.print()


def normalize_node(node: Operation):
    if isinstance(node, Merge):
        min_a = min(node.attribute_name0 or "", node.attribute_name1 or "")
        max_a = max(node.attribute_name0 or "", node.attribute_name1 or "")
        return f"Merge({min_a}, {max_a})"
    if isinstance(node, Filter) and isinstance(node.value, str):
        node.value = node.value.lower()
        node_copy = node.shallow_copy()
        node_copy.value = 'dummy'
        return node.print_node()
    if isinstance(node, ProjectionRoot):
        attr_txts = sorted([
            ProjectionRoot.attr2txt(a_str, fn)
            for a_str, fn in node.attrs
        ])
        attr_str = ", ".join(attr_txts)
        return f"Projection({attr_str}, distinct={node.distinct})"
    else:
        return node.print_node()


def comp_eq(tree1: Operation, tree2: Operation):
    nodes1 = Counter(normalize_node(n) for n in tree1.list_of_nodes())
    nodes2 = Counter(normalize_node(n) for n in tree2.list_of_nodes())
    return nodes1 == nodes2
