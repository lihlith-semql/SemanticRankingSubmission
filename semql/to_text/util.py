
from typing import List, Callable

from semql.to_text.config import UNSUPPORTED_OPS
from semql.core.ast import GetData, Operation


def contains_unsupported(op):
    if any(isinstance(op, unsupported) for unsupported in UNSUPPORTED_OPS):
        return True
    else:
        return any(contains_unsupported(child) for child in op.children)


def only_known(op, db_meta):
    if isinstance(op, GetData):
        return op.table_name in set(db_meta['tables'].keys())
    elif any(isinstance(op, cls) for cls in UNSUPPORTED_OPS):
        return False
    else:
        return all([only_known(child, db_meta) for child in op.children])


def unique_leaves(op: Operation):
    leaves = op.return_leaves()
    num_leaves = len(leaves)
    num_uniq = len({l.print_node() for l in leaves})
    return num_leaves == num_uniq


def accept_tree(op: Operation, db_meta):
    if contains_unsupported(op):
        return False

    return unique_leaves(op) and only_known(op, db_meta)


def get_table_name(attr_str):
    tokens = attr_str.split('.')

    if len(tokens) != 2:
        raise ValueError(f"cant handle attribute string: {attr_str}")

    return tokens[0]


def get_attr_name(attr_str):
    tokens = attr_str.split('.')

    if len(tokens) != 2:
        raise ValueError(f"cant handle attribute string: {attr_str}")

    table_name, attr_name = tokens
    return attr_name


def get_table(attr_str, db_meta_data):
    tokens = attr_str.split('.')

    if len(tokens) != 2:
        raise ValueError(f"cant handle attribute string: {attr_str}")

    table_name, _ = tokens
    return db_meta_data['tables'][table_name]


def get_attr(attr_str, db_meta_data):
    tokens = attr_str.split('.')

    if len(tokens) != 2:
        raise ValueError(f"cant handle attribute string: {attr_str}")

    table_name, attr_name = tokens
    return db_meta_data['tables'][table_name].attributes[attr_name]


def print_tree(tree: Operation, prefix=''):
    print(prefix + tree.print_node())
    for child in tree.children:
        print_tree(child, prefix + '\t')


def iter_nodes_by_predicate(tree: Operation, predicate: Callable[[Operation], bool]):
    if predicate(tree):
        yield tree

    for c in tree.children:
        yield from iter_nodes_by_predicate(c, predicate)
