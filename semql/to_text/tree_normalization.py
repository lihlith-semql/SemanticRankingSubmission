
from semql.core.ast import *

from semql.to_text.util import get_table_name


def extract_filter_chains(root: Operation):

    def bottom_up(leaf: Operation):
        assert len(leaf.children) == 0
        assert isinstance(leaf, GetData)
        res = []
        node = leaf
        while node:
            if isinstance(node, Filter):
                if get_table_name(node.attribute_name) == leaf.table_name:
                    res.append({
                        'type': Filter,
                        'args': {
                            'attribute_name': node.attribute_name,
                            'operation': node.operation,
                            'value': node.value,
                        }
                    })
            elif isinstance(node, Max):
                if get_table_name(node.attribute_name) == leaf.table_name:
                    res.append({
                        'type': Max,
                        'args': {
                            'attribute_name': node.attribute_name,
                        }
                    })
            elif isinstance(node, Min):
                if get_table_name(node.attribute_name) == leaf.table_name:
                    res.append({
                        'type': Min,
                        'args': {
                            'attribute_name': node.attribute_name,
                        }
                    })

            node = node.parent

        return res

    return {
        id(leaf): bottom_up(leaf=leaf)
        for leaf in root.return_leaves()
    }


def delete_filter_min_max(op: Operation):
    if any(isinstance(op, to_delete) for to_delete in {Filter, Min, Max}):
    # if isinstance(op, Filter):
        return delete_filter_min_max(op.children[0])
    elif isinstance(op, GetData):
        return op
    else:
        new = op.shallow_copy()
        new_children = [
            delete_filter_min_max(child)
            for child in op.children
        ]
        for child in new_children:
            child.parent = new
        new.set_children(new_children)
        return new


def reinsert(op, filter_chains):
    if isinstance(op, GetData):
        chain = filter_chains.get(id(op), [])
        current = op
        for elem in chain:
            new_op = elem['type'](table=current, **elem['args'])
            current = new_op
        return current
    else:
        new = op.shallow_copy()
        new_children = [
            reinsert(child, filter_chains)
            for child in op.children
        ]
        for child in new_children:
            child.parent = new
        new.set_children(new_children)
        return new


def normalize(tree: Operation):
    chains = extract_filter_chains(tree)
    new = delete_filter_min_max(tree)
    new = reinsert(new, chains)
    return new
