from semql.core.ast import *


# TODO from this, we make the operations for the expert. Adding a node from the reference tree to a partial solution.


def is_identical(tree: Operation, candidate_tree: Operation, partial_eq=False):
    root_eq = tree.node_equality(candidate_tree, partial_eq=partial_eq, ignore_value=True)
    child_eq = []
    for child, candidate_child in zip(tree.children, candidate_tree.children):
        child_eq.append(is_identical(child, candidate_child, partial_eq=partial_eq))

    if (root_eq or type(candidate_tree) == NoOp) and not False in child_eq:
        return True
    else:
        return False


def is_subtree(tree: Operation, candidate_tree: Operation, return_original_node=False):
    if type(candidate_tree) == NoOp:
        return True

    if tree.deep_structural_equality(candidate_tree):
        return True

    subtree_eq = []
    for child in tree.children:
        eq = is_subtree(child, candidate_tree)
        if eq:
            subtree_eq.append(child)

    if len(subtree_eq) > 0 :
        return True
    else:
        return False


def get_next_ops(tree: Operation, candidate_tree: Operation) -> List[Operation]:
    root_eq = is_identical(tree, candidate_tree)#tree.node_equality(candidate_tree)
    child_eq = []
    for child, candidate_child in zip(tree.children, candidate_tree.children):
        next_ops = get_next_ops(child, candidate_child)
        if next_ops is None:
            return None

        child_eq.extend(next_ops)

    if type(candidate_tree) == NoOp:
        child_eq.append(tree)
    elif not root_eq and type(candidate_tree) is not NoOp:
        return None

    return child_eq


def potential_next_ops(gold_tree: Operation, candidate_tree: Operation) -> List[Operation]:
    if type(candidate_tree) == NoOp:
        return [gold_tree]

    if len(gold_tree.children) == 0 and len(candidate_tree.children) and gold_tree.node_equality(candidate_tree):
        return []

    next_ops = get_next_ops(gold_tree, candidate_tree)

    #in case the trees are not equal
    if next_ops is not None and gold_tree.parent is not None:
        next_ops += [gold_tree.parent]
    if next_ops is not None:
        return next_ops

    subtree_eq = []
    for child in gold_tree.children:
        eq = potential_next_ops(child, candidate_tree)
        subtree_eq.extend(eq)

    return subtree_eq


def append_op(tree: Operation, candidate_tree: Operation) -> Operation:
    if type(candidate_tree) == NoOp:
        new_op = type(tree)()
        if type(new_op) == GetData:
            new_op.data_source = tree.data_source
        new_op.replace_op(candidate_tree)
        if new_op is not None:
            if new_op.parent is not None:
                return new_op.parent
            else:
                return new_op
    else:
        for child, candidate_child in zip(tree.children, candidate_tree.children):
            new_op = append_op(child, candidate_child)
            if new_op is not None:
                if new_op.parent is not None:
                    return new_op.parent
                else:
                    return new_op
        if not tree.node_equality(candidate_tree, partial_eq=False, ignore_value=True):
            new_op = tree.deepcopy()
            if type(new_op) == GetData:
                new_op.data_source = tree.data_source
            if candidate_tree.parent is None:
                return new_op
            new_op.replace_op(candidate_tree)
            return new_op


def append_next_op(tree: Operation, candidate_tree: Operation) -> Operation:
    if type(candidate_tree) == NoOp:
        return tree.shallow_copy()

    appended = False
    if is_identical(tree, candidate_tree, partial_eq=True):
        new_op = append_op(tree, candidate_tree)
        #in case there is no NoOp add a new root
        if not new_op and tree.parent is not None:
            candidate_tree.copy_parent_from_op(tree)
            appended = True
            candidate_tree = candidate_tree.parent
        elif new_op is not None:
            candidate_tree = new_op
            appended = True
    else:
        for child in tree.children:
            new_candidate_tree = append_next_op(child, candidate_tree)
            if new_candidate_tree is not None:
                appended = True
                candidate_tree = new_candidate_tree
                break

    if appended:
        return candidate_tree
    else:
        return None



