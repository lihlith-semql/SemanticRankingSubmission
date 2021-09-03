from semql.core.ast import *
from random import choice, seed
from semql.tree_sampling.constraints import Constraints, FilterConstraint
from typing import Type

operations = [Filter, Merge, Done, GetData, ExtractValues, Sum, Average, IsEmpty, Union, NoOp, Distinct, Intersection, Difference]

final_ops = [cls for cls in operations if issubclass(cls, FinalOperation)]
table_ops = [cls for cls in operations if issubclass(cls, TableOperation)]
aggr_ops = [cls for cls in operations if issubclass(cls, Aggregation)]
list_ops = [cls for cls in operations if issubclass(cls, ListOperation)]


class ConstraintTreeGenerator:
    def __init__(self, constraint: Constraints):
        self.constraint = constraint
        self.tree_nodes = list()

    def generate_structure(self):
        """
        Generates the structure of the tree. Given a list of constraints it deduces the merge operations involved.
        The tree structure consists of GetData nodes and merges.

        In case the constraints contain a set operation, then the covering nodes are merged first

        :return: Merge operation
        """
        path = self.constraint.merge_path.merge_path
        tree = [GetData(self.constraint.data_name, node.name) for node in path]

        node_dict = self.constraint.graph.nodes
        if self.constraint.setop_constraint is not None:
            set_op_nodes = [GetData(self.constraint.data_name, node.name) for node in self.constraint.setop_constraint.final_window]
            while len(set_op_nodes) > 1:
                choice_idx = range(len(set_op_nodes) - 1)
                idx = choice(choice_idx)
                leaf0, leaf1 = set_op_nodes[idx], set_op_nodes[idx + 1]
                leaves0 = leaf0.return_leaves()
                right_most_leaf = leaves0[-1]

                leaves1 = leaf1.return_leaves()
                left_most_leaf = leaves1[0]
                arg0, arg1 = node_dict[right_most_leaf.table_name].neighbours[node_dict[left_most_leaf.table_name]]
                merge = Merge(leaf0, leaf1, '{}.{}'.format(right_most_leaf.table_name, arg0),
                              '{}.{}'.format(left_most_leaf.table_name, arg1))

                del set_op_nodes[idx:idx + 2]
                set_op_nodes.insert(idx, merge)

            set_op_tree = set_op_nodes[0]
            set_op_leaves = set_op_tree.return_leaves()
            indices = [tree.index(set_op_leaf) for set_op_leaf in set_op_leaves]
            min_idx = min(indices)
            max_idx = max(indices)
            del tree[min_idx: max_idx + 1]
            tree.insert(min_idx,set_op_tree)

        while len(tree) > 1:
            choice_idx = range(len(tree) - 1)
            idx = choice(choice_idx)
            leaf0, leaf1 = tree[idx], tree[idx + 1]
            leaves0 = leaf0.return_leaves()
            right_most_leaf = leaves0[-1]

            leaves1 = leaf1.return_leaves()
            left_most_leaf = leaves1[0]
            arg0, arg1 = node_dict[right_most_leaf.table_name].neighbours[node_dict[left_most_leaf.table_name]]
            merge = Merge(leaf0, leaf1, '{}.{}'.format(right_most_leaf.table_name, arg0), '{}.{}'.format(left_most_leaf.table_name, arg1))

            del tree[idx:idx + 2]
            tree.insert(idx, merge)
        return tree[0]

    def insert_set_operation(self, tree: Operation, setop_class: type):
        filter_constraint = self.constraint.setop_constraint.constraint_filter0
        leaves = tree.return_leaves()
        filter_leaf = [leaf for leaf in leaves if leaf.table_name == filter_constraint.table.name][0] #leaf on which the filter is applied
        attribute_leaf = [leaf for leaf in leaves if leaf.table_name == self.constraint.setop_constraint.attribute_table.name][0]
        path_to_root = filter_leaf.get_path_to_root()
        covering_operation = filter_leaf
        for operation in path_to_root:
            leaves = operation.return_leaves()
            if attribute_leaf in leaves:
                covering_operation = operation
                break

        # copy constraint
        copy_covering_operation0 = covering_operation.deepcopy()
        copy_covering_operation1 = covering_operation.deepcopy()

        copy_covering_operation0 = self.insert_single_constraint(copy_covering_operation0, self.constraint.setop_constraint.constraint_filter0)
        copy_covering_operation1 = self.insert_single_constraint(copy_covering_operation1, self.constraint.setop_constraint.constraint_filter1)

        arguments = {
            'table0': copy_covering_operation0,
            'table1': copy_covering_operation1,
            'attribute_name0': self.constraint.setop_attribute(),
            'attribute_name1': self.constraint.setop_attribute()
        }
        is_new_root = covering_operation.insert_node_on_path(setop_class, arguments)
        if is_new_root:
            tree = copy_covering_operation0.parent
        return tree

    def insert_single_constraint(self, tree: Operation, filter_constraint: FilterConstraint, leaf: GetData=None):
        if leaf is None:
            leaves = tree.return_leaves()
            leaf = [node for node in leaves if node.table_name == filter_constraint.table.name][0]
        path_to_root = leaf.get_path_to_root(tree) #root to leaf
        #check if there is a set operation:
        setop_idx = [i for i, op in enumerate(path_to_root) if issubclass(type(op), SetOperations)]
        if len(setop_idx) == 0:
            path_node: Operation = choice(path_to_root)
            arguments = {
                'table': path_node,
                'attribute_name': '{}.{}'.format(filter_constraint.table.name, filter_constraint.attribute_name),
                'operation': filter_constraint.operation,
                'value': filter_constraint.value,
            }
            is_new_root = path_node.insert_node_on_path(Filter, arguments)
            if is_new_root:
                tree = path_node.parent
        else:
            place_inside = choice([True, False])
            #in case the filter is not applied to the setup constraint table -> it must be placed before the setop
            if not filter_constraint.table == self.constraint.setop_constraint.attribute_table:
                place_inside = True

            idx = setop_idx[0]
            if place_inside:
                #add it to both the children of the setop
                set_operation = path_to_root[idx]
                self.insert_single_constraint(set_operation.children[1], filter_constraint)
                self.insert_single_constraint(set_operation.children[0], filter_constraint)
            else:
                #add it to the path before the setop
                path_node = choice(path_to_root[idx:])
                arguments = {
                    'table': path_node,
                    'attribute_name': '{}.{}'.format(filter_constraint.table.name, filter_constraint.attribute_name),
                    'operation': filter_constraint.operation,
                    'value': filter_constraint.value,
                }
                is_new_root = path_node.insert_node_on_path(Filter, arguments)
                if is_new_root:
                    tree = path_node.parent

        return tree

    def insert_groupby(self, tree: Operation):
        groupy_by_class = self.constraint.groupby_constraint.groupy_by_class
        op = groupy_by_class(tree, self.constraint.groupby_attribute(), self.constraint.groupby_aggregate_attr())
        if self.constraint.groupby_constraint.having_filter is not None:
            filter_constraint = self.constraint.groupby_constraint.having_filter
            if groupy_by_class == CountBy:
                attribute_name = 'count'
            else:
                attribute_name = '{}.{}'.format(filter_constraint.table.name, filter_constraint.attribute_name)
            op = Filter(op, attribute_name, filter_constraint.operation, filter_constraint.value)
        return op

    def insert_constraints(self, tree: Operation):
        """Given the tree structure, insert the filter operations. The filter operations can be inserted along the path from the
        appropriate GetData node to root"""
        leaves = tree.return_leaves()
        for filter_constraint in self.constraint.filter_constraints:
            leaf = [node for node in leaves if node.table_name == filter_constraint.table.name][0]
            tree = self.insert_single_constraint(tree, filter_constraint, leaf)
        return tree

    def add_final_operation(self, tree: TableOperation) -> FinalOperation:
        final_op = tree
        if self.constraint.get_query_type_class() in [Done, IsEmpty, Count] and self.constraint.aggregation_constraint is None:
            dist_op = Distinct(tree, self.constraint.result_type_str())
            extract_op = ExtractValues(dist_op, str(self.constraint.result_type))
            final_op = self.constraint.get_query_type_class()(extract_op)
        elif self.constraint.get_query_type_class() in [Sum, Average,]:
            entity_id = [attr[0] for attr in self.constraint.result_type.result_table.attributes if attr[2] == 'PRI'][0]
            if self.constraint.groupby_constraint is None:
                dist_op = Distinct(tree, '{}.{}'.format(self.constraint.result_type.result_table.name, entity_id))
            else:
                dist_op = Distinct(tree, self.constraint.result_type_str())
            final_op = self.constraint.get_query_type_class()(dist_op, self.constraint.result_type_str())
        elif self.constraint.aggregation_constraint is not None:
            agg_op = self.constraint.get_aggregation_class()(tree, self.constraint.aggregation_constraint.aggregation_attribute_name)
            extract_op = ExtractValues(agg_op, self.constraint.result_type_str())
            final_op = Done(extract_op)

        return final_op

    def list_of_nodes(self, tree: Operation):
        if type(tree) in [GetData, NoOp]:
            return [tree]
        else:
            nodes = []
            for child in tree.children:
                nodes.extend(self.list_of_nodes(child))
            return nodes + [tree]

    def generate_tree_from_constraints(self):
        tree = self.generate_structure()
        if self.constraint.setop_constraint is not None:
            tree = self.insert_set_operation(tree, self.constraint.setop_constraint.setop_class)
        tree = self.insert_constraints(tree)
        if self.constraint.groupby_constraint is not None:
            tree = self.insert_groupby(tree)
        tree = self.add_final_operation(tree)
        return tree

    def compute_complexity_score(self) -> float:
        score = 0.0
        score += len(self.constraint.merge_path)
        if self.constraint.groupby_constraint is not None:
            score += 1
        if self.constraint.setop_constraint is not None:
            score += 1
        if self.constraint.get_query_type_class() in [IsEmpty, Count]:
            score += 0.25
        elif self.constraint.get_query_type_class() in [Sum, Average]:
            score += 0.5
        score += 0.5*len(self.constraint.filter_constraints)
        if self.constraint.aggregation_constraint is not None:
            score += 0.5
        return score
