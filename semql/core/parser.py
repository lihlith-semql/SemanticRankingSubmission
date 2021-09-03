from semql.core.ast import *
from typing import Dict


class TreeParser:
    def __init__(self, tree_dict: List[Dict], cls_name_to_class: Dict[str, type]):
        """
        This class handles the translation form a JSON file to internal Tree representation.
        :param tree_dict: The JSON file, which is already parsed.
        :param cls_name_to_class: Mapping between names of classes (strings) and the Operation type.
        """
        self.tree_dict = tree_dict
        self.cls_name_to_class = cls_name_to_class

    def parse_node_dict(self, node_dict: Dict):
        args = {}
        for k, v in node_dict['arguments'].items():
            args[k] = v

        for child_id, child_argname in node_dict['children'].items():
            child_node_dict = [node_dict for node_dict in self.tree_dict if node_dict['node_id'] == child_id][0]
            child_object = self.parse_node_dict(child_node_dict)
            args[child_argname] = child_object

        cls = self.cls_name_to_class[node_dict['operation']]

        operation = cls(**args)
        operation.tokens = node_dict.get('tokens', [])
        return operation

    def _legacy_recursion(self, root_op: Dict, idx_to_op: Dict):
        """
        This parser serves to parse the trees produced by the current annotation Tool. Will be removed as soon as the anntoaton tool integrates this library.
        :param root_op:
        :param idx_to_op:
        :return:
        """
        operation = root_op['operation']
        cls = self.cls_name_to_class[operation]
        return_op = None
        if cls == ExtractValues:
            attribute_name = root_op['arguments']['attribute']
            table_op = idx_to_op[root_op['inputNodes'][0]]
            table = self._legacy_recursion(table_op, idx_to_op)
            return_op = ExtractValues(table, attribute_name)
        elif cls == Filter:
            attribute_name = root_op['arguments']['attribute']
            comp_operation = root_op['arguments']['comparisonOperator']
            if comp_operation == '><':
                larger_value = root_op['arguments']['largerValue']
                smaller_value = root_op['arguments']['smallerValue']
                value = '{} < {}'.format(smaller_value, larger_value)
            else:
                value = root_op['arguments']['value']
            table_op = idx_to_op[root_op['inputNodes'][0]]
            table = self._legacy_recursion(table_op, idx_to_op)
            return_op = Filter(table, attribute_name, comp_operation, value)
        elif cls == Merge:
            attribute_name0 = root_op['arguments']['mergeAttribute1']
            attribute_name1 = root_op['arguments']['mergeAttribute2']
            table_op0 = idx_to_op[root_op['inputNodes'][0]]
            table_op1 = idx_to_op[root_op['inputNodes'][1]]
            table0 = self._legacy_recursion(table_op0, idx_to_op)
            table1 = self._legacy_recursion(table_op1, idx_to_op)
            return_op = Merge(table0, table1, attribute_name0, attribute_name1)
        elif issubclass(cls, SetOperations):
            table_op0 = idx_to_op[root_op['inputNodes'][0]]
            table_op1 = idx_to_op[root_op['inputNodes'][1]]
            table0 = self._legacy_recursion(table_op0, idx_to_op)
            table1 = self._legacy_recursion(table_op1, idx_to_op)
            return_op = cls(table0, table1, 'None', 'None')
        elif cls == Count:
            table_op = idx_to_op[root_op['inputNodes'][0]]
            table = self._legacy_recursion(table_op, idx_to_op)
            return_op = Count(table)
        elif cls == Distinct:
            table_op = idx_to_op[root_op['inputNodes'][0]]
            table = self._legacy_recursion(table_op, idx_to_op)
            return_op = Distinct(table)
        elif cls in [Sum, Average]:
            table_op = idx_to_op[root_op['inputNodes'][0]]
            table = self._legacy_recursion(table_op, idx_to_op)
            return_op = Sum(table, 'None')
        elif cls == GetData:
            table_name = root_op['arguments']['table']
            return_op = GetData(table_name)

        return_op.tokens = root_op['tokenIndices']
        return return_op

    def _parse_legacy_json(self, op_dict: Dict):
        operations = op_dict['operationNodes']
        idx_to_op = {int(idx): op for idx, op in operations.items()}
        max_val = max(idx_to_op.keys())
        root_op = idx_to_op[max_val]
        return self._legacy_recursion(root_op, idx_to_op)

    def parse_dict(self) -> Operation:
        start_dict = [node_dict for node_dict in self.tree_dict if node_dict['node_id'] == '0'][0]
        tree = self.parse_node_dict(start_dict)
        return tree
