from abc import abstractmethod, ABC
from typing import List, Dict
from collections import defaultdict
from copy import deepcopy


class OpResult(object):
    """
    Stores the results returned when calling `Operation.run()`.
    """

    def __init__(self, data_dicts: List[Dict], column_names: List[str]):
        self._data_dicts = data_dicts
        self._column_names = column_names
        self._validate_and_load_columns()

    def get_columns(self) -> List[str]:
        return self._column_names

    def get_data(self) -> List[Dict]:
        return deepcopy(self._data_dicts)

    def get(self, idx: int) -> Dict:
        return self._data_dicts[idx].copy()

    def __len__(self):
        return len(self._data_dicts)

    def __eq__(self, other):
        if not isinstance(other, OpResult):
            return NotImplemented

        other_data = other._data_dicts
        if len(self._data_dicts) != len(other_data):
            return False

        if not self._is_columns_equal(other):
            return False

        this_elements = self._elements_to_tuple(self._data_dicts, self._column_names)
        other_elements = self._elements_to_tuple(other._data_dicts, self._column_names)

        return this_elements == other_elements

    def overlap(self, other):
        if not isinstance(other, OpResult):
            return NotImplemented

        if not self._is_columns_equal(other):
            return 0.0

        this_elements = self._elements_to_tuple(self._data_dicts, self._column_names)
        other_elements = self._elements_to_tuple(other._data_dicts, self._column_names)

        number_of_intersection = len(set(this_elements).intersection(other_elements))
        if len(this_elements) > 0:
            overlap = number_of_intersection/len(this_elements)
        else:
            if number_of_intersection == 0:
                overlap = 1.0
            else:
                overlap = 0.0

        return overlap

    @staticmethod
    def _elements_to_tuple(data_dicts, column_names):
        this_elements = defaultdict(lambda: 0)
        for element in data_dicts:
            row = []
            for col in column_names:
                row.append(element[col])
            this_elements[tuple(row)] += 1
        return this_elements

    def _is_columns_equal(self, other):
        for col in self._column_names:
            if col not in other.get_columns():
                return False
        return True

    def _validate_and_load_columns(self):
        """
        Validates `self._data_dicts` by checking that all `dict` objects
        contain the same column names. Raises a `ValueError` if this is
        not the case.
        """
        if self._data_dicts is None or len(self._data_dicts) == 0:
            return

        dict_keys_count = defaultdict(lambda: 0)

        for row in self._data_dicts:
            for key in row.keys():
                dict_keys_count[key] += 1

        key_counts = list(dict_keys_count.values())
        ref_val = key_counts[0]

        if any(map(lambda x: ref_val != x, key_counts)):
            raise ValueError('Not all dicts of `OpResult` have the same headers')

        self._column_names = list(dict_keys_count.keys())


class ExecutorFactory:

    def get_executor(self, type: str, root_node):
        type = type.lower()
        if type == 'semql':
            return SemQLExecutor(root_node)
        elif type == 'sql':
            return SQLExecutor(root_node)
        else:
            raise ValueError('Wrong type of executor.')


class Executor(ABC):

    @abstractmethod
    def __init__(self, root_node: 'Operation'):
        self.root_node = root_node

    @abstractmethod
    def run(self, save_intermediate_result_in_nodes: bool = False, preview_limit: int = 0):
        pass


class SemQLExecutor(Executor):
    """
    This class is responsible for executing a given operation tree in memory, using the SemQL operations
    directly and returning the resulting records.
    """
    def __init__(self, root_node: 'Operation'):
        super().__init__(root_node)
        self.result_for_node = {}

    def run(self, save_intermediate_result_in_nodes: bool = False, preview_limit: int = 0) -> 'OpResult':
        result = self._dfs_run(self.root_node)

        if not save_intermediate_result_in_nodes:
            # delete the intermediate results (so that it is equal to the sql execution)
            stack = [self.root_node]
            while stack:
                cur_node = stack.pop()
                cur_node.op_result = None
                stack.extend(cur_node.children)
        elif preview_limit > 0:
            # limit the intermediate results, so that it can i.e. be used as a preview
            stack = [self.root_node]
            while stack:
                cur_node = stack.pop()
                op_result_limited = OpResult(cur_node.op_result.get_data()[:preview_limit], cur_node.op_result.get_columns())
                cur_node.op_result = op_result_limited
                stack.extend(cur_node.children)

        return result

    def _dfs_run(self, node: 'Operation') -> 'OpResult':
        for child in node.children:
            self._dfs_run(child)

        node.run()
        return node.get_result()


class SQLExecutor(Executor):

    """
    This class is responsible for executing a given operation tree in SQL directly and returning the resulting records.
    """
    def __init__(self, root_node: 'Operation'):
        super().__init__(root_node)


    def run(self, save_intermediate_result_in_nodes: bool = False, preview_limit: int = 0):
        from semql.core.ast import IsEmpty  # TODO: break circular dependency on imports

        sql_statement = self.root_node.to_sql(save_intermediate_result_in_nodes, preview_limit)
        self.root_node._set_datasource()
        datasource = self.root_node.data_src
        try:
            result_dicts, columns = datasource.execute_sql(sql_statement)
        except Exception as e:
            if datasource is not None:
                datasource.conn.close()
            raise e
        op_result = OpResult(result_dicts, columns)
        datasource.conn.close()


        # Check if the root node is an IsEmpty-Node. If so, then the last operation has to be calculated in python,
        # because SQLite doesn't know boolean data types.
        if isinstance(self.root_node, IsEmpty):
            column_name = f'is_empty_{id(self.root_node)}'
            column_name = 'is_empty'
            is_empty_res = len(op_result.get_data()) == 0
            op_result = OpResult([{column_name: is_empty_res}], [column_name])
            if save_intermediate_result_in_nodes:
                self.root_node.op_result = op_result

        return op_result
