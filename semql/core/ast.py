import re
import inspect
import operator
import numbers

from abc import abstractmethod, ABC

from typing import List, Tuple
from enum import Enum, auto
from collections import defaultdict
from semql.execution import OpResult
from semql.data_sources import BaseDataSource, SqliteDataSource
from semql.sql_execution.sql_generation_helper import *

primitive_types = {str, int, float, bool, type(None)}


class Operation(ABC):
    op_result: OpResult = None
    parent: 'Operation' = None
    tokens: List = []
    children: List['Operation'] = []
    sql_statement: str = None
    data_src: BaseDataSource = None
    data_src_name: str = None
    score: int = 0

    def __init__(self):
        super().__init__()

    def _set_datasource(self, data_source=None):

        if not isinstance(self, NoOp):
            if data_source is None:
                leaves = self.return_leaves()
                get_data_node = leaves[0]
                if isinstance(get_data_node, GetData):
                    data_source = get_data_node.data_source
                else:
                    raise ValueError('The leaves are not get data nodes.')
            self.data_src_name = data_source
            self.data_src = BaseDataSource.get(data_source)

    def _get_data_source_name(self):
        leaves = self.return_leaves()
        get_data_node = leaves[0]
        if isinstance(get_data_node, GetData):
            data_source = get_data_node.data_source
        else:
            raise ValueError('The leaves are not get data nodes.')
        return data_source

    @staticmethod
    def get_op_classes() -> List[type]:
        def _inheritors(klass):
            subclasses = set()
            work = [klass]
            while work:
                parent = work.pop()
                for child in parent.__subclasses__():
                    if child not in subclasses:
                        subclasses.add(child)
                        work.append(child)
            return subclasses

        return _inheritors(Operation)

    @staticmethod
    def get_op_dict() -> Dict[str, type]:
        return {op.__name__: op for op in Operation.get_op_classes()}

    def print(self):
        raise NotImplementedError

    @abstractmethod
    def run(self) -> 'OpResult':
        pass

    def set_result(self, result: 'OpResult'):
        self.op_result = result

    def get_result(self) -> 'OpResult':
        return self.op_result

    @staticmethod
    def get_label(operation):
        return operation.__class__.__name__

    @staticmethod
    def get_children(operation):
        return operation.children

    def set_children(self, children: List['Operation']):
        self.children = children

    def to_json(self, node_id: str, store_results=False, preview_data=-1) -> List[Dict]:
        children_json = []
        for child_idx, child_node in enumerate(self.children):
            child_json = child_node.to_json(node_id + str(child_idx), store_results=store_results,
                                            preview_data=preview_data)
            children_json.extend(child_json)

        child_arg_names = [arg_name for arg_name, arg_type in
                           inspect.getfullargspec(type(self).__init__).annotations.items()
                           if isinstance(arg_type, type) and issubclass(arg_type, Operation)]

        children = {node_id + str(i): arg_name for i, arg_name in enumerate(child_arg_names)}

        node_arguments = [arg_name for arg_name, arg_type in
                          inspect.getfullargspec(type(self).__init__).annotations.items()
                          if isinstance(arg_type, type) and not issubclass(arg_type, Operation)]

        this_json = {
            'node_id': node_id,
            'operation': self.__class__.__name__,
            'children': children,
            'label': self.print_node(),
            'arguments': {arg_name: arg_value for arg_name, arg_value in self.__dict__.items() if
                          arg_name in node_arguments},
            'tokens': self.tokens,
            'score': self.score
        }
        if store_results:
            if preview_data < 0:
                this_json['results'] = self.op_result.get_data()
            else:
                preview_results = []
                result_length = len(self.op_result._data_dicts)
                for i in range(min([result_length, preview_data])):
                    preview_results.append(self.op_result.get(i))
                this_json['results'] = preview_results
                this_json['result_length'] = result_length

        return children_json + [this_json]

        pass

    @abstractmethod
    def print_node(self):
        pass

    @abstractmethod
    def print_noargs(self):
        pass

    def node_equality(self, other_node: 'Operation', partial_eq:bool = False, ignore_value=False) -> bool:
        #ignore value: if we have Filters with empty values, ignore them -> during generation
        if not isinstance(other_node, Operation):
            return False
        if not type(self) == type(other_node):
            return False

        if not partial_eq:
            if ignore_value:
                self_args = {var: val for var, val in self.__dict__.items() if type(val) in primitive_types and not var in ['parent', 'value']}
                other_args = {var: val for var, val in other_node.__dict__.items() if type(val) in primitive_types and not var in ['parent', 'value']}
            else:
                self_args = {var: val for var, val in self.__dict__.items() if type(val) in primitive_types}
                other_args = {var: val for var, val in other_node.__dict__.items() if type(val) in primitive_types and not var == 'parent'}

            return self_args == other_args
        else:
            #ignore None attributes in other_node
            if ignore_value:
                self_args = {var: val for var, val in self.__dict__.items() if type(val) in primitive_types and not var in ['parent', 'value']}
                other_args = {var: val for var, val in other_node.__dict__.items() if type(val) in primitive_types and val is not None and not var in ['parent', 'value']}
            else:
                self_args = {var: val for var, val in self.__dict__.items() if  type(val) in primitive_types and not var in ['parent']}
                other_args = {var: val for var, val in other_node.__dict__.items() if type(val) in primitive_types and val is not None and not var in ['parent']}

            args_equalities = [val == self_args[var] for var, val in other_args.items()]
            return not False in args_equalities


    def node_object_equality(self, other_node: 'Operation') -> bool:
        self_args = {var: val for var, val in self.__dict__.items() if type(val) in primitive_types}
        other_args = {var: val for var, val in other_node.__dict__.items() if type(val) in primitive_types}

        return self_args == other_args and type(self) == type(other_node) and id(self) == id(other_node)

    def deep_equality(self, other_node: 'Operation'):
        if not type(self) == type(other_node):
            return False
        self_args = {var: val for var, val in self.__dict__.items() if type(val) in primitive_types}
        other_args = {var: val for var, val in other_node.__dict__.items() if type(val) in primitive_types}
        if not self_args == other_args:
            return False
        for my_child, other_child in zip(self.children, other_node.children):
            if not my_child.deep_equality(other_child):
                return False
        return True

    def deep_structural_equality(self, other_node: 'Operation'):
        """
        Similar to deep equality, but if the other_node has None-arguments it still counts as equal.
        This servers to access if the struture of a partially rendered Tree is equal to the structure of the gold tree.
        :param other_node:
        :return:
        """
        if type(other_node) is NoOp:
            return True
        elif not type(self) == type(other_node):
            return False

        if not self.node_equality(other_node, partial_eq=True, ignore_value=True):
            return False

        for my_child, other_child in zip(self.children, other_node.children):
            if not my_child.deep_structural_equality(other_child):
                return False
        return True

    def shallow_copy(self):
        args = {var: val for var, val in self.__dict__.items() if type(val) in primitive_types}
        obj = type(self)()
        for k, v in args.items():
            obj.__dict__[k] = v
        obj.op_result = self.op_result
        obj.tokens = self.tokens
        return obj

    def deepcopy(self) -> 'Operation':
        instance = self.shallow_copy()
        new_children = []
        for child in self.children:
            new_child = child.deepcopy()
            new_child.parent = instance
            new_children.append(new_child)
        instance.children = new_children
        return instance

    def replace_op(self, old_op: 'Operation'):
        """
        Here this Operation replaces the old Operation. It sets the replaces the child of the parent and sets this Operations parent to the old Operations parent.
        :param old_op: Which Operation to replace in the tree.
        :return: None
        """
        parent_op = old_op.parent
        child_indices = [idx for idx, child in enumerate(parent_op.children) if old_op.node_equality(child)]
        child_idx = child_indices[0]
        self.parent = parent_op
        parent_op.children[child_idx] = self

    def copy_parent_from_op(self, operation: 'Operation'):
        """
        This function copies the parent of another Operation to serve as parent for this Operation.
        :param operation: other Operation to copy the parent from
        :return: None
        """
        new_root = operation.parent.shallow_copy()
        child_idx = [idx for idx, child in enumerate(operation.parent.children) if child.deep_equality(operation)][0]
        new_root.parent = None
        new_root.children[child_idx] = self
        self.parent = new_root

    def insert_node_on_path(self, cls: type, args: Dict) -> bool:
        """
        Inserts a new Operation as parent of this Operation.
        :param cls: Class Type of the new Operation
        :param op_attribute_name:  attribute_name in the constructor of the new Class to which this Operation is the argument
        :param args: other arguments of the class
        :return: True if the new Operation is a root
        """
        parent_node = self.parent
        op = cls(**args)
        if parent_node is not None:
            child_idx = [idx for idx, child in enumerate(parent_node.children) if child.node_object_equality(self)][0]
            op.parent = parent_node
            parent_node.children[child_idx] = op
            return False
        else:
            return True

    def return_leaves(self) -> List['Operation']:
        """
        Returns a list of all the leaf Operations in the Tree (either GetData or NoOps)
        :return: List of leaf Operations
        """
        if len(self.children) == 0:
            return [self]

        leaves = []
        for operation in self.children:
            child_leaves = operation.return_leaves()
            leaves.extend(child_leaves)
        return leaves

    def get_path_to_root(self, root: 'Operation' = None) -> List['Operation']:
        path = []
        node = self
        while node.parent is not None and not node is root:
            path.append(node)
            node = node.parent
        path.append(node)
        return path

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        pass

    def list_of_nodes(self) -> List['Operation']:
        tree_list = [self]
        for child in self.children:
            child_nodes = child.list_of_nodes()
            tree_list.extend(child_nodes)
        return tree_list

    def execute_sql_and_set_result(self, preview_limit: int):
        if self.sql_statement:
            self._set_datasource()
            statement_wrapped = wrap_statement_in_select(self.sql_statement, preview_limit)
            try:
                result_dicts, columns = self.data_src.execute_sql(statement_wrapped)
            except Exception as e:
                self.data_src.conn.close()
                raise e
            self.set_result(OpResult(result_dicts, columns))
            self.data_src.conn.close()

    def __eq__(self, other):
        return self.node_equality(other)

    def __str__(self):
        return self.print_node()

    def __repr__(self):
        return self.print_node()

    def __hash__(self):
        return hash(self.print_node())

    def _set_and_return_result(self, rows: List[Dict], columns: List[str]) -> OpResult:
        new_op_res = OpResult(rows, columns)
        self.set_result(new_op_res)
        return new_op_res

    def _is_numeric(self, attr_name: str, row: Dict) -> bool:
        return isinstance(row[attr_name], numbers.Number)


class TableOperation(Operation):
    pass


class BoolOperation(Operation):
    pass


class NumericOperation(Operation):
    pass


class ListOperation(Operation):
    pass


class FinalOperation(Operation):
    pass


class NoOp(Operation):
    def __init__(self):
        super().__init__()
        self.children = []

    def run(self) -> 'OpResult':
        # TODO: Implement!
        pass

    def print(self):
        return "NoOp"

    def print_node(self):
        return 'NoOp'

    def print_noargs(self):
        return 'NoOp()'

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource()
        return ''


class SetOperations(TableOperation):
    def __init__(self, table0: TableOperation = None, table1: TableOperation = None, attribute_name0: str = None,
                 attribute_name1: str = None):
        super().__init__()
        if table0 is None:
            table0 = NoOp()
        if table1 is None:
            table1 = NoOp()
        self.attribute_name0 = attribute_name0
        self.attribute_name1 = attribute_name1
        self.setop = None
        table0.parent = self
        table1.parent = self
        self.children = [table0, table1]

    def print(self):
        return "{}({},{},{},{})".format(self.setop, self.children[0].print(), self.children[1].print(),
                                        self.attribute_name0,
                                        self.attribute_name1)

    def print_node(self):
        return '{}({},{})'.format(self.get_label(self), self.attribute_name0, self.attribute_name1)

    def print_noargs(self):
        return "{}({},{})".format(type(self).__name__, self.children[0].print_noargs(), self.children[1].print_noargs())


class Done(FinalOperation):
    def __init__(self, result: ListOperation = None):
        super().__init__()
        if result is None:
            result = NoOp()
        result.parent = self
        self.children = [result]

    def run(self) -> 'OpResult':
        return self._set_and_return_result(
            self.children[0].get_result().get_data(), self.children[0].get_result().get_columns()
        )

    def print(self):
        return "done({})".format(self.children[0].print())

    def print_node(self):
        return '{}'.format(self.get_label(self))

    def print_noargs(self):
        return "Done({})".format(self.children[0].print_noargs())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource()
        self.sql_statement = self.children[0].to_sql(save_intermediate_result, preview_limit).strip()
        if self.sql_statement.startswith('('):
            self.sql_statement = self.sql_statement[1:-1]
        if save_intermediate_result:
            self.op_result = self.children[0].op_result
        return self.sql_statement


class ProjectionRoot(FinalOperation):

    class ProjectionFN:
        NONE = auto()
        SUM = auto()
        AVG = auto()
        MIN = auto()
        MAX = auto()
        COUNT = auto()

    def __init__(
            self,
            table: TableOperation,
            attrs: List[Tuple[str, 'ProjectionFN']],
            distinct: bool = False,
    ):
        super().__init__()
        if table is None:
            table = NoOp()
        table.parent = self
        self.children = [table]
        self.attrs = attrs
        self.distinct = distinct

    def run(self) -> 'OpResult':
        # TODO
        raise NotImplementedError

    @staticmethod
    def attr2txt(attr_str: str, fn: ProjectionFN):
        if fn == ProjectionRoot.ProjectionFN.NONE:
            return attr_str
        elif fn == ProjectionRoot.ProjectionFN.SUM:
            return f"SUM({attr_str})"
        elif fn == ProjectionRoot.ProjectionFN.AVG:
            return f"AVG({attr_str})"
        elif fn == ProjectionRoot.ProjectionFN.MIN:
            return f"MIN({attr_str})"
        elif fn == ProjectionRoot.ProjectionFN.MAX:
            return f"MAX({attr_str})"
        elif fn == ProjectionRoot.ProjectionFN.COUNT:
            return f"COUNT({attr_str})"

    def prettyattrs(self):
        return ", ".join([
            self.attr2txt(attr_name, fn)
            for attr_name, fn in self.attrs
        ])

    def print(self):
        sub = self.children[0].print()
        attrs = self.prettyattrs()
        distinct = f"distinct = {self.distinct}"
        return f"ProjectionRoot({sub}, {attrs}, {distinct})"

    def print_node(self):
        attrs = self.prettyattrs()
        distinct = f"distinct = {self.distinct}"
        return f"ProjectionRoot({attrs}, {distinct})"

    def print_noargs(self):
        sub = self.children[0].print_noargs()
        return f"ProjectionRoot({sub})"

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource()
        attr_list = [
            self.attr2txt(
                get_attribute_alias(*attr_name.split(".")),
                fn,
            )
            for attr_name, fn in self.attrs
        ]
        distinct_sql = "DISTINCT " if self.distinct else ""
        self.sql_statement = ' SELECT ' + distinct_sql + ", ".join(attr_list) + ' FROM (' \
                             + self.children[0].to_sql(save_intermediate_result, preview_limit) + ') '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class IsEmpty(FinalOperation):
    def __init__(self, result: ListOperation = None):
        super().__init__()
        if result is None:
            result = NoOp()
        result.parent = self
        self.children = [result]

    def print(self):
        return "isEmpty({})".format(self.children[0].print())

    def print_noargs(self):
        return "IsEmpty({})".format(self.children[0].print_noargs())

    def print_node(self):
        return 'Boolean'

    def run(self) -> 'OpResult':
        # column_name = f'is_empty_{id(self)}'
        column_name = 'is_empty'
        is_empty_res = len(self.children[0].get_result()) == 0
        return self._set_and_return_result([{column_name: is_empty_res}], [column_name])

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource()
        self.sql_statement = self.children[0].to_sql(save_intermediate_result, preview_limit)
        if save_intermediate_result:
            self.op_result = self.children[0].op_result
        return self.sql_statement


class Distinct(TableOperation):
    # TODO mayba change to Distince(table, attr_name)
    def __init__(self, result: TableOperation = None, attribute_name: str = None, ignore_primary_key=False):
        super().__init__()
        if result is None:
            result = NoOp()
        result.parent = self
        self.attribute_name = attribute_name
        self.ignore_primary_key = ignore_primary_key
        self.children = [result]

    def print(self):
        return "distinct({}, {})".format(self.children[0].print(), self.attribute_name)

    def print_noargs(self):
        return "Distinct({}, {})".format(self.children[0].print_noargs(), self.attribute_name)

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.attribute_name)

    def run(self) -> 'OpResult':
        # super()._set_datasource()
        kept_rows = []
        child_rows = self.children[0].get_result().get_data()
        distinct_indices = []
        distinct_values = set()
        data_src_name = self._get_data_source_name()
        is_primary_key = is_attribute_primary_key(*self.attribute_name.split("."), data_src_name)
        if is_primary_key:
            # keep attributes of entity
            table_name = self.attribute_name.split(".")[0]
            attr2alias = get_attributes_with_aliases_for_table(table_name, data_src_name)
            attribute_names = ['{}.{}'.format(table_name, attr) for attr in list(attr2alias.keys())]
        else:
            attribute_names = [self.attribute_name]

        if len(child_rows) > 0:
            for index, row in enumerate(child_rows):
                if self.attribute_name is None:
                    if frozenset(row) not in distinct_values:
                        distinct_values.add(frozenset(row))
                        distinct_indices.append(index)
                else:
                    if row[self.attribute_name] not in distinct_values:
                        distinct_indices.append(index)
                        distinct_values.add(row[self.attribute_name])

            for index in distinct_indices:
                row = child_rows[index]
                projected_row = {}
                for k, v in row.items():
                    if k in attribute_names:
                        projected_row[k] = v
                kept_rows.append(projected_row)

        return self._set_and_return_result(kept_rows, attribute_names)

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        ##super()._set_datasource()
        data_src_name = self._get_data_source_name()
        if is_attribute_primary_key(*self.attribute_name.split("."), data_src_name) and not self.ignore_primary_key:
            self.sql_statement = ' SELECT DISTINCT ' + get_aliases_for_table(self.attribute_name.split(".")[0],
                                                                             data_src_name) + ' FROM ( ' \
                                 + self.children[0].to_sql(save_intermediate_result, preview_limit) + ' )'
        else:
            self.sql_statement = ' SELECT DISTINCT ' + get_attribute_alias(*self.attribute_name.split(".")) + ' FROM ( ' \
                                 + self.children[0].to_sql(save_intermediate_result, preview_limit) + ' )'
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class ExtractValues(ListOperation):
    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        if table is None:
            table = NoOp()
        super().__init__()
        self.attribute_name = attribute_name
        table.parent = self
        self.children = [table]

    def run(self) -> 'OpResult':
        extracted_val = []
        extracted_col = []

        for row in self.children[0].get_result().get_data():
            if self.attribute_name not in row:
                raise ValueError(f'Missing attribute {self.attribute_name}')
            else:
                extracted_val.append(row[self.attribute_name])

        op_res_dicts = [{self.attribute_name: x} for x in extracted_val]
        return self._set_and_return_result(op_res_dicts, extracted_col)

    def print(self):
        return "extractValues({},{})".format(self.children[0].print(), self.attribute_name)

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.attribute_name)

    def print_noargs(self):
        return "ExtractValues({})".format(self.children[0].print_noargs())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource()
        self.sql_statement = ' SELECT ' + get_attribute_alias(*self.attribute_name.split(".")) + ' FROM (' \
                             + self.children[0].to_sql(save_intermediate_result, preview_limit) + ') '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Aggregation(FinalOperation):
    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super().__init__()
        if table is None:
            table = NoOp()
        self.attribute_name = attribute_name
        table.parent = self
        self.children = [table]

    def print(self):
        raise NotImplementedError

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.attribute_name)


class Sum(Aggregation):
    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super(Sum, self).__init__(table, attribute_name)

    def print(self):
        return "sum({},{})".format(self.children[0].print(), self.attribute_name)

    def print_noargs(self):
        return "Sum({})".format(self.children[0].print_noargs())

    def run(self) -> 'OpResult':
        summed_value = 0

        child_data = self.children[0].get_result().get_data()
        if len(child_data) == 0:
            return self._set_and_return_result([{self.attribute_name: None}], columns=[])

        for row in child_data:
            if self.attribute_name not in row:
                raise ValueError(f'Missing attribute {self.attribute_name}')

            if row[self.attribute_name] is None:
                continue
            elif not self._is_numeric(self.attribute_name, row):
                raise ValueError(f'Attribute {self.attribute_name} is not numeric')
            else:
                summed_value += row[self.attribute_name]

        return self._set_and_return_result([{self.attribute_name: summed_value}], columns=[])

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        self.sql_statement = ' SELECT SUM(' + get_attribute_alias(*self.attribute_name.split(".")) + ') FROM ( ' + \
                             self.children[0].to_sql(save_intermediate_result, preview_limit) + ') '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Average(Aggregation):
    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super(Average, self).__init__(table, attribute_name)

    def print(self):
        return "avg({},{})".format(self.children[0].print(), self.attribute_name)

    def print_noargs(self):
        return "Average({})".format(self.children[0].print_noargs())

    def run(self) -> 'OpResult':
        summed_value = 0
        child_data = self.children[0].get_result().get_data()

        for row in self.children[0].get_result().get_data():
            if self.attribute_name not in row:
                raise ValueError(f'Missing attribute {self.attribute_name}')

            if row[self.attribute_name] is None:
                continue
            elif not self._is_numeric(self.attribute_name, row):
                raise ValueError(f'Attribute {self.attribute_name} is not numeric')
            else:
                summed_value += row[self.attribute_name]

        if len(child_data) == 0:
            return self._set_and_return_result([
                {self.attribute_name: 'nan'}
            ], columns=[])
        else:
            return self._set_and_return_result([
                {self.attribute_name: summed_value / len(child_data)}
            ], columns=[])

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        self.sql_statement = ' SELECT AVG(' + get_attribute_alias(*self.attribute_name.split(".")) + ') FROM ( ' + \
                             self.children[0].to_sql(save_intermediate_result, preview_limit) + ') '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class MaxAggregation(Aggregation):

    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super(MaxAggregation, self).__init__(table, attribute_name)

    def print(self):
        return f"MaxAgg({self.children[0].print()}, {self.attribute_name})"

    def print_noargs(self):
        return f"MaxAgg({self.children[0].print_noargs()})"

    def row2val(self, row):
        if self.attribute_name not in row:
            raise ValueError(f"Missing attribute {self.attribute_name}")
        if row[self.attribute_name] is None:
            return None
        elif not self._is_numeric(self.attribute_name, row):
            raise ValueError(f"Attribute {self.attribute_name} is not numeric")
        else:
            return row[self.attribute_name]

    def run(self) -> 'OpResult':
        child_data = self.children[0].get_result().get_data()
        result = max(
            filter(
                lambda val: val is not None,
                map(lambda r: self.row2val(r), child_data)
            ),
            default=None,
        )
        if result is None:
            return self._set_and_return_result(
                [{self.attribute_name: 'nan'}],
                columns=[],
            )
        else:
            return self._set_and_return_result(
                [{self.attribute_name: result}],
                columns=[],
            )

    def to_sql(
            self,
            save_intermediate_result: bool = False,
            preview_limit: int = 0,
    ) -> str:
        self.sql_statement = f" SELECT" \
                             f" MAX({get_attribute_alias(*self.attribute_name.split('.'))})" \
                             f" FROM ({self.children[0].to_sql(save_intermediate_result, preview_limit)}) "
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class MinAggregation(Aggregation):

    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super(MinAggregation, self).__init__(table, attribute_name)

    def print(self):
        return f"MinAgg({self.children[0].print()}, {self.attribute_name})"

    def print_noargs(self):
        return f"MinAgg({self.children[0].print_noargs()})"

    def row2val(self, row):
        if self.attribute_name not in row:
            raise ValueError(f"Missing attribute {self.attribute_name}")
        if row[self.attribute_name] is None:
            return None
        elif not self._is_numeric(self.attribute_name, row):
            raise ValueError(f"Attribute {self.attribute_name} is not numeric")
        else:
            return row[self.attribute_name]

    def run(self) -> 'OpResult':
        child_data = self.children[0].get_result().get_data()
        result = min(
            filter(
                lambda val: val is not None,
                map(lambda r: self.row2val(r), child_data)
            ),
            default=None,
        )
        if result is None:
            return self._set_and_return_result(
                [{self.attribute_name: 'nan'}],
                columns=[],
            )
        else:
            return self._set_and_return_result(
                [{self.attribute_name: result}],
                columns=[],
            )

    def to_sql(
            self,
            save_intermediate_result: bool = False,
            preview_limit: int = 0,
    ) -> str:
        self.sql_statement = f" SELECT" \
                             f" MIN({get_attribute_alias(*self.attribute_name.split('.'))})" \
                             f" FROM ({self.children[0].to_sql(save_intermediate_result, preview_limit)}) "
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Count(FinalOperation):
    def __init__(self, table: TableOperation = None):
        super().__init__()
        if table is None:
            table = NoOp()
        table.parent = self
        self.children = [table]

    def print(self):
        return "count({})".format(self.children[0].print())

    def print_node(self):
        return '{}'.format(self.get_label(self))

    def print_noargs(self):
        return "Count({})".format(self.children[0].print_noargs())

    def run(self) -> 'OpResult':
        child_data = self.children[0].get_result().get_data()
        return self._set_and_return_result([{'count': len(child_data)}], columns=[])

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        self.sql_statement = ' SELECT COUNT(*) FROM ( ' + self.children[0].to_sql(save_intermediate_result,
                                                                                  preview_limit) + ') '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Max(TableOperation):
    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super().__init__()
        if table is None:
            table = NoOp()
        self.attribute_name = attribute_name
        table.parent = self
        self.children = [table]

    def print(self):
        return "max({},{})".format(self.children[0].print(), self.attribute_name)

    def run(self) -> 'OpResult':
        input_data = self.children[0].get_result().get_data()
        max_value = float('-inf')
        ret_rows = []

        for row in input_data:
            if not self.attribute_name in row:
                raise ValueError('Attribute {} is missing'.format(self.attribute_name))

            if row[self.attribute_name] is None:
                continue
            elif not self._is_numeric(self.attribute_name, row):
                raise ValueError('Attribute {} is not numeric'.format(self.attribute_name))
            elif row[self.attribute_name] == max_value:
                ret_rows.append(row)
            elif row[self.attribute_name] > max_value:
                max_value = row[self.attribute_name]
                ret_rows = [row]

        if len(ret_rows) == 0:
            return self._set_and_return_result([], columns=[])

        return self._set_and_return_result(ret_rows, columns=[])

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.attribute_name)

    def print_noargs(self):
        return "Max({})".format(self.children[0].print_noargs())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        # the case distinction in this method is needed for a filter on a countby attribute
        sql_statement = self.children[0].to_sql(save_intermediate_result, preview_limit) + ' ) WHERE '
        if len(self.attribute_name.split(".")) == 1:
            max_attribute = self.attribute_name.split(".")[0]
        else:
            max_attribute = get_attribute_alias(*self.attribute_name.split("."))
        self.sql_statement = sql_statement + max_attribute + ' = ( SELECT MAX (' + max_attribute + ' ) ' + \
                             ' FROM ( ' + self.children[0].to_sql(save_intermediate_result, preview_limit) + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Min(TableOperation):
    def __init__(self, table: TableOperation = None, attribute_name: str = None):
        super().__init__()
        if table is None:
            table = NoOp()
        self.attribute_name = attribute_name
        table.parent = self
        self.children = [table]

    def run(self) -> 'OpResult':
        input_data = self.children[0].get_result().get_data()
        min_value = float('inf')

        ret_rows = []

        for row in input_data:
            if not self.attribute_name in row:
                raise ValueError('Attribute {} is missing'.format(self.attribute_name))

            if row[self.attribute_name] is None:
                continue
            elif not self._is_numeric(self.attribute_name, row):
                raise ValueError('Attribute {} is not numeric'.format(self.attribute_name))
            elif row[self.attribute_name] == min_value:
                ret_rows.append(row)
            elif row[self.attribute_name] < min_value:
                min_value = row[self.attribute_name]
                ret_rows = [row]

        if len(ret_rows) == 0:
            return self._set_and_return_result([], columns=[])

        return self._set_and_return_result(ret_rows, columns=[])

    def print(self):
        return "min({},{})".format(self.children[0].print(), self.attribute_name)

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.attribute_name)

    def print_noargs(self):
        return "Min({})".format(self.children[0].print_noargs())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        # the case distinction in this method is needed for a filter on a countby attribute
        sql_statement = self.children[0].to_sql(save_intermediate_result, preview_limit) + ' ) WHERE '
        if len(self.attribute_name.split(".")) == 1:
            min_attribute = self.attribute_name.split(".")[0]
        else:
            min_attribute = get_attribute_alias(*self.attribute_name.split("."))
        self.sql_statement = sql_statement + min_attribute + ' = ( SELECT MIN (' + min_attribute + ' ) ' + \
                             ' FROM ( ' + self.children[0].to_sql(save_intermediate_result, preview_limit) + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Union(SetOperations):
    def __init__(self, table0: TableOperation = None, table1: TableOperation = None, attribute_name0: str = None,
                 attribute_name1: str = None):
        super(Union, self).__init__(table0, table1, attribute_name0, attribute_name1)
        self.setop = 'union'

    def run(self) -> 'OpResult':
        children_results = [c.get_result() for c in self.children]
        new_result = []

        for res in children_results:
            new_result = new_result + res.get_data()

        return self._set_and_return_result(new_result, self.children[0].get_result().get_columns())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        left_child, right_child = self.children
        self.sql_statement = ' SELECT * FROM ( SELECT * FROM (' + left_child.to_sql(save_intermediate_result,
                                                                                    preview_limit) + ') UNION SELECT * FROM ' \
                                                                                                     '(' + right_child.to_sql(
            save_intermediate_result, preview_limit) + ' )) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Intersection(SetOperations):
    def __init__(self, table0: TableOperation = None, table1: TableOperation = None, attribute_name0: str = None,
                 attribute_name1: str = None):
        super(Intersection, self).__init__(table0, table1, attribute_name0, attribute_name1)
        self.setop = 'intersect'

    def run(self) -> 'OpResult':
        children_results = [c.get_result() for c in self.children]
        table0_results = children_results[0]
        table1_results = children_results[1]

        children_header = [c.get_result().get_columns() for c in self.children]
        children_header = set(map(lambda x: ''.join(x), children_header))

        if len(children_header) != 1:
            raise ValueError('Not all children contain the same columns')

        table0_results_data = table0_results.get_data()
        table1_results_data = table1_results.get_data()

        table0_attribute_values = set([row[self.attribute_name0] for row in table0_results_data])
        table1_attribute_values = set([row[self.attribute_name1] for row in table1_results_data])

        intersected_attribtue_values = table0_attribute_values.intersection(table1_attribute_values)

        intersected_results = [row for row in table0_results_data if
                               row[self.attribute_name0] in intersected_attribtue_values]

        return self._set_and_return_result(intersected_results, self.children[0].get_result().get_columns())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        data_src_name = self._get_data_source_name()
        left_child, right_child = self.children
        table, attribute = self.attribute_name0.split(".")
        self.sql_statement = ' SELECT * FROM ( SELECT ' + get_aliases_for_table(table,
                                                                                data_src_name) + ' FROM ' + left_child.to_sql(
            save_intermediate_result, preview_limit) + \
                             ' INTERSECT SELECT ' + get_aliases_for_table(table,
                                                                          data_src_name) + ' FROM ' + right_child.to_sql(
            save_intermediate_result, preview_limit) + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Difference(SetOperations):
    def __init__(self, table0: TableOperation = None, table1: TableOperation = None, attribute_name0: str = None,
                 attribute_name1: str = None):
        super(Difference, self).__init__(table0, table1, attribute_name0, attribute_name1)
        self.setop = 'difference'

    def run(self) -> 'OpResult':
        children_results = [c.get_result() for c in self.children]
        difference_results = children_results[0].get_data()
        children_results = children_results[1:]

        children_header = [c.get_result().get_columns() for c in self.children]
        children_header = set(map(lambda x: ''.join(x), children_header))

        if len(children_header) != 1:
            raise ValueError('Not all children contain the same columns')

        for child_results in children_results:
            for res in child_results.get_data():
                if res in difference_results:
                    difference_results.remove(res)

        return self._set_and_return_result(difference_results, self.children[0].get_result().get_columns())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        data_src_name = self._get_data_source_name()
        left_child, right_child = self.children
        table, attribute = self.attribute_name0.split(".")
        self.sql_statement = ' SELECT * FROM ( SELECT ' + get_aliases_for_table(table,
                                                                                data_src_name) + ' FROM ' + left_child.to_sql(
            save_intermediate_result, preview_limit) + \
                             ' EXCEPT SELECT ' + get_aliases_for_table(table,
                                                                       data_src_name) + ' FROM ' + right_child.to_sql(
            save_intermediate_result, preview_limit) + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class GroupBy(TableOperation):
    def __init__(self, table: TableOperation = None, group_by_attribute_name: str = None,
                 aggregate_by_attribute_name: str = None):
        super().__init__()
        if table is None:
            table = NoOp()
        self.group_by_attribute_name = group_by_attribute_name
        self.aggregate_by_attribute_name = aggregate_by_attribute_name
        table.parent = self
        self.children = [table]

    def print_node(self):
        return '{}'.format(
            self.get_label(self),
            self.group_by_attribute_name,
            self.aggregate_by_attribute_name
        )


class AverageBy(GroupBy):
    def run(self) -> 'OpResult':
        child_result = self.children[0].get_result()
        child_data = child_result.get_data()

        group_attr = self.group_by_attribute_name
        aggr_attr = self.aggregate_by_attribute_name

        if group_attr not in child_result.get_columns():
            raise ValueError(f'Missing attribute {group_attr}')

        if aggr_attr not in child_result.get_columns():
            raise ValueError(f'Missing attribute {aggr_attr}')

        group_by_values = list(set([x[group_attr] for x in child_data]))
        group_by_dict = defaultdict(lambda: [])

        for row in child_data:
            if row[aggr_attr] is None:
                continue

            if not self._is_numeric(aggr_attr, row):
                raise ValueError(f'Attribute {aggr_attr} is not numeric')

            group_by_dict[row[group_attr]].append(row[aggr_attr])

        for key in group_by_dict.keys():
            value = sum(group_by_dict[key]) / len(group_by_dict[key])
            group_by_dict[key] = value

        res_dicts = []

        for k, v in group_by_dict.items():
            res_dicts.append({group_attr: k, aggr_attr: v})

        res_columns = [group_attr, aggr_attr]

        return self._set_and_return_result(res_dicts, res_columns)

    def print(self):
        return "avgBy({},{},{})".format(self.children[0].print(), self.group_by_attribute_name,
                                        self.aggregate_by_attribute_name)

    def print_noargs(self):
        return "AvgBy({})".format(self.children[0].print_noargs())

    def print_node(self):
        return '{}({}, {})'.format(self.get_label(self), self.group_by_attribute_name, self.aggregate_by_attribute_name)

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        group_by_alias = get_attribute_alias(*self.group_by_attribute_name.split("."))
        aggregate_by_alias = get_attribute_alias(*self.aggregate_by_attribute_name.split("."))
        self.sql_statement = ' SELECT * FROM ( SELECT AVG(' + aggregate_by_alias + ') ' + aggregate_by_alias + ', ' + \
                             group_by_alias + ' FROM (' + self.children[0].to_sql(save_intermediate_result,
                                                                                  preview_limit) + ' ) ' + ' GROUP BY ' + group_by_alias + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class SumBy(GroupBy):
    def run(self) -> 'OpResult':
        child_result = self.children[0].get_result()
        child_data = child_result.get_data()
        if len(child_data) == 0:
            return self._set_and_return_result([], columns=[])

        group_attr = self.group_by_attribute_name
        aggr_attr = self.aggregate_by_attribute_name

        if group_attr not in child_result.get_columns():
            raise ValueError(f'Missing attribute {group_attr}')

        if aggr_attr not in child_result.get_columns():
            raise ValueError(f'Missing attribute {aggr_attr}')

        group_by_values = list(set([x[group_attr] for x in child_data]))
        group_by_dict = defaultdict(lambda: 0)

        for row in child_data:
            if row[aggr_attr] is None:
                continue

            if not self._is_numeric(aggr_attr, row):
                raise ValueError(f'Attribute {aggr_attr} is not numeric')

            group_by_dict[row[group_attr]] += row[aggr_attr]

        res_dicts = []
        res_columns = [group_attr, aggr_attr]

        for k, v in group_by_dict.items():
            res_dicts.append({group_attr: k, aggr_attr: v})

        return self._set_and_return_result(res_dicts, res_columns)

    def print(self):
        return "sumBy({},{},{})".format(self.children[0].print(), self.group_by_attribute_name,
                                        self.aggregate_by_attribute_name)

    def print_noargs(self):
        return "SumBy({})".format(self.children[0].print_noargs())

    def print_node(self):
        return '{}({}, {})'.format(self.get_label(self), self.group_by_attribute_name, self.aggregate_by_attribute_name)

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        group_by_alias = get_attribute_alias(*self.group_by_attribute_name.split("."))
        aggregate_by_alias = get_attribute_alias(*self.aggregate_by_attribute_name.split("."))
        self.sql_statement = ' SELECT * FROM ( SELECT SUM(' + aggregate_by_alias + ') ' + aggregate_by_alias + ', ' + \
                             group_by_alias + ' FROM (' + self.children[0].to_sql(save_intermediate_result,
                                                                                  preview_limit) + ' ) ' + ' GROUP BY ' + group_by_alias + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class CountBy(GroupBy):
    def run(self) -> 'OpResult':
        child_result = self.children[0].get_result()
        child_data = child_result.get_data()
        if len(child_data) == 0:
            return self._set_and_return_result([], columns=[])

        group_attr = self.group_by_attribute_name
        aggr_attr = 'count'
        if group_attr not in child_result.get_columns():
            raise ValueError(f'Missing attribute {group_attr}')

        group_by_values = list(set([x[group_attr] for x in child_data]))
        group_by_dict = defaultdict(lambda: 0)

        for row in child_data:
            group_by_dict[row[group_attr]] += 1

        res_dicts = []
        res_columns = [group_attr, aggr_attr]

        for k, v in group_by_dict.items():
            res_dicts.append({group_attr: k, 'count': v})

        return self._set_and_return_result(res_dicts, res_columns)

    def print(self):
        return "countBy({},{})".format(self.children[0].print(), self.group_by_attribute_name)

    def print_noargs(self):
        return "CountBy({})".format(self.children[0].print_noargs())

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.group_by_attribute_name)

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        group_by_alias = get_attribute_alias(*self.group_by_attribute_name.split("."))
        aggregate_by_alias = get_attribute_alias(*self.aggregate_by_attribute_name.split("."))
        self.sql_statement = ' SELECT * FROM ( SELECT COUNT(*) AS cnt, ' + \
               group_by_alias + ' FROM (' + self.children[0].to_sql(save_intermediate_result, preview_limit) + ' ) ' + ' GROUP BY ' + group_by_alias + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Merge(TableOperation):
    def __init__(self, table0: TableOperation = None, table1: TableOperation = None, attribute_name0: str = None,
                 attribute_name1: str = None):
        super().__init__()
        if table0 is None:
            table0 = NoOp()
        if table1 is None:
            table1 = NoOp()
        if type(table1) == str:
            print('Hello')
        self.attribute_name0 = attribute_name0
        self.attribute_name1 = attribute_name1
        table0.parent = self
        table1.parent = self
        self.children = [table0, table1]

    def run(self) -> 'OpResult':
        left_child_res = self.children[0].get_result()
        right_child_res = self.children[1].get_result()

        l_attr = self.attribute_name0
        r_attr = self.attribute_name1

        merged_data = []

        merged_columns = left_child_res.get_columns() + right_child_res.get_columns()

        attr_to_row_dict = defaultdict(list)

        for l_row in left_child_res.get_data():
            if l_attr not in l_row:
                raise ValueError(f'Missing attribute {l_attr}')

            attr_to_row_dict[l_row[l_attr]].append(l_row)

        for r_row in right_child_res.get_data():
            if r_attr not in r_row:
                raise ValueError(f'Missing attribute {r_attr}')

            rows_with_attr = attr_to_row_dict[r_row[r_attr]]

            if len(rows_with_attr) > 0:
                merged_data += list(map(lambda l: {**l, **r_row}, rows_with_attr))

        return self._set_and_return_result(merged_data, merged_columns)

    def print(self):
        return "merge({},{},{},{})".format(self.children[0].print(), self.children[1].print(), self.attribute_name0,
                                           self.attribute_name1)

    def print_noargs(self):
        return "Merge({}, {})".format(self.children[0].print_noargs(), self.children[1].print_noargs())

    def print_node(self):
        return '{}({}, {})'.format(self.get_label(self), self.attribute_name0, self.attribute_name1)

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        left_child, right_child = self.children
        self.sql_statement = left_child.to_sql(save_intermediate_result, preview_limit) + ' JOIN ( ' + right_child.to_sql(save_intermediate_result, preview_limit) + ' ) ON ' + \
               get_attribute_alias(*self.attribute_name0.split(".")) + ' = ' + \
               get_attribute_alias(*self.attribute_name1.split("."))
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class Filter(TableOperation):
    OPERATOR_MAP = {
        '=': operator.eq,
        '==': operator.eq,
        '<': operator.lt,
        '>': operator.gt,
        '>=': operator.ge,
        '<=': operator.le,
        '!=': lambda x, y: not operator.eq(x, y)
    }

    def __init__(self, table: TableOperation = None, attribute_name: str = None, operation: str = None,
                 value: str = None):
        super().__init__()
        if table is None:
            table = NoOp()
        self.attribute_name = attribute_name
        self.operation = operation
        self.value = value
        table.parent = self
        self.children = [table]

    def run(self) -> 'OpResult':
        def _filter_data(row: Dict) -> bool:
            """Returns true if the `row` should be kept
               with respect to the filter and false otherwise."""
            attr_name = self.attribute_name
            filter_fn = self.operation

            if attr_name not in row:
                raise ValueError(f'Column {attr_name} is missing')
            elif filter_fn not in self.OPERATOR_MAP:
                raise ValueError(f'Unknown comparison operator {filter_fn}')
            else:
                # there are cases where the attributes are none ->  handle those
                if row[attr_name] is None:
                    return False
                return self.OPERATOR_MAP[filter_fn](row[attr_name], self.value)

        child_result = self.children[0].get_result()
        child_data = child_result.get_data()
        child_header = child_result.get_columns()
        result_data = list(filter(_filter_data, child_data))
        return self._set_and_return_result(result_data, child_header)

    def print(self):
        return "filter({},{},{},{})".format(self.children[0].print(), self.attribute_name, self.operation, self.value)

    def print_node(self):
        return '{}({}, {}, {})'.format(self.get_label(self), self.attribute_name, self.operation, self.value)

    def print_noargs(self):
        return "Filter({})".format(self.children[0].print_noargs())

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        # super()._set_datasource(
        # the case distinction in this method is needed for a filter on a countby attribute
        self.sql_statement = ' ( SELECT * FROM ( ' + self.children[0].to_sql(save_intermediate_result,
                                                                             preview_limit) + ' ) WHERE '
        if len(self.attribute_name.split(".")) == 1:
            if self.attribute_name == 'count':
                self.sql_statement += 'cnt'
            else:
                self.sql_statement += self.attribute_name.split(".")[0]
        else:
            self.sql_statement += get_attribute_alias(*self.attribute_name.split("."))
        self.sql_statement += ' ' + self.operation + ' ' + get_value_for_statement(self.value) + ' ) '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement


class GetData(TableOperation):
    def __init__(self, data_source: str = '', table_name: str = None):
        super().__init__()
        self.data_source = data_source
        self.table_name = table_name
        self.children = []

    def run(self) -> 'OpResult':
        super()._set_datasource(self.data_source)
        attr2alias = get_attributes_with_aliases_for_table(self.table_name, self.data_source)
        result = self._set_and_return_result(
            *self.data_src.get_data_and_project(self.table_name, list(attr2alias.keys())))
        self.data_src.conn.close()
        return result

    def print(self):
        return "getData({})".format(self.table_name)

    def print_node(self):
        return '{}({})'.format(self.get_label(self), self.table_name)

    def print_noargs(self):
        return 'GetData()'

    def to_sql(self, save_intermediate_result: bool = False, preview_limit: int = 0) -> str:
        attr2alias = get_attributes_with_aliases_for_table(self.table_name, self.data_source)
        projection = ''
        attr2alias_items = list(attr2alias.items())
        for attr, alias in attr2alias_items[:-1]:
            projection += escape_phrase(attr) + ' AS ' + get_value_for_statement(alias) + ', '
        last_attr, last_alias = attr2alias_items[-1]
        projection += escape_phrase(last_attr) + ' AS ' + get_value_for_statement(last_alias)
        self.sql_statement = ' (SELECT ' + projection + ' FROM ' + escape_phrase(self.table_name) + ') '
        if save_intermediate_result:
            self.execute_sql_and_set_result(preview_limit)
        return self.sql_statement
