import json
from semql.tree_sampling.table_schema import Graph, Node
import random
from typing import Tuple
from semql.core.ast import *
from collections import defaultdict

operations = [Filter, Merge, Done, GetData, ExtractValues, Sum, Average, IsEmpty, Union, NoOp, Distinct, Intersection,
              Difference]

final_ops = [cls for cls in operations if issubclass(cls, FinalOperation)]

numeric_types = ['int', 'float', 'decimal', 'numeric']

numeric_operations = ['<', '>', '=', '!=', '>=', '<=']
string_ops = ['=', '!=', '=']

question_type_mapping: Dict[str, type] = {
    'average': Average,
    'sum': Sum,
    'list': Done,
    'count': Count,
    'bool': IsEmpty
}

setop_mapping: Dict[str, type] = {
    'none': None,
    'random': random.choice([Union, Intersection, Difference]),
    'union': Union,
    'intersection': Intersection,
    'difference': Difference,
}

groupby_mapping: Dict[str, type] = {
    'none': None,
    'random': random.choice([CountBy, SumBy, AverageBy]),
    'countBy': CountBy,
    'sumBy': SumBy,
    'averageBy': AverageBy,
}

aggregation_mapping: Dict[str, type] = {
    'none': None,
    'max': Max,
    'min': Min,
}


class FilterConstraint:
    def __init__(self, table: Node, attribute_name: str, operation: str, value: str):
        self.table = table
        self.attribute_name = attribute_name
        self.operation = operation
        self.value = value

    def __deepcopy__(self, memodict={}) -> 'FilterConstraint':
        return FilterConstraint(self.table, self.attribute_name, self.operation, self.value)

    def __str__(self):
        return "{}.{} {} {}".format(self.table.name, self.attribute_name, self.operation, self.value)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.table == other.table and self.attribute_name == other.attribute_name and self.operation == other.operation and self.value == other.value

class QueryType:
    query_type_class = None

    def __init__(self, query_type:str):
        self.get_final_op_for_query(query_type)

    def get_final_op_for_query(self, query_type: str):
        #if unspecified then sample at random
        if query_type is None:
            query_type_class = random.choice(list(question_type_mapping.values()))
        else:
            query_type_class = question_type_mapping.get(query_type, None)
        if query_type_class is None:
            raise ValueError('[Query Type]: Unsupported Query Type: please use one of: {}'.format(list(question_type_mapping.keys())))
        else:
            self.query_type_class = query_type_class

    def __str__(self):
        return '{}'.format(self.query_type_class)


class ResultType:
    result_table = None
    result_attr = None

    def __init__(self, result_table: str, result_attribute:str, query_type: QueryType, graph_nodes: Dict[str, Node]):
        if result_table is None:
            self.sample_result_type(graph_nodes, query_type)
        else:
            self.result_table = graph_nodes[result_table]
            if result_attribute is None:
                self.sample_result_attribute(self.result_table, query_type)
            else:
                #check if result attribute and query type are compatible
                if query_type.query_type_class in [Sum, Average]:
                    table_node = graph_nodes[result_table]
                    table_attributes = table_node.attributes
                    table_result_attr = [attr for attr in table_attributes if attr[0] == result_attribute][0]
                    if not table_result_attr[1].lower().startswith(tuple(numeric_types)):
                        raise ValueError('[Result Type]: Defined Result Attribute "{}" is not compatible with query type: "{}"'.format(result_attribute, query_type.query_type_class))

                self.result_attr = result_attribute

    def __str__(self):
        return '{}.{}'.format(self.result_table.name, self.result_attr)

    def sample_result_type(self, graph_nodes: Dict[str, Node], query_type: QueryType):
        eligible_attr_for_table = {}
        for table_name, table in graph_nodes.items():
            eligible_attr = self.get_eligible_attributes(table, query_type)
            if len(eligible_attr) > 0:
                eligible_attr_for_table[table_name] = eligible_attr

        selected_table = random.choice(list(eligible_attr_for_table.keys()))

        self.result_attr = random.choice([attr[0] for attr in eligible_attr_for_table[selected_table] if attr[2] == ''])
        self.result_table = graph_nodes[selected_table]

    def sample_result_attribute(self, table: Node, query_type: QueryType):
        eligible_attr = self.get_eligible_attributes(table, query_type)
        self.result_attr = random.choice(eligible_attr)[0]

    @staticmethod
    def get_eligible_attributes(table: Node, query_type:QueryType):
        if query_type.query_type_class in [Sum, Average]:
            eligible_attr = [attr for attr in table.attributes if attr[2] == '' and attr[1].lower().startswith(tuple(numeric_types))]
        else:
            eligible_attr = [attr for attr in table.attributes if attr[2] == '']

        return eligible_attr


class MergePath:
    merge_path = None

    def __init__(self, result_type: ResultType, path_length: int, graph: Graph):
        self.select_path(result_type.result_table, path_length, graph)

    def select_path(self, start_node: Node, max_depth: int, graph: Graph):
        if max_depth == -1:
            target_node = random.choice(list(graph.entity_nodes))
            path_options = graph.shortest_path(start_node, target_node)
        else:
            path_options = []
            while len(path_options) == 0:
                path_options = graph.all_fixed_paths(start_node, max_depth, defaultdict(lambda: 0))
                max_depth -= 1

        if len(path_options) <= 0:
            raise ValueError('[Merge Path]: There are no Paths in the Graph that satisfy this condition!')

        self.merge_path = random.choice(path_options)

    def __len__(self):
        return len(self.merge_path)

    def __str__(self):
        return '{}'.format(self.merge_path)


class SetOpConstraint:
    setop_class = None
    attribute_table = None
    attribute = None
    constraint_filter0 = None
    constraint_filter1 = None

    def __init__(self, set_op: str, selected_path: MergePath, result_type: ResultType, entities_for_attributes: Dict, allowed_nodes: List[Node],
                 setop_window: int = 1):
        self.setop_class = self.get_class_for_setop(set_op)
        self.attribute_table = result_type.result_table
        self.attribute = [attribute for attribute in self.attribute_table.attributes if attribute[2] == 'PRI' and not attribute[2] == result_type.result_attr][0][0]

        filter_table, self.final_window = self.get_filter_table(selected_path, setop_window, allowed_nodes)
        filter_attributes = self.get_candidate_attributes(filter_table)
        if len(filter_attributes) == 0:
            raise ValueError('[SetOp Sampling]: No attributes for table: {}'.format(filter_table))
        filter_attribute = random.choice(filter_attributes)[0]

        self.set_constraint_filters(filter_table, filter_attribute, entities_for_attributes)

    def set_constraint_filters(self, filter_table, filter_attribute, entities_for_attributes):
        values = random.sample(entities_for_attributes[filter_table.name][filter_attribute], k=2)
        value0, value1 = values[0], values[1]

        if self.setop_class == Union:
            op = '='
        elif self.setop_class == Intersection:
            op = random.choice(['=']*3 + ['!='])
        else:
            op = '='

        self.constraint_filter0 = FilterConstraint(filter_table, filter_attribute, op, value0)
        self.constraint_filter1 = FilterConstraint(filter_table, filter_attribute, op, value1)

    def get_candidate_attributes(self, table_candidate: Node) -> List[str]:
        if table_candidate == self.attribute_table:
            table_candidate_attributes = [attribute for attribute in table_candidate.attributes if
                                          attribute[2] != 'PRI' and not attribute[0] == self.attribute]
        else:
            table_candidate_attributes = [attribute for attribute in table_candidate.attributes if
                                          attribute[2] != 'PRI']

        return table_candidate_attributes

    def get_filter_table(self, path: MergePath, window_size: int, allowed_nodes: List[Node], allow_window_reduction=True) -> Tuple[Node, int]:

        def is_table_candidate(table_candidate: Node) -> bool:
            table_candidate_attributes = self.get_candidate_attributes(table_candidate)

            if len(table_candidate_attributes) > 0:
                return True
            else:
                return False

        possible_tables = []
        result_idx = path.merge_path.index(self.attribute_table)
        while len(possible_tables) == 0 and window_size > 0:
            if result_idx + window_size - 1 <= len(path):
                table_candidate = path.merge_path[result_idx + window_size - 1]
                if is_table_candidate(table_candidate):
                    possible_tables.append(table_candidate)

            if result_idx - window_size + 1 >= 0:
                table_candidate = path.merge_path[result_idx - window_size + 1]
                if is_table_candidate(table_candidate):
                    possible_tables.append(table_candidate)

            possible_tables = [table for table in possible_tables if table in allowed_nodes]
            if not allow_window_reduction:
                #only allow one iteration if the window reduction is not allowed
                break
            window_size -= 1

        if len(possible_tables) <= 0:
            raise ValueError('[SetOp Sampling]: No valid window found!')

        filter_table = random.choice(possible_tables)
        filter_idx = path.merge_path.index(filter_table)

        window = path.merge_path[min([filter_idx, result_idx]): max([filter_idx, result_idx]) + 1]
        return filter_table, window

    @staticmethod
    def get_class_for_setop(setop: str) -> type:
        if setop == 'random':
            return random.choice([Union, Intersection, Difference])

        return setop_mapping[setop]

    def __str__(self):
        return "{}({}, {}, {}.{})".format(self.setop_class.__name__, self.constraint_filter0, self.constraint_filter1, self.attribute_table.name, self.attribute)


class GroupByConstraint:
    attribute_entity_type = None
    aggregate_attribute = None

    def __init__(self, result_type: ResultType, path: MergePath, groupy_by_type: str):
        self.return_entity_type = result_type.result_table
        self.return_attribute_name = result_type.result_attr
        self.groupy_by_class = self.get_class_for_groupby(groupy_by_type)

        self.select_aggregate_attribute(path.merge_path)
        #self.aggregate_attribute = '{}.{}'.format(self.attribute_entity_type, self.aggregare_attribute_name)
        self.having_filter = None

    @staticmethod
    def get_class_for_groupby(groupy_by_type: str) -> type:
        if groupy_by_type == 'random':
            groupy_by_op = random.choice([CountBy, SumBy, AverageBy])
        else:
            groupy_by_op = groupby_mapping[groupy_by_type]

        return groupy_by_op

    def select_aggregate_attribute(self, selected_path: List[Node]):
        def get_attribute(entity_type: Node):
            if self.groupy_by_class == CountBy:
                attributes = [attr for attr in entity_type.attributes if attr[2] == '']
            else:
                attributes = [attr for attr in entity_type.attributes if
                              attr[2] == '' and attr[1].lower().startswith(tuple(numeric_types))]
            if len(attributes) > 0:
                attribute = random.choice(attributes)
                return attribute[0]
            else:
                return None

        idx = selected_path.index(self.return_entity_type)
        max_idx = len(selected_path) - 1
        min_idx = 0

        difference_max = max_idx - idx  # 0 or positive
        difference_min = min_idx - idx  # 0 or negative

        while difference_max > 0 or difference_min < 0:
            if abs(difference_max) > abs(difference_min):
                entity_type = selected_path[idx + difference_max]
                attribute = get_attribute(entity_type)
                if attribute is not None:
                    self.attribute_entity_type = entity_type
                    self.aggregate_attribute = attribute
                    return
                difference_max -= 1
            else:
                entity_type = selected_path[idx + difference_min]
                attribute = get_attribute(entity_type)
                if attribute is not None:
                    self.attribute_entity_type = entity_type
                    self.aggregate_attribute = attribute
                    return
                difference_min += 1

        if difference_min == difference_max == 0:
            entity_type = selected_path[idx]
            attribute = get_attribute(entity_type)
            if attribute is not None:
                self.attribute_entity_type = entity_type
                self.aggregate_attribute = attribute
                return

        # if there is no way to find a attribute raise an error
        raise ValueError('[GroupBy]: Cannot sample an aggregation attribute!')

    def sample_having_filter(self, entities_for_attributes: Dict[str, Dict[str, List]]):
        op = random.choice(numeric_operations)
        if self.groupy_by_class == CountBy:
            value = random.choice(list(range(50)))
            self.having_filter = FilterConstraint(self.attribute_entity_type, 'count', operation=op, value=value)
        else:
            value = random.choice(entities_for_attributes[self.attribute_entity_type.name][self.aggregate_attribute])
            self.having_filter = FilterConstraint(self.attribute_entity_type, self.aggregate_attribute, operation=op, value=value)

    def __str__(self):
        return '{}({}, {})'.format(self.groupy_by_class.__name__, self.groupby_attribute, self.aggregate_attribute)


class AggregationConstraint:

    def __init__(self, agg_type:str, result_type: ResultType, path: MergePath, groupby: GroupByConstraint, setop: SetOpConstraint):
        self.aggregation_class = self.get_class_for_agg(agg_type)

        if groupby is not None:
            if groupby.groupy_by_class == CountBy:
                self.aggregation_type = ''
                self.aggregation_attribute = 'count'
            else:
                self.aggregation_type = groupby.attribute_entity_type
                self.aggregation_attribute = groupby.aggregate_attribute
        elif setop is not None:
            self.aggregation_type = setop.attribute_table
            self.aggregation_attribute = self.sample_attribute_for_table(self.aggregation_type)
        else:
            self.sample_aggregation_attribute(result_type, path)

        self.aggregation_attribute_name = '{}.{}'.format(self.aggregation_type, self.aggregation_attribute)

    @staticmethod
    def sample_attribute_for_table(table: Node) -> str:
        numeric_attributes = [attr[0] for attr in table.attributes if attr[2] == '' and attr[1].lower().startswith(tuple(numeric_types))]
        if len(numeric_attributes) == 0:
            raise ValueError('[Aggregation]: Cannot sample numeric attribute for this entity')
        else:
            return random.choice(numeric_attributes)

    def sample_aggregation_attribute(self, result_type: ResultType, path: MergePath):
        eligible_tables = {}
        for node in path.merge_path:
            numeric_attributes = [attr[0] for attr in node.attributes if attr[2] == '' and attr[1].lower().startswith(tuple(numeric_types))]
            if node == result_type.result_table:
                numeric_attributes = [x for x in numeric_attributes if x != result_type.result_attr]

            if len(numeric_attributes) > 0:
                eligible_tables[node.name] = numeric_attributes

        if len(eligible_tables) == 0:
            raise ValueError('[Aggregation]: Cannot sample numeric attribute')

        self.aggregation_type = random.choice(list(eligible_tables.keys()))
        self.aggregation_attribute = random.choice(eligible_tables[self.aggregation_type])

    @staticmethod
    def get_class_for_agg(agg_type: str) -> type:
        if agg_type == 'random':
            agg_class = random.choice([Max, Min])
        else:
            agg_class = aggregation_mapping[agg_type]

        return agg_class


class Constraints:
    def __init__(self, config_dict: Dict, graph: Graph, entities_for_attributes: Dict[str, Dict[str, List]]):
        self.graph = graph
        self.entities_for_attributes = entities_for_attributes

        self.total_constraint = config_dict['total_number_of_constraints']
        self.max_constraint_per_table = config_dict['max_constraint_per_table']
        self.path_length = config_dict['path_length']
        self.query_type = QueryType(config_dict['query_type'])

        result_table = config_dict.get('result_table', None)
        result_attr = config_dict.get('result_attr', None)
        self.data_name = config_dict.get('data_name', None)
        self.result_type = ResultType(result_table, result_attr, self.query_type, self.graph.nodes)
        self.merge_path = MergePath(self.result_type, self.path_length, self.graph)

        # get setop constraint -> infer attributes based on path
        set_op = config_dict.get('setop', 'none')
        if set_op != 'none':
            self.setop_constraint = SetOpConstraint(
                set_op,
                self.merge_path,
                self.result_type,
                self.entities_for_attributes,
                graph.entity_nodes,
                setop_window=config_dict.get('setop_window', 1)
            )
        else:
            self.setop_constraint = None

        groupby_type = config_dict.get('groupby_type', 'none')
        if groupby_type != 'none':
            self.groupby_constraint = GroupByConstraint(self.result_type, self.merge_path, groupby_type)
            if self.query_type.query_type_class in [Done, Count, IsEmpty]:
                self.groupby_constraint.sample_having_filter(entities_for_attributes)
        else:
            self.groupby_constraint = None

        aggregation_type = config_dict.get('aggregation', 'none')
        if aggregation_type != 'none':
            self.aggregation_constraint = AggregationConstraint(aggregation_type, self.result_type, self.merge_path, self.groupby_constraint, self.setop_constraint)
        else:
            self.aggregation_constraint = None

        if self.total_constraint > 0 and self.max_constraint_per_table > 0:
            self.filter_constraints = self.sample_path_constraints(self.merge_path)
        else:
            self.filter_constraints = []

    def get_query_type_class(self) -> type:
        return self.query_type.query_type_class

    def get_aggregation_class(self) -> type:
        return self.aggregation_constraint.aggregation_class

    def result_type_str(self):
        return str(self.result_type)

    def groupby_aggregate_attr(self):
        return '{}.{}'.format(self.groupby_constraint.attribute_entity_type.name, self.groupby_constraint.aggregate_attribute)

    def groupby_attribute(self):
        return '{}.{}'.format(self.groupby_constraint.return_entity_type.name, self.groupby_constraint.return_attribute_name)

    def setop_attribute(self):
        return '{}.{}'.format(self.setop_constraint.attribute_table.name, self.setop_constraint.attribute)

    def sample_constraint_for_table(self, table):
        if table == self.result_type.result_table:
            attributes = [attr for attr in table.attributes if attr[2] == '' and not attr[0] == self.result_type.result_attr]
            attribute = random.choice(attributes)
        else:
            attribute = random.choice([attr for attr in table.attributes if attr[2] == ''])
        if attribute[1].lower().startswith('varchar') or attribute[1].lower().startswith('text') or attribute[1].lower().startswith('longtext'):
            op = random.choice(string_ops)
        else:
            op = random.choice(numeric_operations)

        attribute_name = attribute[0]
        value = random.choice(self.entities_for_attributes[table.name][attribute_name])

        filter_constraint = FilterConstraint(table, attribute_name, op, value)
        return filter_constraint

    def sample_constraint_tables_for_path(self, path: MergePath, max_per_table: int = 2, overall_max: int = 4) -> List[Node]:
        eligible_tables = [x for x in path.merge_path if len([attr for attr in x.attributes if attr[2] == '' and not attr[0] is self.result_type.result_attr]) > 0]
        if len(eligible_tables) == 0:
            return []

        tables = []
        sample_table_list = eligible_tables * max_per_table
        if self.setop_constraint is not None:
            setop_constraint_table0 = self.setop_constraint.constraint_filter0.table
            node_idx = [idx for idx, node in enumerate(sample_table_list) if node == setop_constraint_table0]
            if len(node_idx) > 0:
                del sample_table_list[node_idx[0]]
                overall_max -= 1
        else:
            # need at least one filter on the other side
            result_index = path.merge_path.index(self.result_type.result_table)
            if result_index == 0:
                node = path.merge_path[-1]
                tables.append(node)
            else:
                node = path.merge_path[0]
                tables.append(node)
            node_idx = [idx for idx, n in enumerate(sample_table_list) if n == node][0]
            del sample_table_list[node_idx]
            overall_max -= 1

        tables += random.sample(sample_table_list, k=min(overall_max, len(sample_table_list)))
        # if one of these tables is already used as setop operation -> reduce max_per_table by one for this
        return tables

    def sample_path_constraints(self, selected_path: MergePath) -> List[FilterConstraint]:
        filter_constraints = []
        tables_constraints = self.sample_constraint_tables_for_path(selected_path, self.max_constraint_per_table, self.total_constraint)
        for table in tables_constraints:
            filter_constraint = self.sample_constraint_for_table(table)
            filter_constraints.append(filter_constraint)
        return filter_constraints


if __name__ == "__main__":
    attribute_triples = json.load(open('../../data/imdb_mysql/attribute_triples.json'))
    attributes_for_entity_type = json.load(open('../../data/imdb_mysql/attributes_for_entity_type.json'))
    entities_for_entitytype_attribute_pairs = json.load(
        open('../../data/imdb_mysql/entities_for_entitytype_attribute_pairs.json'))
    config_dict = json.load(open('../../conf/movie_sampling.json'))

    graph = Graph(attribute_triples, attributes_for_entity_type)

    for i in range(10000):
        try:
            constraints = Constraints(config_dict, graph, entities_for_entitytype_attribute_pairs)
            print(constraints.filter_constraints, constraints.merge_path, constraints.setop_constraint, constraints.groupby_constraint)
        except Exception as e:
            print(e)
