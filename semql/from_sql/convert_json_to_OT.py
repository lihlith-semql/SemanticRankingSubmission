from semql.core.ast import *
from typing import Dict, Tuple
from semql.from_sql.process_sql import get_sql, get_schemas_from_json, Schema, WHERE_OPS, AGG_OPS
from semql_data.data_helper import get_metadata_filepath_for_db

INVERTED_WHERE_OPS = {k: v for k, v in enumerate(WHERE_OPS)}
INVERTED_AGG_OPS = {k: v for k, v in enumerate(AGG_OPS)}


def converter_for_db(name: str):
    schemas, db_names, tables = get_schemas_from_json('tables.json')
    attr_file = get_metadata_filepath_for_db(
        name, "attributes_for_entity_type.json")
    with open(attr_file, 'r') as fin:
        attr_for_entity = json.load(fin)

    schema = Schema(schema=schemas[name], table=tables[name])

    return Converter(
        schema=schema,
        db_name=name,
        attributes_for_entity_type=attr_for_entity,
    )


class DirectConverter:

    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conv = converter_for_db(db_name)

    def __call__(self, sql_statement):
        try:
            sql_label = get_sql(self.conv.schema, sql_statement)
            ot = self.conv(sql_label)
            return ot
        except Exception as e:
            return None


class ConvertorCache:

    def __init__(self):
        self.cache = {}

    def get(self, db_name):
        c = self.cache.get(db_name)
        if c is None:
            c = DirectConverter(db_name)
            self.cache[db_name] = c
        return c



class Converter:

    def __init__(self, schema: Schema, db_name: str, attributes_for_entity_type: Dict):
        self.schema = schema
        self.db_name = db_name
        self.attributes_for_entity_type = attributes_for_entity_type

        self.inverted_tab_idx = {v: k for k, v in self.schema.idMap.items() if not '.' in k}
        self.inverted_col_idx = {v: k for k, v in self.schema.idMap.items() if '.' in k}
        self.inverted_col_idx[0] = '*'

    def parse_table_units(self,table_units: List):
        get_data_nodes = []
        for table_unit in table_units:
            tab_name = self.inverted_tab_idx[table_unit[1]]
            get_data = GetData(data_source=self.db_name, table_name=tab_name)
            get_data_nodes.append(get_data)
        return get_data_nodes

    def create_merge_nodes(self, get_data_nodes, conditions):
        assert len(get_data_nodes) - 1 == len(conditions)
        curr_merge = get_data_nodes[0]
        for idx in range(len(conditions)):
            g1= get_data_nodes[idx + 1]
            condition = conditions[idx]
            col_id0 = condition[2][1][1] #very ugly inedxing.. hope this is consistent
            col_id1 = condition[3][1]
            col_name0 = self.inverted_col_idx[col_id0]
            col_name1 = self.inverted_col_idx[col_id1]

            curr_merge = Merge(curr_merge, g1, col_name0, col_name1)
        return curr_merge


    def parse_from(self,from_label):
        table_units = from_label['table_units']
        conditions = from_label.get('conds', [])
        filtered_conds = [cond for cond in conditions if not type(cond) == str]
        get_data_nodes = self.parse_table_units(table_units)
        merged = self.create_merge_nodes(get_data_nodes, filtered_conds)
        return merged

    def get_attr_type(self, attr_name):
        table_name = attr_name.split('.')[0]
        att_name = attr_name.split('.')[1]
        attributes = self.attributes_for_entity_type[table_name]
        for att, type_name, key_type in attributes:
            if att == att_name:
                if type_name.lower().startswith('varchar'):
                    type_name = 'varchar'
                return type_name.lower()

    def parse_single_where(self, where_label: Tuple):
        op_id = where_label[1]
        op_str = INVERTED_WHERE_OPS[op_id]
        if op_id == 8:
            raise NotImplementedError('We do NOT support IN or NOT IN')
        attr_id = where_label[2][1][1]
        attr_name = self.inverted_col_idx[attr_id]
        table_name = attr_name.split('.')[0]
        att_type = self.get_attr_type(attr_name)
        if type(where_label[3]) == dict:
            raise NotImplementedError('We do NOT support NESTED QUERIES')

        if att_type in ['int', 'integer']:
            value = int(where_label[3].strip('"'))
        elif att_type in ['real']:
            value = float(where_label[3].strip('"'))
        elif att_type in ['text', 'varchar']:
            value = where_label[3].strip('"')
        elif type(where_label[3]) == dict:
            raise NotImplementedError('We do NOT support NESTED QUERIES')
        else:
            raise NotImplementedError('We do NOT support NESTED QUERIES')

        if op_id == 1:
            val1 = int(where_label[4].strip('"'))
            value = sorted([value, val1])

        return table_name, attr_name, op_str, value

    def insert_filter(self, current_ot: Operation, table_name:str, attr_name:str, op_str:str, value:str):
        get_data = [node for node in current_ot.return_leaves() if node.table_name == table_name][0]
        #filter = Filter(get_data, attr_name, op_str, value)
        if not op_str == 'between':
            arguments = {
                'table': get_data,
                'attribute_name': attr_name,
                'operation': op_str,
                'value': value
            }
            is_new_root = get_data.insert_node_on_path(Filter, arguments)
            if is_new_root:
                current_ot = get_data.parent
        else:
            arguments = {
                'table': get_data,
                'attribute_name': attr_name,
                'operation': '>',
                'value': value[0]
            }
            is_new_root = get_data.insert_node_on_path(Filter, arguments)
            if is_new_root:
                current_ot = get_data.parent

            arguments = {
                'table': get_data,
                'attribute_name': attr_name,
                'operation': '<',
                'value': value[1]
            }
            is_new_root = get_data.insert_node_on_path(Filter, arguments)
            if is_new_root:
                current_ot = get_data.parent

        return current_ot

    def parse_where_clause(self, current_ot: Operation, where_labels: List):
        if 'or' in where_labels:
            raise NotImplementedError('OR ops are not supported')
        for where_label in where_labels:
            if where_label == 'and':
                continue
            table_name, attr_name, op_str, value = self.parse_single_where(where_label)
            current_ot = self.insert_filter(current_ot, table_name, attr_name, op_str, value)
        return current_ot

    def parse_single_select(self, select_label):
        agg_type = INVERTED_AGG_OPS[select_label[0]]
        attribute = self.inverted_col_idx[select_label[1][1][1]]
        is_distinct = select_label[1][1][2]
        return agg_type, attribute, is_distinct

    def add_nodes_to_ot(self, current_op: Operation, selection_data: List):
        pass

    def add_single_node(self, current_op: Operation, selection: Tuple, is_distinct_over: bool):
        #will be depricated soon, only here since we handle single select args atm
        agg_type, attr_name, is_distinct = selection
        if is_distinct or is_distinct_over:
            current_op = Distinct(current_op, attribute_name=attr_name, ignore_primary_key=True)
        if agg_type == 'none':
            if attr_name == '*':
                current_op = Done(current_op)
            else:
                ev = ExtractValues(current_op, attr_name)
                current_op = Done(ev)
        elif agg_type == 'max':
            current_op = Max(current_op, attr_name)
            ev = ExtractValues(current_op, attr_name)
            current_op = Done(ev)
        elif agg_type == 'min':
            current_op = Min(current_op, attr_name)
            ev = ExtractValues(current_op, attr_name)
            current_op = Done(ev)
        elif agg_type == 'count':
            if attr_name == '*':
                attr_name = '*.*'
                #current_op = ExtractValues(current_op, attr_name)
            else:
                current_op = ExtractValues(current_op, attr_name)
            current_op = Count(current_op)
        elif agg_type == 'sum':
            current_op = Sum(current_op, attr_name)
        elif agg_type == 'avg':
            current_op = Average(current_op, attr_name)

        return current_op

    def create_final_projection_node(self, current_op: Operation, selection_data: List[Tuple], is_distinct: bool):
        d = {
            'none': ProjectionRoot.ProjectionFN.NONE,
            'max': ProjectionRoot.ProjectionFN.MAX,
            'min': ProjectionRoot.ProjectionFN.MIN,
            'count': ProjectionRoot.ProjectionFN.COUNT,
            'sum': ProjectionRoot.ProjectionFN.SUM,
            'avg': ProjectionRoot.ProjectionFN.AVG
        }

        attrs = []
        for selection_dp in selection_data:
            sel_type = d[selection_dp[0]]
            attr_name = selection_dp[1]
            if attr_name == '*':
                attr_name = '*.*'

            attrs.append((attr_name, sel_type))

        node = ProjectionRoot(current_op, attrs, is_distinct)
        return node

    def parse_select_clause(self, current_op: Operation, select_labels: List):
        selection_data = []
        is_distinct_global = select_labels[0]
        for select_lable in select_labels[1]:
            agg_type, attibute_name, is_distinct = self.parse_single_select(select_lable)
            selection_data.append((agg_type, attibute_name, is_distinct))
        #create nodes here: for now, just create one node, since we do not support multiple args in seleciton yet
        if len(selection_data) > 1:
            ot = self.create_final_projection_node(current_op, selection_data, is_distinct_global)
        else:
            ot = self.add_single_node(current_op, selection_data[0], is_distinct_global)
        return ot

    def parse_orderby(self, current_op: Operation, orderby_labels: Tuple, limit: int):
        if len(orderby_labels) == 0:
            return current_op
        order_type = orderby_labels[0]
        order_attr_id = orderby_labels[1][0][1][1]
        order_attr = self.inverted_col_idx[order_attr_id]

        if limit == 1:
            if order_type == 'desc':
                current_op = Max(current_op, attribute_name=order_attr)
            else:
                current_op = Min(current_op, attribute_name=order_attr)
        else:
            raise NotImplementedError('ORDERBY IS NOT IMPLEMENTED')

        return current_op

    def __call__(self, sql_label):
        if len(sql_label['groupBy']) > 0:
            raise NotImplementedError('GROUPBY is not Implemented')
        if sql_label['intersect'] is not None and len(sql_label['intersect']) > 0:
            raise NotImplementedError('INTERSECT is not Implemented')
        if sql_label['union'] is not None and len(sql_label['union']) > 0:
            raise NotImplementedError('UNION is not Implemented')
        if sql_label['except'] is not None and len(sql_label['except']) > 0:
            raise NotImplementedError('EXCEPT is not Implemented')
        ot = self.parse_from(sql_label['from'])
        ot = self.parse_where_clause(ot, sql_label['where'])
        ot = self.parse_orderby(ot, sql_label['orderBy'], limit=sql_label['limit'])
        ot = self.parse_select_clause(ot, sql_label['select'])

        return ot