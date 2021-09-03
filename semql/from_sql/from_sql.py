import os, json

from semql.execution import ExecutorFactory
from semql.core.ast import *
from semql.from_sql.process_sql import get_schemas_from_json, Schema, get_sql, AGG_OPS
from semql.from_sql.convert_json_to_OT import Converter
from semql_data.data_helper import get_path_to_db_file, get_metadata_filepath_for_db
from collections import Counter
from semql.to_text.db_meta import DB_META_MAP
from semql.to_text.generator import generator

INVERTED_AGG_OPS = {k: v for k, v in enumerate(AGG_OPS)}

def run_sql(tree: Operation):
    sql_executor = ExecutorFactory().get_executor('sql', tree)
    sampled_tree_result = sql_executor.run(save_intermediate_result_in_nodes=False)
    return sampled_tree_result


def run_raw_sql(sql_statement, db_name):
    datasource: SqliteDataSource = BaseDataSource.get(db_name)
    try:
        result_dicts, columns = datasource.execute_sql(sql_statement)
    except Exception as e:
        if datasource is not None:
            datasource.conn.close()
        raise e
    op_result = OpResult(result_dicts, columns)
    datasource.conn.close()
    return op_result


def update_cols(op_sql_res, table, select_labels: List):
    attribute_names = []
    for select_label in select_labels:
        agg_type = INVERTED_AGG_OPS[select_label[0]]
        attribute = table['column_names_original'][select_label[1][1][1]]
        att_name = attribute[1].lower()
        if att_name == '*' and agg_type == 'none':
            attribute_name = '*'
        elif att_name == '*' and agg_type == 'count':
            attribute_name = 'COUNT(*)'
        else:
            table_name = table['table_names_original'][attribute[0]].lower()
            attribute_name = f'{table_name}_{att_name}'
        attribute_names.append(attribute_name)

    new_col_names = {}
    for col_name in op_sql_res.get_columns():
        col_name = col_name.lower()
        col_name_inner = col_name.replace('avg(', '').replace(')', '')
        col_name_inner = col_name_inner.replace('sum(', '').replace(')', '')
        col_name_inner = col_name_inner.replace('min(', '').replace(')', '')
        col_name_inner = col_name_inner.replace('count(', '').replace('distinct', '').replace(')', '')
        col_name = col_name_inner.replace('max(', '').replace(')', '').strip()
        if '.' in col_name:
            col_name = col_name.split('.')[1]

        for table_col in table['column_names_original']:
            if col_name == table_col[1].lower():
                table_name = table['table_names_original'][table_col[0]].lower()
                attr_name = f'{table_name}_{col_name}'
                if attr_name in attribute_names:
                    new_col_names[col_name] = attr_name.lower()
                    break

    for idx in range(len(op_sql_res._column_names)):
        col_name = op_sql_res._column_names[idx].lower()
        if col_name.lower() == 'count(*)':
            op_sql_res._column_names[idx] = col_name.upper()
        elif col_name.lower().startswith('count('):
            col_name_inner = col_name.replace('count(', '').replace('distinct', '').replace(')', '').strip()
            if '.' in col_name_inner:
                col_name_inner = col_name_inner.split('.')[1]
            new_col_name = new_col_names[col_name_inner]
            op_sql_res._column_names[idx] = f'COUNT(*)'
        elif col_name.lower().startswith('avg('):
            col_name_inner = col_name.replace('avg(', '').replace(')', '')
            if '.' in col_name_inner:
                col_name_inner = col_name_inner.split('.')[1]
            new_col_name = new_col_names[col_name_inner]
            op_sql_res._column_names[idx] = f'AVG({new_col_name})'
        elif col_name.lower().startswith('sum('):
            col_name_inner = col_name.replace('sum(', '').replace(')', '')
            if '.' in col_name_inner:
                col_name_inner = col_name_inner.split('.')[1]
            new_col_name = new_col_names[col_name_inner]
            op_sql_res._column_names[idx] = f'SUM({new_col_name})'
        elif col_name.lower().startswith('min('):
            col_name_inner = col_name.replace('min(', '').replace(')', '')
            if '.' in col_name_inner:
                col_name_inner = col_name_inner.split('.')[1]
            new_col_name = new_col_names[col_name_inner]
            op_sql_res._column_names[idx] = f'MIN({new_col_name})'
        elif col_name.lower().startswith('max('):
            col_name_inner = col_name.replace('max(', '').replace(')', '')
            if '.' in col_name_inner:
                col_name_inner = col_name_inner.split('.')[1]
            new_col_name = new_col_names[col_name_inner]
            op_sql_res._column_names[idx] = f'MAX({new_col_name})'
        else:
            if '.' in col_name:
                col_name = col_name.split('.')[1]
            new_col_name = new_col_names[col_name]
            op_sql_res._column_names[idx] = new_col_name


    for idx in range(len(op_sql_res._data_dicts)):
        record = op_sql_res._data_dicts[idx]
        new_record = {}
        for key, value in record.items():
            key = key.lower()
            if key.lower() == 'count(*)':
                new_record[key.upper()] = value
            elif key.lower().startswith('count('):
                col_name_inner = key.replace('count(', '').replace('distinct', '').replace(')', '').strip()
                if '.' in col_name_inner:
                    col_name_inner = col_name_inner.split('.')[1]
                new_col_name = new_col_names[col_name_inner]
                new_record[f'COUNT(*)'] = value
            elif key.lower().startswith('avg('):
                col_name_inner = key.replace('avg(', '').replace(')', '')
                if '.' in col_name_inner:
                    col_name_inner = col_name_inner.split('.')[1]
                new_col_name = new_col_names[col_name_inner]
                new_record[f'AVG({new_col_name})'] = value
            elif key.lower().startswith('sum('):
                col_name_inner = key.replace('sum(', '').replace(')', '')
                if '.' in col_name_inner:
                    col_name_inner = col_name_inner.split('.')[1]
                new_col_name = new_col_names[col_name_inner]
                new_record[f'SUM({new_col_name})'] = value
            elif key.lower().startswith('min('):
                col_name_inner = key.replace('min(', '').replace(')', '')
                if '.' in col_name_inner:
                    col_name_inner = col_name_inner.split('.')[1]
                new_col_name = new_col_names[col_name_inner]
                new_record[f'MIN({new_col_name})'] = value
            elif key.lower().startswith('max('):
                col_name_inner = key.replace('max(', '').replace(')', '')
                if '.' in col_name_inner:
                    col_name_inner = col_name_inner.split('.')[1]
                new_col_name = new_col_names[col_name_inner]
                new_record[f'MAX({new_col_name})'] = value
            else:
                if '.' in key:
                    key = key.split('.')[1]
                new_key = new_col_names[key]
                new_record[new_key] = value

        op_sql_res._data_dicts[idx] = new_record

    return new_col_names


if __name__ == '__main__':
    table_file = "tables.json"
    fname = 'bridge_table_output_withdb.txt'
    schemas, db_names, tables = get_schemas_from_json(table_file)
    all_attributes_for_entity_type = {}

    sql = "SELECT count(*) FROM pets"
    db_id = "pets_1"
    table_file = "tables.json"

    with open(get_metadata_filepath_for_db(db_id, 'attributes_for_entity_type.json'), 'rt', encoding='utf-8') as ifile:
        all_attributes_for_entity_type[db_id] = attributes_for_entity_type = json.load(ifile)

    BaseDataSource.set(
        'SqliteDataSource',
        {'db_path': get_path_to_db_file(db_id)},
        key=db_id
    )

    schema = schemas[db_id]
    table = tables[db_id]
    schema = Schema(schema, table)
    sql_label = get_sql(schema, sql)
    c = Converter(schema, db_id, all_attributes_for_entity_type[db_id])
    ot = c(sql_label)
    gold_ot = generator(ot, DB_META_MAP[db_id])
    op_ot_res = run_sql(ot)
    op_sql_res = run_raw_sql(sql, db_id)
    new_col_map = update_cols(op_sql_res, table, sql_label['select'][1])
    is_corr = op_ot_res == op_sql_res

    err_counter = Counter()
    good_dbs = Counter()
    root_nodes = Counter()
    n_corr, N = 0, 0
    dbs = set()
    allowed_dbs = {'car_1'}
    with open(fname, 'rt', encoding='utf-8') as res_file:
        for line in res_file.readlines():
            jdict = json.loads(line.replace('\n', ''))
            index = jdict['index']
            beams = jdict['beams']
            db_name = jdict['db_name']
            if not db_name in allowed_dbs:
                pass

            if db_name not in all_attributes_for_entity_type.keys():
                with open(get_metadata_filepath_for_db(db_name, 'attributes_for_entity_type.json'), 'rt',
                          encoding='utf-8') as ifile:
                    all_attributes_for_entity_type[db_name] = attributes_for_entity_type = json.load(ifile)

            schema = schemas[db_name]
            table = tables[db_name]
            schema = Schema(schema, table)
            dbs.add(db_name)
            correct_code = beams[0]['correct_code']
            orig_question = beams[0]['orig_question']

            BaseDataSource.set(
                'SqliteDataSource',
                {'db_path': get_path_to_db_file(db_name)},
                key=db_name
            )
            ot = None
            try:
                sql_label = get_sql(schema, correct_code)
                c = Converter(schema, db_name, all_attributes_for_entity_type[db_name])
                ot = c(sql_label)
                gold_ot = generator(ot, DB_META_MAP[db_name])
                print('-'*len(orig_question))
                print(correct_code)
                print(orig_question)
                print(gold_ot)
                print('-'*len(orig_question))
                good_dbs.update([db_name])
                root_nodes.update([type(ot)])
                n_corr += 1
            except NotImplementedError as e:
                err_counter.update([str(e)])
                #print(e, db_name, correct_code)
            except Exception as e:
                print('-' * len(orig_question))
                print(correct_code)
                print(orig_question)
                if ot is not None:
                    print(ot.print())
                print('-' * len(orig_question))
                err_counter.update([str(e)])
                pass
            N += 1

    print(err_counter)
    print(n_corr, N, n_corr/N)
    print(sorted(list(dbs)))
    print(good_dbs)
    print(root_nodes)
