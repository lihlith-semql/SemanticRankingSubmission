from semql.core.ast import *
from semql.from_sql.process_sql import get_schemas_from_json, Schema, get_sql, AGG_OPS
from semql.from_sql.convert_json_to_OT import Converter
from semql_data.data_helper import get_path_to_db_file, get_metadata_filepath_for_db
from semql.to_text.db_meta import DB_META_MAP
from semql.to_text.generator import generator

INVERTED_AGG_OPS = {k: v for k, v in enumerate(AGG_OPS)}

if __name__ == '__main__':
    table_file = "tables.json"
    fname = 'bridge_table_output_withdb.txt'
    schemas, db_names, tables = get_schemas_from_json(table_file)
    all_attributes_for_entity_type = {}

    sql = 'SELECT T1.dog_id FROM Treatments AS T1 JOIN Dogs AS T2 ON T1.dog_id = T2.dog_id WHERE T2.age > 9'
    db_id = "dog_kennels"
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
    c = Converter(sql_label, schema, db_id, all_attributes_for_entity_type[db_id])
    ot = c()
    sql = ot.to_sql()

    print(sql)
    hypothesis_str = generator(ot, DB_META_MAP[db_id])
    print(hypothesis_str)
    print(ot.print())
    print("Return the number of flights departing from Aberdeen.")