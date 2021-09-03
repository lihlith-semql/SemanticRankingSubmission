import json

from semql.from_sql.process_sql import get_schemas_from_json, Schema, get_sql
from semql.from_sql.convert_json_to_OT import Converter
from semql_data.data_helper import get_metadata_filepath_for_db


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


class MyConvertor:
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
            c = MyConvertor(db_name)
            self.cache[db_name] = c
        return c