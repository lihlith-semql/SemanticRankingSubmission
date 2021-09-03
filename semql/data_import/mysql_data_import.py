from typing import Dict, List, Tuple
from semql.data_import.data_import_template import DataImport
import mysql.connector
from collections import defaultdict
import random
import argparse
import json
from decimal import Decimal
from datetime import date


class MySQLImport(DataImport):
    def __init__(self, config_dict: Dict):
        host = config_dict['host']
        username = config_dict['username']
        password = config_dict['password']
        self.max_values = config_dict['max_values_for_col']
        self.table_blacklist = config_dict['table_blacklist']
        self.table_attribute_blacklist = config_dict['table_attribute_blacklist']

        mydb = mysql.connector.connect(
            host=host,
            user=username,
            passwd=password,
            database="moviedata"
        )

        self.cursor = mydb.cursor()
        super(MySQLImport, self).__init__(config_dict)

    def get_table_names(self):
        self.cursor.execute("USE moviedata")
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        table_names = []
        for (table_name,) in tables:
            if table_name not in self.table_blacklist:
                table_names.append(table_name)
        return table_names

    def get_attributes_for_entity_types(self) -> Dict[str, List[Tuple[str, str, str]]]:
        entity_types = self.get_table_names()
        attributes_for_table = {}
        attr_types = set()
        for table in entity_types:
            self.cursor.execute("describe {}".format(table))
            result = self.cursor.fetchall()
            attributes = []
            for attr_name, attr_type, isNull, key_type, default_value, extra in result:
                if attr_name not in self.table_attribute_blacklist.get(table, []):
                    attributes.append((attr_name, attr_type, key_type))
            attributes_for_table[table] = attributes
        return attributes_for_table

    def get_attribute_triples(self, entity_types: List[str]) -> Dict[str, Tuple]:
        table_to_references = defaultdict(lambda: [])

        for table in entity_types:
            self.cursor.execute("\
                    select TABLE_NAME, COLUMN_NAME,REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
                    from INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                    where REFERENCED_TABLE_NAME = '{}'".format(table)
                           )
            result = self.cursor.fetchall()
            for table_name, col_name, ref_table_name, ref_col_name in result:
                table_to_references[table_name].append((ref_table_name, col_name, ref_col_name))
        return dict(table_to_references)

    def get_entities_for_entitytype_attribute_pairs(self, attributes_for_entity_types: Dict[str, List[Tuple[str, str, str]]]) -> Dict[str, Dict[str, List]]:
        entities_for_entitytype_attribute_pairs = dict()

        for entity_types, attributes in attributes_for_entity_types.items():
            entities_for_attribute = dict()
            for attribute_name, attribute_type, key_type in attributes:
                query = "select distinct {0}.{1} from {0} where {0}.{1} IS NOT NULL;".format(entity_types, attribute_name)
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                entities = list()
                for entity in result:
                    if type(entity[0]) == Decimal:
                        entity = float(entity[0])
                    elif type(entity[0]) == date:
                        entity = str(entity[0])
                    else:
                        entity = entity[0]

                    entities.append(entity)
                if len(entities) > self.max_values:
                    entities = random.sample(entities, k=self.max_values)
                entities_for_attribute[attribute_name] = entities
            entities_for_entitytype_attribute_pairs[entity_types] = entities_for_attribute
        return entities_for_entitytype_attribute_pairs


if __name__ == "__main__":
    with open('conf/movie_sampling.json', 'rt', encoding='utf-8') as ifile:
        config_dict = json.load(ifile)

    parser = argparse.ArgumentParser(description='Preprocess Data')
    parser.add_argument('-o, --host', dest='host', default='localhost', type=str)
    parser.add_argument('-u, --user', dest='user', type=str)
    parser.add_argument('-p, --password', dest='password', type=str)
    args = parser.parse_args()

    config_dict['host'] = args.host
    config_dict['username'] = args.user
    config_dict['password'] = args.password

    MySQLImport(config_dict)