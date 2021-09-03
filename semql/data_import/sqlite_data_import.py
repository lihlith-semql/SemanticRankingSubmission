from typing import Dict, List, Tuple
from semql.data_import.data_import_template import DataImport
import argparse
import json
from semql.data_sources import SqliteDataSource
from collections import defaultdict
from semql_data import data_helper


class SQLLiteImport(DataImport):
    def __init__(self, config_dict: Dict):
        path = config_dict['path']
        self.max_values = config_dict['max_values_for_col']
        self.table_blacklist = config_dict['table_blacklist']
        self.table_attribute_blacklist = config_dict['table_attribute_blacklist']

        self.data_source = SqliteDataSource({'db_path': path})
        super(SQLLiteImport, self).__init__(config_dict)


    def get_table_names(self):
        return self.data_source.get_table_names()

    def get_attributes_for_entity_types(self) -> Dict[str, List[Tuple[str, str, str]]]:
        attributes_for_tables = self.data_source.get_attributes_for_tables()
        new_attributes_for_tables = dict()
        for table_name, attributes in attributes_for_tables.items():
            new_attributes = []
            for attribute in attributes:
                new_attribute = (
                    attribute[0].decode('utf-8').lower(),
                    attribute[1].decode('utf-8'),
                    attribute[2]
                )
                new_attributes.append(new_attribute)

            new_attributes_for_tables[table_name.lower()] = new_attributes

        return new_attributes_for_tables

    def get_attribute_triples(self, entity_types: List[str]) -> Dict[str, Tuple]:
        attribute_triples = self.data_source.get_references_for_tables()
        new_attribute_triples = {}
        for table_name, attributes in attribute_triples.items():
            new_attributes = []
            for attribute in attributes:
                new_attribute = (
                    attribute[0].decode('utf-8').lower(),
                    attribute[1].decode('utf-8').lower(),
                    attribute[2].decode('utf-8').lower()
                )
                new_attributes.append(new_attribute)
            new_attribute_triples[table_name.lower()] = new_attributes
        return new_attribute_triples

    def get_entities_for_entitytype_attribute_pairs(self, attributes_for_entity_types: Dict[str, List[Tuple[str, str, str]]]) -> Dict[str, Dict[str, List]]:
        entities_for_entitytype_attribute_pairs = dict()

        for entity_type, attributes in attributes_for_entity_types.items():
            values_for_attribute = defaultdict(lambda: set())
            data, columns = self.data_source.get_data(entity_type)
            for row in data:
                for attribute, value in row.items():
                    values_for_attribute[attribute.split('.')[1].lower()].add(value)

            for attribute, value_set in values_for_attribute.items():
                values_for_attribute[attribute] = list(value_set)

            entities_for_entitytype_attribute_pairs[entity_type.lower()] = dict(values_for_attribute)

        return entities_for_entitytype_attribute_pairs


if __name__ == "__main__":
    config_file = 'movie_sampling.json'
    with open(data_helper.get_config_file_path(config_file), 'rt', encoding='utf-8') as ifile:
        config_dict = json.load(ifile)

    parser = argparse.ArgumentParser(description='Preprocess Data')
    parser.add_argument('-d, --data-name', dest='data_name', type=str)
    args = parser.parse_args()
    config_dict['data_name'] = args.data_name

    config_dict['path'] = data_helper.get_path_to_db_file(config_dict['data_name'])

    SQLLiteImport(config_dict)