from abc import abstractmethod, ABC
from typing import List, Dict, Set, Tuple
import json
import os
from semql_data.data_helper import BASE_PATH_DATABASE_METADATA


class DataImport(ABC):
    def __init__(self, config_dict: Dict):
        data_name = config_dict['data_name']
        data_type = config_dict['data_type']
        attributes_for_entity_types = self.get_attributes_for_entity_types()
        entity_type = list(attributes_for_entity_types.keys())
        attribute_triples = self.get_attribute_triples(entity_type)
        entities_for_entitytype_attribute_pairs = self.get_entities_for_entitytype_attribute_pairs(attributes_for_entity_types)

        path0 = os.path.join(BASE_PATH_DATABASE_METADATA, data_type)
        if not os.path.exists(path0):
            os.mkdir(path0)

        path = os.path.join(path0, data_name)
        if not os.path.exists(path):
            os.mkdir(path)

        with open(os.path.join(path, 'attributes_for_entity_type.json'), 'wt', encoding='utf-8') as ofile:
            json.dump(attributes_for_entity_types, ofile, ensure_ascii=False)

        with open(os.path.join(path, 'attribute_triples.json'), 'wt', encoding='utf-8') as ofile:
            json.dump(attribute_triples, ofile, ensure_ascii=False)

        with open(os.path.join(path, 'entities_for_entitytype_attribute_pairs.json'), 'wt', encoding='utf-8') as ofile:
            json.dump(entities_for_entitytype_attribute_pairs, ofile, ensure_ascii=False)


    @abstractmethod
    def get_attributes_for_entity_types(self) -> Dict[str, List[Tuple[str, str, str]]]:
        pass

    @abstractmethod
    def get_attribute_triples(self, entity_types: List[str]) -> Dict[str, Tuple]:
        pass

    @abstractmethod
    def get_entities_for_entitytype_attribute_pairs(self, attributes_for_entity_types: Dict[str, List[Tuple[str, str, str]]]) -> Dict[str, Dict[str, List[str]]]:
        pass

