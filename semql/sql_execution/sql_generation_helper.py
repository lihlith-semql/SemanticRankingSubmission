import json
from typing import Dict
from semql_data import data_helper

attributes4entity_types = 'attributes_for_entity_type.json'

databases = [
    'chinook_1',
    'chinook_synthetic',
    'college_2',
    'driving_school',
    'formula_1',
    'moviedata',
    'moviedata_synthetic',
    'moviedata_beta',
    'battle_death',
    'car_1',
    'concert_singer',
    'course_teach',
    'cre_Doc_Template_Mgt',
    'dog_kennels',
    'employee_hire_evaluation',
    'flight_2',
    'museum_visit',
    'network_1',
    'orchestra',
    'pets_1',
    'poker_player',
    'real_estate_properties',
    'singer',
    'student_transcripts_tracking',
    'tvshow',
    'voter_1',
    'world_1',
    'wta_1'
]

attributes_for_tables = {}
for database in databases:
    attributes_for_tables[database] = json.load(open(
        data_helper.get_metadata_filepath_for_db(database, attributes4entity_types)))


def wrap_statement_in_select(statement: str, preview_limit: int = 0) -> str:
    statement = ' SELECT * FROM (' + statement + ' )'
    if preview_limit > 0:
        statement += ' LIMIT ' + str(preview_limit)
    return statement


def get_value_for_statement(value) -> str:
    """
    If the type of a value is a string, then the value is wrapped in apostrophes and returned.
    If the type is an int/float/etc., then str() is called on the value and the value is returned
    :param value:
    :return: string, either wrapped in apostrophes or not
    """
    if value is None:
        return ' NULL '
    if type(value) is str:
        return '"' + value.replace('"', "'") + '"'
    if value == 'count':
        value = 'cnt'  # count is an sql-term so it may confuse some naive parsers

    return str(value)


def get_attributes_with_aliases_for_table(table: str, datasource: str) -> Dict[str, str]:
    """
    Returns a dictionary mapping the attributes of a given table to the corresponding aliases
    :param table: The table for which the attributes and aliases should be returned.
    :return: Dictionary, which maps the attributes of a given table to the corresponding aliases
    """
    table_attributes = attributes_for_tables[datasource][table]
    return {triple[0]: get_attribute_alias(table, triple[0]) for triple in table_attributes}


def get_aliases_for_table(table: str, datasource: str) -> str:
    """
    Returns a string of attributes and aliases for a given table, which can directly be used in an sql statement.
    E.g.: 'id as table_id, name as table_name, ...'
    :param table: The table for which the attributes and aliases should be returned.
    :return: string of attributes and aliases
    """
    attr2alias = get_attributes_with_aliases_for_table(table, datasource)
    attr2alias_items = list(attr2alias.items())
    projection = ''
    for attr, alias in attr2alias_items[:-1]:
        projection += alias + ', '
    last_attr, last_alias = attr2alias_items[-1]
    projection += last_alias
    return projection


def get_attribute_alias(table: str, attr: str) -> str:
    """
    Creates an alias for a given table and attribute by joining them together with an underscore.
    :param table:
    :param attr:
    :return:
    """
    if attr == '*' and table == '*':
        return '*'
    return table + '_' + attr


def is_attribute_primary_key(table: str, attribute: str, datasource: str) -> bool:
    """
    Checks if an attribute is a primary key for a given table.
    :param table:
    :param attribute:
    :return: True if attribute is a primary key, false otherwise
    """
    return any([triple[0] == attribute and triple[2] == 'PRI' for triple in attributes_for_tables[datasource][table]])


def escape_phrase(phrase: str) -> str:
    """
    This helper method wraps a given phrase into double quotes. This is i.e. used to wrap table names or attributes
    into double quotes, because in SQLite it could get a naming clash with keywords.
    :param phrase:
    :return:
    """
    return '"' + phrase + '"'
