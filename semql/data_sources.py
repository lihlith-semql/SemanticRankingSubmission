import sqlite3

from abc import abstractmethod, ABC
from typing import List, Tuple, Dict
from contextlib import closing
from operator import itemgetter

from cuttlepool import CuttlePool


class BaseDataSource(ABC):
    _current = {}

    """
    The base class for all other `DataSource` classes.
    """
    def __init__(self, config: Dict):
        self.config = config
        super().__init__()

    @staticmethod
    def get(key: str=None) -> 'BaseDataSource':
        class_name = BaseDataSource._current[key]['class_name']
        config = BaseDataSource._current[key]['config']

        if not class_name in globals():
            raise ValueError(f'invalid data source class {class_name}')
        else:
            return globals()[class_name](config)

    @staticmethod
    def set(class_name: str, config: Dict, key: str = None) -> None:
        BaseDataSource._current[key] = {
            'class_name': class_name,
            'config': config
        }

    @staticmethod
    def has(key: str = None) -> bool:
        if BaseDataSource._current.get(key, None) is None:
            return False

        class_name = BaseDataSource._current[key]['class_name']
        return class_name in globals()

    @abstractmethod
    def get_table_names(self) -> List[str]:
        pass

    @abstractmethod
    def get_attributes_for_tables(self) -> List[Tuple]:
        pass

    @abstractmethod
    def get_references_for_tables(self) -> Dict:
        pass

    @abstractmethod
    def get_data(self, table_name: str, filters: Dict={}):
        pass

    def get_data_and_project(self, table_name: str, column_names: List[str]):
        pass


class SqliteDataSource(BaseDataSource):
    TABLE_NAMES_SQL = "SELECT name FROM sqlite_master WHERE type='table';"
    TABLE_ATTRS_SQL = "PRAGMA TABLE_INFO({})"
    TABLE_REFS_SQL = "PRAGMA FOREIGN_KEY_LIST({})"

    def __init__(self, config: Dict):
        if 'db_path' not in config:
            raise ValueError('Missing mandatory config value "db_path"')
        else:
            self.conn = SqliteConnectionPool.for_path(config['db_path']).get_resource()

        super().__init__(config)

    def get_table_names(self) -> List[str]:
        with closing(self.conn.cursor()) as c:
            table_names = c.execute(SqliteDataSource.TABLE_NAMES_SQL)
            return list(map(lambda x: x[0].lower().decode('utf-8'), table_names))

    def get_attributes_for_tables(self) -> Dict[str, List[Tuple[bytes, bytes, str]]]:
        table_names = self.get_table_names()
        table_attrs = {x: [] for x in table_names}

        with closing(self.conn.cursor()) as c:
            for table in table_names:
                foreign_keys_res = c.execute(SqliteDataSource.TABLE_REFS_SQL.format(table))
                foreign_keys = [fk[3] for fk in foreign_keys_res]

                attrs_sql = SqliteDataSource.TABLE_ATTRS_SQL.format(table)
                attrs_res = c.execute(attrs_sql)
                for attr in attrs_res:
                    key_info = ''
                    if attr[1] in foreign_keys:
                        key_info = 'MUL'
                    elif attr[5] > 0: # is pk?
                        key_info = 'PRI'

                    new_attr = [
                        attr[1],  # name
                        attr[2],  # type
                        key_info
                    ]
                    table_attrs[table].append(new_attr)

            return table_attrs

    def get_references_for_tables(self) -> Dict:
        table_names = self.get_table_names()
        table_refs = {x: [] for x in table_names}

        with closing(self.conn.cursor()) as c:
            for table in table_names:
                refs_sql = SqliteDataSource.TABLE_REFS_SQL.format(table)
                refs_res = c.execute(refs_sql)
                table_refs[table] = [list(r[2:5]) for r in refs_res]

            return table_refs

    def get_data(self, table_name: str):
        table_names = self.get_table_names()
        if table_name not in table_names:
            raise ValueError(f'Table {table_name} does not exist')

        with closing(self.conn.cursor()) as c:
            result = c.execute(f'SELECT * FROM {table_name}')
            columns = list(map(itemgetter(0), result.description))
            columns = [table_name + '.' + column for column in columns]
            result_dicts = []

            for row in result:
                row_dict = {}

                for col, val in zip(columns, row):
                    if type(val) == bytes:
                        val = val.decode('utf-8')

                    row_dict[f'{col}'] = val

                result_dicts.append(row_dict)

            return result_dicts, columns

    def get_data_and_project(self, table_name: str, column_names: List[str]):
        table_names = self.get_table_names()
        if table_name not in table_names:
            raise ValueError(f'Table {table_name} does not exist')

        column_string = ', '.join(['"{}"'.format(col) for col in column_names])
        with closing(self.conn.cursor()) as c:
            result = c.execute(f'SELECT {column_string} FROM {table_name}')
            columns = list(map(itemgetter(0), result.description))
            columns = [table_name + '.' + column for column in columns]
            result_dicts = []

            for row in result:
                row_dict = {}

                for col, val in zip(columns, row):
                    if type(val) == bytes:
                        val = val.decode('utf-8')

                    row_dict[f'{col}'] = val

                result_dicts.append(row_dict)

            return result_dicts, columns

    def execute_sql(self, sql_statement: str):
        cursor = self.conn.cursor()
        try:
            result = cursor.execute(sql_statement)
        except Exception as e:
            cursor.close()
            self.conn.commit()
            raise e

        columns = list(map(itemgetter(0), result.description))
        result_dicts = []

        for row in result:
            row_dict = {}

            for col, val in zip(columns, row):
                if type(val) == bytes:
                    val = val.decode('utf-8')

                row_dict[f'{col}'] = val

            result_dicts.append(row_dict)
        cursor.close()
        return result_dicts, columns


class SqliteConnectionPool(CuttlePool):
    CONNECTION_POOL_CAPACITY = 10

    # We keep one connection pool per database path
    _by_path = {}

    @staticmethod
    def for_path(db_path):
        """Creates or returns an existing connection pool for the SQLite
           database located at the given `db_path`."""
        if db_path not in SqliteConnectionPool._by_path:
            new_conn_pool = SqliteConnectionPool(
                factory=sqlite3.connect,
                capacity=SqliteConnectionPool.CONNECTION_POOL_CAPACITY,
                database=db_path
            )

            SqliteConnectionPool._by_path[db_path] = new_conn_pool

        return SqliteConnectionPool._by_path[db_path]

    def normalize_resource(self, resource):
        # To prevent decoding problems with the original mysql data,
        # see https://stackoverflow.com/questions/22751363
        resource.text_factory = bytes

    def ping(self, resource):
        """Checks if the given `resource` can still be used by executing a
           simple SQL query."""
        try:
            res = resource.execute('SELECT 1').fetchall()
            return (1,) in res
        except sqlite3.Error:
            return False
