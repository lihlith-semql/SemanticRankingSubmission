
import json
from pathlib import Path

from semql.core.ast import Operation
from semql.core.parser import TreeParser
from semql.data_sources import SqliteDataSource

from semql.to_text.db_meta import DB_META_MAP
from semql.to_text.util import accept_tree


SEMQL_DATA = Path(__file__).parents[2] / 'semql_data'


class DataHelper(object):

    def __init__(self, db_name: str, data_folder: Path = SEMQL_DATA):
        self.db_name = db_name

        all_trees = []
        for tree_file in (
            data_folder / 'annotated_tree_files' / 'single_files' / self.db_name
        ).glob('*.json'):
            with open(tree_file, 'r') as fin:
                all_trees.append(
                    json.load(fin)
                )

        for tree in all_trees:
            tree['parsed'] = TreeParser(
                tree['tree'], Operation.get_op_dict()).parse_dict()
            
        db_path = data_folder / 'database_files' / 'sqllite' / f"{self.db_name}.db"

        self.data_source = SqliteDataSource(config={'db_path': str(db_path)})
        self.db_meta = DB_META_MAP[self.db_name]
        self.trees = [
            tree
            for tree in all_trees
            if accept_tree(tree['parsed'], self.db_meta)
        ]
