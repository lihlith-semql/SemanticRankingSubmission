from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation


POKER_PLAYER_DB = {
    "tables": {
        "people": Table(
            name='people',
            pretty_name='person',
            attributes={
                "people_id": PrimaryKey(),
                "nationality": Text(name='nationality'),
                "name": Name(is_default=True),
                "birth_date": Date(name='birth date', participle='born'),
                "height": Numeric(name='height'),
            },
        ),
        "poker_player": PartialRelation(
            name='poker_player',
            pretty_name='poker player',
            attributes={
                "poker_player_id": PrimaryKey(),
                "final_table_made": Numeric(name='final table earning'),
                "best_finish": Numeric(name='best finish'),
                "money_rank": Numeric(name='money rank'),
                "earnings": Numeric(name='earning'),
                "people_id": ForeignKey(name='persons', target_table='people'),
            },
            templates={
                'people_id': {
                    'poker_player': ["$poker_player", "that are", "$people"],
                    'people': ["$people", "that are", "$poker_player"],
                }
            }
        ),
    },
}
