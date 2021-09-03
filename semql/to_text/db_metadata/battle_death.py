
from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

BATTLE_DEATH_DB = {
    "tables": {
        "battle": Table(
            name="battle",
            pretty_name="battle",
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
                "date": Date(
                    name="date",
                    participle="fought",
                    preposition="in",
                    auxiliary="were",
                ),
                "bulgarian_commander": Text(name="bulgarian commander"),
                "latin_commander": Text(name="latin commander"),
                "result": Text(name="result"),
            },
        ),
        "ship": PartialRelation(
            name="ship",
            pretty_name="ship",
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
                "tonnage": Text(name="tonnage"),
                "ship_type": Text(name="type"),
                "location": Text(name="location"),
                "disposition_of_ship": Text(name="disposition"),
                "lost_in_battle": ForeignKey(
                    name="lost_in_battle", target_table="battle"),
            },
            templates={
                "lost_in_battle": {
                    "ship": ["$ship", "that were lost in", "$battle"],
                    "battle": ["$battle", "where", "$ship", "were lost"],
                }
            },
        ),
        "death": PartialRelation(
            name="death",
            pretty_name="casualty count",
            attributes={
                "id": PrimaryKey(),
                "note": Text(name="note"),
                "killed": Numeric(name="number of killed", unit="person"),
                "injured": Numeric(name="number of injured", unit="person"),
                "caused_by_ship_id": ForeignKey(
                    name="lost_by_ship_id", target_table="ship"),
            },
            templates={
                "caused_by_ship_id": {
                    "ship": ["$ship", "responsible for", "$death"],
                    "death": ["$death", "caused by", "$ship"],
                }
            },
        ),
    },
}
