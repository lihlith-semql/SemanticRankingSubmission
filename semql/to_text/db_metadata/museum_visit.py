
from semql.to_text.attribute import *
from semql.to_text.table import Table, PartialRelation, RelationTable

MUSEUM_VISIT_DB = {
    "tables": {
        "museum": Table(
            name="museum",
            attributes={
                "museum_id": PrimaryKey(),
                "name": Name(is_default=True),
                "num_of_staff": Numeric(name="staff count", unit='person'),
                "open_year": Date(
                    name="opening year", participle="opened", preposition='in'),
            }
        ),
        "visitor": Table(
            name="visitor",
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
                "level_of_membership": Numeric(name="membership level"),
                "age": Numeric(name="age"),
            }
        ),
        "visit": PartialRelation(
            name="visit",
            attributes={
                "num_of_ticket": Numeric(name="ticket number"),
                "total_spent": Numeric(name="amount spent"),
                "museum_id": ForeignKey(name="museum_id", target_table="museum"),
                "visitor_id": ForeignKey(name="visitor_id", target_table="visitor"),
            },
            templates={
                "museum_id": {
                    "museum": ["$museum", "in which", "$visit", "took place"],
                    "visit": ["$visit", "to", "$museum"],
                },
                "visitor_id": {
                    "visitor": ["$visitor", "who made", "$visit"],
                    "visit": ["$visit", "by", "$visitor"],
                },
            },
        )
    },
}
