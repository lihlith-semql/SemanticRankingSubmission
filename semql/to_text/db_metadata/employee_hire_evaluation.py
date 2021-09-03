
from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

from semql.to_text.db_metadata.chinook import LiveIn

class YearAwarded(Attribute):

    def __init__(self):
        super(YearAwarded, self).__init__(
            attr_type="text", name="year", is_default=False)

    def is_primary_key(self):
        return True


EMPLOYEE_HIRE_EVALUATION_DB = {
    "tables": {
        "employee": Table(
            name="employee",
            attributes={
                "employee_id": PrimaryKey(),
                "name": Name(is_default=True),
                "age": Numeric(name="age"),
                "city": LiveIn(name="city"),
            }
        ),
        "shop": Table(
            name="shop",
            attributes={
                "shop_id": PrimaryKey(),
                "name": Name(is_default=True),
                "location": Text(name='location'),
                "district": Text(name="district"),
                "number_products": Numeric(name="number of products"),
                "manager_name": VerbPhrase(
                    name="manager name",
                    auxiliary="are",
                    participle="managed",
                    preposition="by",
                )
            }
        ),
        "hiring": RelationTable(
            name="hiring",
            attributes={
                "shop_id": ForeignKey(name="shop_id", target_table="shop"),
                "employee_id": ForeignKey(name="employee_id", target_table="employee"),
                "start_from": Literal(name="start from date"),
                "is_full_time": Literal(name="full time status"),
            },
            relation_components={'shop', 'employee'},
            templates={
                "shop": ["$shop", "who hired", "$employee", "$start_from", "$is_full_time"],
                "employee": ["$employee", "who was hired by", "$shop",
                             "$start_from", "$is_full_time"],
            },
            attr_templates={
                "start_from": ["starting from", "$filter"],
                "is_full_time": ["with full time status", "$filter"],
            }
        ),
        "evaluation": PartialRelation(
            name="evaluation",
            pretty_name="bonus",
            attributes={
                "year_awarded": YearAwarded(),
                "bonus": Numeric(name="amount"),
                "employee_id": ForeignKey(name="employee_id", target_table="employee"),
            },
            templates={
                "employee_id": {
                    "employee": ["$employee", "who were awarded", "$evaluation"],
                    "evaluation": ["$evaluation", "who were awarded to", "$employee"],
                }
            }
        )
    },
}
