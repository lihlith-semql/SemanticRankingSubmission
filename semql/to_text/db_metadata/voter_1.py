
from semql.to_text.table import *
from semql.to_text.attribute import *


class AreaCode(Numeric):

    def __init__(self):
        super(AreaCode, self).__init__(name="area code", unit=None, is_default=False)

    def is_primary_key(self):
        return True


VOTER_1 = {
    "tables": {
        "area_code_state": Table(
            name="area_code_state",
            pretty_name="state",
            attributes={
                "state": Text(name="state code"),
                "area_code": AreaCode(),
            }
        ),
        "contestants": Table(
            name="contestants",
            pretty_name="contestant",
            attributes={
                "contestant_number": PrimaryKey(),
                "contestant_name": Name(is_default=True),
            }
        ),
        "votes": PartialRelation(
            name="votes",
            pretty_name="vote",
            attributes={
                "vote_id": PrimaryKey(),
                "phone_number": Text(name="phone number"),
                "state": ForeignKey(name="state code", target_table="area_code_state"),
                "contestant_number": ForeignKey(name="contestant_number", target_table="contestants"),
                "created": Date(name="creation date", participle="created", preposition="at"),
            },
            templates={
                "state": {
                    "area_code_state": ["$area_code_state", "in which", "$votes", "were cast"],
                    "votes": ["$votes", "cast in", "$area_code_state"],
                },
                "contestant_number": {
                    "contestants": ["$contestants", "who received", "$votes"],
                    "votes": ["$votes", "cast for", "$contestants"],
                },
            }
        )
    },
}
