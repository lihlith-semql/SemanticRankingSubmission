from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation


class StartFrom(VerbPhrase):
    def __init__(self, name):
        super(StartFrom, self).__init__(
            name=name,
            auxiliary="",
            participle="depart",
            preposition="from",
            is_default=False,

        )
        self.target_table = 'airports'

    def is_foreign_key(self):
        return True


class ArriveAt(VerbPhrase):
    def __init__(self, name):
        super(ArriveAt, self).__init__(
            name=name,
            auxiliary="",
            participle="arrive",
            preposition="at",
            is_default=False,

        )
        self.target_table = 'airports'

    def is_foreign_key(self):
        return True


FLIGHTS2_DB = {
    "tables": {
        "airlines": Table(
            name='airlines',
            pretty_name='airline',
            attributes={
                "uid": PrimaryKey(),
                "airline": Name(is_default=True),
                "abbreviation": Text(name='abbreviation'),
                "country": Text(name='country')
            },
        ),
        "airports": Table(
            name='airports',
            pretty_name='airport',
            attributes={
                "airportcode": Text(name='code'),
                "city": Text(name='city'),
                "airportname": Name(is_default=True),
                "country": Text(name='country'),
                "countryabbrev": Text(name="country abbreviation"),
            },
        ),
        "flights": PartialRelation(
            name='flights',
            pretty_name='flight',
            attributes={
                "airline": ForeignKey(name="airline", target_table="airlines"),
                "flightno": Numeric(name='flight number'),
                "sourceairport": StartFrom(name="source airport"),
                "destairport": ArriveAt(name="destination airport"),
            },
            templates={
                'sourceairport': {
                    'flights': ["$flights", "from", "$airports"],
                    'airports': ["$airports", "from which", "$flights", "depart"],
                },
                'destairport': {
                    'flights': ['$flights', 'to', '$airports'],
                    'airports': ['$airports', 'where', "$flights", "arrive"]
                },
                'airline': {
                    'airlines': ['$airlines', 'that host' ,'$flights'],
                    'flights': ['$flights', 'hosted by', '$airlines']
                },
            }
        ),
    },
}
