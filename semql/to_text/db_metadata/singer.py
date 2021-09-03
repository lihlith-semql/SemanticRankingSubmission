
from semql.to_text.attribute import *
from semql.to_text.table import *

SINGER_DB = {
    "tables": {
        "singer": Table(
            name="singer",
            attributes={
                "singer_id": PrimaryKey(),
                "name": Name(is_default=True),
                "birth_year": Date(name="birth year", participle="born", preposition="in"),
                "net_worth_millions": Numeric(name="net worth", unit="million"),
                "citizenship": Text(name="home country"),
            }
        ),
        "song": PartialRelation(
            name="song",
            attributes={
                "song_id": PrimaryKey(),
                "title": Name(is_default=True),
                "singer_id": ForeignKey(name="singer_id", target_table="singer"),
                "sales": Numeric(name="number of units sold"),
                "highest_position": Numeric(name="highest position"),
            },
            templates={
                "singer_id": {
                    "singer": ["$singer", "who performed", "$song"],
                    "song": ["$song", "performed by", "$singer"],
                }
            }
        )
    },
}
