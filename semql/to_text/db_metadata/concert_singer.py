
from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

CONCERT_SINGER_DB = {
    "tables": {
        "stadium": Table(
            name="stadium",
            pretty_name="stadium",
            attributes={
                "stadium_id": PrimaryKey(),
                "location": Text(name="location"),
                "name": Name(is_default=True),
                "capacity": Numeric(name="capacity", unit="person"),
                "hightest": Numeric(name="highest number of visitors"),
                "lowest": Numeric(name="lowest number of visitors"),
                "average": Numeric(name="average number of visitors"),
            }
        ),
        "singer": Table(
            name="singer",
            pretty_name="singer",
            attributes={
                "singer_id": PrimaryKey(),
                "name": Name(is_default=True),
                "country": Text(name="home country"),
                "song_name": VerbPhrase(
                    name="most famous song",
                    auxiliary="are",
                    participle="famous",
                    preposition="for singing"
                ),
                "song_release_year": Date(
                    name="release year",
                    auxiliary="were",
                    participle="released",
                    preposition="in",
                ),
                "age": Numeric(name="age", unit="year"),
                "is_male": Text(name="gender"),
            }
        ),
        "concert": PartialRelation(
            name="concert",
            pretty_name="concert",
            attributes={
                "concert_id": PrimaryKey(),
                "concert_name": Name(is_default=True),
                "theme": Text(name="theme"),
                "year": Text(name="year"),
                "stadium_id": ForeignKey("stadium_id", target_table="stadium"),
            },
            templates={
                "stadium_id": {
                    "stadium": ["$stadium", "hosting", "$concert"],
                    "concert": ["$concert", "hosted in", "$stadium"],
                }
            },
        ),
        "singer_in_concert": RelationTable(
            name='singer_in_concert',
            pretty_name="singer in concert",
            attributes={
                "concert_id": ForeignKey(name="concert_id", target_table="concert"),
                "singer_id": ForeignKey(name="singer_id", target_table="singer"),
            },
            relation_components={'singer', 'concert'},
            templates={
                'concert': [
                    "$concert", "in which", "$singer", "performed"],
                'singer': [
                    "$singer", "who performed in", "$concert"],
            },
        ),
    },
}
