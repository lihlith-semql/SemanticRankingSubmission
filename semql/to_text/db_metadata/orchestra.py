
from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

ORCHESTRA_DB = {
    "tables": {
        "conductor": Table(
            name="conductor",
            attributes={
                "conductor_id": PrimaryKey(),
                "name": Name(is_default=True),
                "age": Numeric(name='age'),
                "nationality": Text(name="nationality"),
                "year_of_work": Numeric(name="career length", unit='year'),
            }
        ),
        "orchestra": PartialRelation(
            name="orchestra",
            attributes={
                "orchestra_id": PrimaryKey(),
                "orchestra": Name(is_default=True),
                "record_company": Text(name="record company"),
                "year_of_founded": Date(name="founding year", participle="founded", preposition="in"),
                "major_record_format": Text(name="major record format"),
                "conductor_id": ForeignKey(name="conductor_id", target_table="conductor"),
            },
            templates={
                "conductor_id": {
                    "conductor": ["$conductor", "who conduct", "$orchestra"],
                    "orchestra": ["$orchestra", "conducted by", "$conductor"],
                }
            }
        ),
        "performance": PartialRelation(
            name="performance",
            attributes={
                "performance_id": PrimaryKey(),
                "orchestra_id": ForeignKey(name="orchestra_id", target_table="orchestra"),
                "type": Text(name="type"),
                "date": Date(name="date", participle="shown", preposition="on"),
                "official_ratings_(millions)": Numeric(name="official rating", unit="million"),
                "weekly_rank": Numeric(name="weekly rank"),
                "share": Text(name="share"),
            },
            templates={
                "orchestra_id": {
                    "orchestra": ["$orchestra", "who gave", "$performance"],
                    "performance": ["$performance", "given by", "$orchestra"],
                }
            }
        ),
        "show": PartialRelation(
            name="show",
            attributes={
                "show_id": PrimaryKey(),
                "performance_id": ForeignKey(name="performance_id", target_table="performance"),
                "if_first_show": Text(name="first show status"),
                "result": Text(name="result"),
                "attendance": Numeric(name="attendance", unit='person'),
            },
            templates={
                "performance_id": {
                    "performance": ["$performance", "who took place during", "$show"],
                    "show": ["$show", "where", "$performance", "took place"],
                }
            }
        )
    },
}
