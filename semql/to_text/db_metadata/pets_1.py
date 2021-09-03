from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

PETS_1DB = {
    "tables": {
        "student": Table(
            name='student',
            pretty_name='student',
            attributes={
                "stuid": PrimaryKey(),
                "lname": Text(name='last name'),
                "fname": Text(name='first name'),
                "age": Numeric(name='age'),
                "sex": Numeric(name='gender'),
                "major": Numeric(name='major id'),
                "advisor": Numeric(name='advisor id'),
                "city_code": Text(name='city code'),
            },
        ),
        "has_pet": RelationTable(
            name='has_pet',
            pretty_name='pet',
            attributes={
                "stuid": ForeignKey(name="student id", target_table='student'),
                "petid": ForeignKey(name="pet id", target_table='pets'),
            },
            relation_components={"student", "pets"},
            templates={
                "student": ["$student", "who own", "$pets"],
                "pet": ["$pets", "owned by", "$student"]
            }
        ),
        "pets": Table(
            name='pets',
            pretty_name='pet',
            attributes={
                "petid": PrimaryKey(),
                "pettype": Text(name="type"),
                "pet_age": Numeric(name="age"),
                "weight": Numeric(name="weight"),
            }
        ),
    },
}
