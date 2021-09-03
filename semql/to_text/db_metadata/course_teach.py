
from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

COURSE_TEACH_DB = {
    "tables": {
        "course": Table(
            name="course",
            pretty_name="course",
            attributes={
                "course_id": PrimaryKey(),
                "starting_date": Text(name="starting date"),
                "course": Name(is_default=True)
            }
        ),
        "teacher": Table(
            name="teacher",
            pretty_name="teacher",
            attributes={
                "teacher_id": PrimaryKey(),
                "name": Name(is_default=True),
                "age": Text(name="age"),
                "hometown": Text(name="home town"),
            }
        ),
        "course_arrange": RelationTable(
            name="course_arrange",
            pretty_name="course arrange",
            attributes={
                "course_id": ForeignKey(name="course_id", target_table="course"),
                "teacher_id": ForeignKey(name="teacher_id", target_table="teacher"),
                "grade": Literal(name="grade"),
            },
            relation_components={'course', 'teacher'},
            templates={
                "course": ["$course", "arranged by", "$teacher", "$grade"],
                "teacher": ["$teacher", "who arranged", "$course", "$grade"],
            },
            attr_templates={
                "grade": ["in grade", "$filter"],
            },
        )
    },
}
