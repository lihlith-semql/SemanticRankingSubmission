from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

STUDENT_TRANSCRIPTS_TRACKING = {
    "tables": {
        "addresses": Table(
            name='addresses',
            pretty_name='address',
            attributes={
                "address_id": PrimaryKey(),
                "line_1": Text(name='first line'),
                "line_2": Text(name='second line'),
                "line_3": Text(name='third line'),
                "city": Text(name='city'),
                "zip_postcode": Text(name='zip code'),
                "state_province_county": Text(name='state province county'),
                "country": Text(name='country'),
                "other_address_details": Text(name='details'),
            },
        ),
        "courses": Table(
            name='courses',
            pretty_name='course',
            attributes={
                "course_id": PrimaryKey(),
                "course_name": Text(name='name'),
                "course_description": Text(name='description'),
                "other_details": Text(name='details'),
            },
        ),
        "transcripts": Table(
            name='transcripts',
            pretty_name='transcript',
            attributes={
                "transcript_id": PrimaryKey(),
                "transcript_date": Date(name='date', participle='released'),
                "other_details": Text(name='details'),
            },
        ),
        "departments": Table(
            name='departments',
            pretty_name='department',
            attributes={
                "department_id": PrimaryKey(),
                "department_name": Text(name='name'),
                "department_description": Text(name='description'),
                "other_details": Text(name='details'),
            },
        ),
        "semesters": Table(
            name='semesters',
            pretty_name='semester',
            attributes={
                "semester_id": PrimaryKey(),
                "semester_name": Text(name='name'),
                "semester_description": Text(name='description'),
                "other_details": Text(name='details'),
            },
        ),
        "degree_programs": PartialRelation(
            name='degree_programs',
            pretty_name='program',
            attributes={
                "*": Name(),
                "degree_program_id": PrimaryKey(),
                "degree_summary_name": Text(name='summary name'),
                "degree_summary_description": Text(name='summary description'),
                "other_details": Text(name='details'),
                "department_id": ForeignKey(name="department", target_table='departments')
            },
            templates={
                'department_id': {
                    'departments': ['$departments', 'with', '$degree_programs'],
                    'degree_programs': ['$degree_programs', 'for', '$departments']
                }
            }
        ),
        "sections": PartialRelation(
            name='sections',
            pretty_name='section',
            attributes={
                "section_id": PrimaryKey(),
                "section_name": Text(name='name'),
                "section_description": Text(name='description'),
                "other_details": Text(name='details'),
                "course_id": ForeignKey(name="course", target_table='courses')
            },
            templates={
                'course_id': {
                    'courses': ['$courses', 'in', '$sections'],
                    'sections': ['$sections', 'with', '$courses']
                }
            }
        ),
        "students": PartialRelation(
            name='students',
            pretty_name='student',
            attributes={
                "student_id": PrimaryKey(),
                "first_name": Text(name='first name'),
                "middle_name": Text(name='middle name'),
                "last_name": Text(name='last name'),
                "cell_mobile_number": Text(name='mobile number'),
                "email_address": Text(name='email address'),
                "ssn": Text(name='ssn'),
                "date_first_registered": Date(name='registration date', participle='registered'),
                "date_left": Date(name='exmatriculation date', participle='left'),
                "other_student_details": Text(name='details'),
                "current_address_id": ForeignKey(name="current address", target_table='addresses'),
                "permanent_address_id": ForeignKey(name="permanent address", target_table='addresses')
            },
            templates={
                'current_address_id': {
                    'addresses': ['$addresses', 'where', '$students', 'currently live in'],
                    'students': ['$students', 'that currently live in', '$addresses']
                },
                'permanent_address_id': {
                    'addresses': ['$addresses', 'of', '$students'],
                    'students': ['$students', 'with', '$addresses']
                }
            }
        ),
        "student_enrolment": RelationTable(
            name='student_enrolment',
            pretty_name='enrolled',
            attributes={
                "student_enrolment_id": PrimaryKey(),
                "other_details": Text(name='details'),
                "degree_program_id": ForeignKey(name="degree program", target_table='degree_programs'),
                "semester_id": ForeignKey(name="semester", target_table='semesters'),
                "student_id": ForeignKey(name="student", target_table='students')
            },
            relation_components={'semesters', 'students', 'degree_programs'},
            templates={
                'degree_programs': ['$degree_programs', 'for which', '$students', 'in the', '$semesters', 'enrolled'],
                'students': ['$students', 'enrolled', 'for', '$degree_programs', 'in', '$semesters'],
                'semesters': ['$semesters', 'in which', '$students', 'enrolled', 'for', '$degree_programs'],
            }
        ),
        "student_enrolment_courses": PartialRelation(
            name='student_enrolment_courses',
            pretty_name='students enrolled',
            attributes={
                "student_course_id": PrimaryKey(),
                "student_enrolment_id": ForeignKey(name="enrolment", target_table='student_enrolment'),
                "course_id": ForeignKey(name="course", target_table='courses'),
            },
            templates={
                'course_id': {
                    'courses': ['$courses', 'for which', '$student_enrolment_courses'],
                    'student_enrolment_courses': ['$student_enrolment_courses', 'in', '$courses'],
                },
                'student_enrolment_id': {
                    'student_enrolment': ['$student_enrolment', 'for', '$student_enrolment_courses'],
                    'student_enrolment_courses': ['$student_enrolment_courses', 'by', '$student_enrolment'],
                }
            }
        ),
        "transcript_contents": RelationTable(
            name='transcript_contents',
            pretty_name='transcript content',
            attributes={
                "student_course_id": ForeignKey(name="course enrollemnt", target_table='student_enrolment_courses'),
                "transcript_id": ForeignKey(name="transcrpt", target_table='transcripts'),
            },
            relation_components={'student_enrolment_courses', 'transcripts'},
            templates={
                'transcripts': ['$transcripts', 'made in', '$student_enrolment_courses'],
                'student_enrolment_courses': ['$student_enrolment_courses', 'for which', '$transcripts', 'where made']
            }
        )
    },
}
