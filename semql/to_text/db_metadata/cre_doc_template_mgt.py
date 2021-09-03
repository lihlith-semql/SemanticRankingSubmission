from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

CREDOCTEMPLATE = {
    "tables": {
        "ref_template_types": Table(
            name='ref_template_types',
            pretty_name='template type',
            attributes={
                "template_type_code": Text(name='code', is_default=True),
                "template_type_description": Text(name='description')
            },
        ),
        "templates": PartialRelation(
            name='templates',
            pretty_name='template',
            attributes={
                "template_id": PrimaryKey(),
                "version_number": Numeric(name='version number'),
                "date_effective_from": Date(
                    name="from", participle="effective", preposition="from", auxiliary="are"),
                "date_effective_to": Date(
                    name="to", participle="effective", preposition="to", auxiliary="are"),
                "template_details": Text(name='details'),
                "template_type_code": ForeignKey(name="template type code", target_table='ref_template_types')
            },
            templates={
                'template_type_code': {
                    'templates': ['$templates', 'with', '$ref_template_types'],
                    'ref_template_types': ['$ref_template_types', 'for', '$templates']
                }
            }
        ),
        "documents": PartialRelation(
            name='documents',
            pretty_name='document',
            attributes={
                "document_id": PrimaryKey(),
                "document_name": Name(is_default=True),
                "document_description": Text(name='description'),
                "other_details": Text(name='details'),
                "template_id": ForeignKey(name="template", target_table='templates')
            },
            templates={
                'template_id': {
                    'templates': ['$templates', 'used for', '$documents'],
                    'documents': ['$documents', 'that use', '$templates']
                }
            }
        ),
        "paragraphs": PartialRelation(
            name='paragraphs',
            pretty_name='paragraph',
            attributes={
                "paragraph_id": PrimaryKey(),
                "paragraph_text": Text(name='text', is_default=True),
                "other_details": Text(name='details'),
                "document_id": ForeignKey(name="document", target_table='documents')
            },
            templates={
                'document_id': {
                    'paragraphs': ['$paragraphs', 'in', '$documents'],
                    'documents': ['$documents', 'that contain', '$paragraphs']
                }
            }
        ),
    },
}
