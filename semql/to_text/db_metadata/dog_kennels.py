from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation


class GoingThroughTreatment(VerbPhrase):
    def __init__(self, name):
        super(GoingThroughTreatment, self).__init__(
            name=name,
            auxiliary="that",
            participle="went",
            preposition="through",
            is_default=False,

        )
        self.target_table = 'dogs'

    def is_foreign_key(self):
        return True


DOG_KENNEL = {
    "tables": {
        "breeds": Table(
            name='breeds',
            pretty_name='breed',
            attributes={
                "breed_code": PrimaryKey(),
                "breed_name": Name(is_default=True),
            },
        ),
        "charges": Table(
            name='charges',
            pretty_name='charge',
            attributes={
                "charge_id": PrimaryKey(),
                "charge_type": Text(name='type'),
                "charge_amount": Numeric(name='amount')
            },
        ),
        "sizes": Table(
            name='sizes',
            pretty_name='size',
            attributes={
                "size_code": PrimaryKey(),
                "size_description": Text(name='description'),
            },
        ),
        "treatment_types": Table(
            name='treatment_types',
            pretty_name='treatment type',
            attributes={
                "treatment_type_code": PrimaryKey(),
                "treatment_type_description": Text(name='description'),
            },
        ),
        "owners": Table(
            name='owners',
            pretty_name='owner',
            attributes={
                "owner_id": PrimaryKey(),
                "first_name": Text(name='first name'),
                "last_name": Text(name='last name'),
                "street": Text(name='street name'),
                "city": Text(name='city'),
                "state": Text(name='state'),
                "zip_code": Text(name='zip code'),
                "email_address": Text(name='email address'),
                "home_phone": Text(name='home phone number'),
                "cell_number": Text(name='cell phone number'),
            },
        ),
        "professionals": Table(
            name='professionals',
            pretty_name='professional',
            attributes={
                "professional_id": PrimaryKey(),
                "first_name": Text(name='first name'),
                "last_name": Text(name='last name'),
                "street": Text(name='street name'),
                "city": Text(name='city'),
                "state": Text(name='state'),
                "zip_code": Text(name='zip code'),
                "role_code": Text(name='role'),
                "email_address": Text(name='email address'),
                "home_phone": Text(name='home phone number'),
                "cell_number": Text(name='cell phone number'),
            },
        ),
        "dogs": PartialRelation(
            name='dogs',
            pretty_name='dog',
            attributes={
                "dog_id": PrimaryKey(),
                "abandoned_yn": Text(name='abandoned status'),
                "name": Name(is_default=True),
                "age": Numeric(name='age'),
                "gender": Text(name='gender'),
                "weight": Numeric(name='weight'),
                "date_of_birth": Date(name='birthdate', participle='born'),
                "date_arrived": Date(name='date of arrival', participle='arrived'),
                "date_adopted": Date(name='date of adoption', participle='adopted'),
                "date_departed": Date(name='date of departure', participle='departed'),
                "owner_id": ForeignKey(name="owner", target_table='owners'),
                "breed_code": ForeignKey(name="breed", target_table='breeds'),
                "size_code": ForeignKey(name="size", target_table='sizes'),
            },
            templates={
                'owner_id': {
                    'owners': ['$owners', 'of', '$dogs'],
                    'dogs': ['$dogs', 'owned by', '$owners']
                },
                'breed_code': {
                    'breeds': ['$breeds', 'to which', '$dogs', 'belong to'],
                    'dogs': ['$dogs', 'of', '$breeds']
                },
                'size_code': {
                    'size_code': ['$size_code', 'to which', '$dogs', 'belong to'],
                    'dogs': ['$dogs', 'of', '$size_code']
                },
            }
        ),
        "treatments": PartialRelation(
            name='treatments',
            pretty_name='treatment',
            attributes={
                "treatment_id": PrimaryKey(),
                "cost_of_treatment": Numeric(name='cost'),
                "dog_id": GoingThroughTreatment(name='dog'),
                "professional_id": ForeignKey(name='professional', target_table='professionals'),
                "treatment_type_code": ForeignKey(name='treatment type', target_table='treatment_types'),
                "date_of_treatment": Date(name='date of treatment', participle='treated'),
            },
            templates={
                'professional_id': {
                    'professionals': ['$professionals', 'that performs', '$treatments'],
                    'treatments': ['$treatments', 'performed by', '$professionals']
                },
                'treatment_type_code': {
                    'treatment_type_code': ['$treatment_type_code', 'of', '$treatments'],
                    'treatments': ['$treatments', 'of type', '$treatment_type_code']
                },
                'dog_id': {
                    'treatments': ['$treatments', 'for', '$dogs'],
                    'dogs': ['$dogs', 'treated', '$treatments']
                },
            }
        ),
    },
}
