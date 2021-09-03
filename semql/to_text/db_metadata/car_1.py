from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation


CAR1_DB = {
    "tables": {
        "continents": Table(
            name='continents',
            pretty_name='continent',
            attributes={
                "contid": PrimaryKey(),
                "continent": Name(is_default=True)
            },
        ),
        "countries": PartialRelation(
            name='countries',
            pretty_name='country',
            attributes={
                "countryid": PrimaryKey(),
                "countryname": Name(is_default=True),
                "continent": ForeignKey(name="continent", target_table='continents')
            },
            templates={
                'continent': {
                    'continents': ['$continents', 'which contain', '$countries'],
                    'countries': ['$countries', 'that lie in', '$continents']
                }
            }
        ),
        "car_makers": PartialRelation(
            name='car_makers',
            pretty_name='car maker',
            attributes={
                "id": PrimaryKey(),
                "maker": Name(is_default=True),
                "fullname": Text(name='full name'),
                "country": ForeignKey(name="country", target_table='countries')
            },
            templates={
                'country': {
                    'car_makers': ['$car_makers', 'that lie in', '$countries'],
                    'countries': ['$countries', 'in which', '$car_makers', 'are located']
                }
            }
        ),
        "model_list": PartialRelation(
            name='model_list',
            pretty_name='model',
            attributes={
                "modelid": PrimaryKey(),
                "model": Name(is_default=True),
                "maker": ForeignKey(name="maker", target_table='car_makers')
            },
            templates={
                'maker': {
                    'car_makers': ['$car_makers', 'that offer', '$model_list'],
                    'model_list': ['$model_list', 'offered by', '$car_makers']
                }
            }
        ),
        "car_names": PartialRelation(
            name='car_names',
            pretty_name='car',
            attributes={
                "makeid": PrimaryKey(),
                "make": Name(is_default=True),
                "model": ForeignKey(name="model", target_table='model_list')
            },
            templates={
                'model': {
                    'car_names': ['$car_names', 'in', '$model_list'],
                    'model_list': ['$model_list', 'that contain', '$car_names']
                }
            }
        ),
        "cars_data": PartialRelation(
            name='cars_data',
            pretty_name='data sheet',
            attributes={
                "mpg": Numeric(name='mileage', unit='mpg'),
                "cylinders": Numeric(name='number of cylinder'),
                "edispl": Numeric(name='edispl'),
                "horsepower": Numeric(name='horsepower'),
                "weight": Numeric(name='weight', unit='lb'),
                "accelerate": Numeric(name='acceleration'),
                "year": Date(name='year', participle='built', preposition='in'),
                "id": ForeignKey(name="car name", target_table='car_names')
            },
            templates={
                'id': {
                    'car_names': ['$car_names', 'with', '$cars_data'],
                    'cars_data': ['$cars_data', 'for', '$car_names']
                }
            }
        )
    },
}
