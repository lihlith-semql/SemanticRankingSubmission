from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

WORLD_1_DB = {
    "tables": {
        "country": Table(
            name='country',
            pretty_name='country',
            attributes={
                "code": PrimaryKey(),
                "name": Name(is_default=True),
                "continent": Text(name='continent'),
                "region": Text(name='region'),
                "surfacearea": Numeric(name='surface area'),
                "indepyear": Numeric(name='year of independence'),
                "population": Numeric(name='population'),
                "lifeexpectancy": Numeric(name='life expectancy'),
                "gnp": Numeric(name='gnp'),
                "gnpold": Numeric(name='old gnp'),
                "localname": Text(name='local name'),
                "governmentform": Text(name='form of government'),
                "headofstate": Text(name='head of state'),
                "capital": Numeric(name='capital'),
                "code2": Text(name='code2'),
            },
        ),
        "countrylanguage": PartialRelation(
            name='countrylanguage',
            pretty_name='language',
            attributes={
                "language": Text(name='language'),
                "isofficial": Text(name='official status'),
                "percentage": Numeric(name="speaker percentage"),
                "countrycode": ForeignKey(name="country", target_table='country'),
            },
            templates={
                'countrycode': {
                    'countrylanguage': ["$countrylanguage", "spoken in", "$country"],
                    'country': ["$country", "where", "$countrylanguage", 'are spoken'],
                }
            }
        ),
        "city": PartialRelation(
            name='city',
            pretty_name='city',
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
                "district": Text(name="district"),
                "population": Numeric(name="population"),
                "countrycode": ForeignKey(name="country", target_table='country'),
            },
            templates={
                'countrycode': {
                    'city': ["$city", "that lies in", "$country"],
                    'country': ["$country", "where", "$city", 'are located'],
                }
            }
        ),
    },
}
