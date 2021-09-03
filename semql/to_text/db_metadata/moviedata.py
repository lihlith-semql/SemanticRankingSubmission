
from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable

MOVIE_DB = {
    "tables": {
        "person": Table(
            name='person',
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
                "gender": Enum(name='gender'),
                "birth_day": Date(name="birth day", participle="born"),
                "birth_place": Location(name="birth place", participle='born'),
                "death_day": Date(name="death day", participle="deceased"),
                "death_place": Location(name="death place", participle="deceased"),
            },
        ),
        "movie": Table(
            name='movie',
            attributes={
                "id": PrimaryKey(),
                "title": Name(is_default=True),
                "original_title": Text(name="original title"),
                "original_language": Enum(name='original language'),
                "budget": Numeric(name="budget", unit="dollar"),
                "revenue": Numeric(name='revenue', unit="dollar"),
                "homepage": Text(name="homepage"),
                "overview": Text(name="overview"),
                "popularity": Numeric(name='popularity'),
                "release_date": Date(name="release date", participle="released"),
                "runtime": Numeric(name="runtime", unit="minute"),
                "status": Text(name="status"),
                "tagline": Text(name="tag line"),
                "vote_average": Numeric(name="vote average"),
                "vote_count": Numeric(name="vote count"),
            },
        ),
        "cast": RelationTable(
            name='cast',
            attributes={
                # "id": PrimaryKey(),
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "person_id": ForeignKey(name="person_id", target_table="person"),
                "character": Name(),
                "order": Numeric(name="cast order"),
            },
            relation_components={'movie', 'person'},
            templates={
                'movie': [
                    "$movie", "starring", "$person", "$character", "$order"],
                'person': [
                    "$person", "who starred",
                    "$character", "$order", "in", "$movie"
                ],
            },
            attr_templates={
                "character": ["as characters", "$filter"],
                "order": ["$filter"],
            },
        ),
        "crew": RelationTable(
            name="crew",
            attributes={
                # "id": PrimaryKey(),
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "person_id": ForeignKey(name="person_id", target_table="person"),
                "department": Literal(name="department"),
                "job": Literal(name="job"),
            },
            relation_components={'movie', 'person'},
            templates={
                "movie": [
                    "$movie", "which had", "$person", "working",
                    "$job", "$department", "in them",
                ],
                "person": [
                    "$person", "who worked in",
                    "$movie", "$job", "$department",
                ],
            },
            attr_templates={
                "department": ["in the", "$filter", "department"],
                "job": ["as a", "$filter"],
            }
        ),
        "country": Table(
            name='country',
            attributes={
                "iso_3166_1": PrimaryKey(),
                "name": Name(is_default=True),
            },
        ),
        "production_country": RelationTable(
            name='production_country',
            pretty_name="producation country",
            attributes={
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "iso_3166_1": ForeignKey(name="iso_3166_1", target_table="country"),
            },
            relation_components={'country', 'movie'},
            templates={
                'country': [
                    "$country", "in which", "$movie", "were produced"],
                'movie': [
                    "$movie", "which were produced in", "$country"],
            },
        ),
        "company": Table(
            name='company',
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
            },
        ),
        "production_company": RelationTable(
            name='production_company',
            pretty_name="production company",
            attributes={
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "company_id": ForeignKey(name="company_id", target_table="company"),
            },
            relation_components={'movie', 'company'},
            templates={
                'movie': [
                    "$movie", "which were produced by", "$company"],
                'company': ["$company", "which produced", "$movie"],
            }
        ),
        "genre": Table(
            name="genre",
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
            },
        ),
        "has_genre": RelationTable(
            name="has_genre",
            attributes={
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "genre_id": ForeignKey(name="genre_id", target_table="genre"),
            },
            relation_components={'movie', 'genre'},
            templates={
                'movie': ["$movie", "with", "$genre"],
                'genre': ["$genre", "comprising", "$movie"],
            },
        ),
        "keyword": Table(
            name="keyword",
            attributes={
                "id": PrimaryKey(),
                "name": Name(is_default=True),
            }
        ),
        "has_keyword": RelationTable(
            name="has_keyword",
            attributes={
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "keyword_id": ForeignKey(name="keyword_id", target_table="keyword"),
            },
            relation_components={'movie', 'keyword'},
            templates={
                "movie": ["$movie", "with", "$keyword"],
                "keyword": ["$keyword", "for", "$movie"],
            },
        ),
        "language": Table(
            name="language",
            attributes={
                "iso_639_1": PrimaryKey(),
                "name": Name(is_default=True),
            }
        ),
        "spoken_language": RelationTable(
            name="spoken_language",
            pretty_name="spoken language",
            attributes={
                "iso_639_1": ForeignKey(name="iso_639_1", target_table="language"),
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
            },
            relation_components={'language', 'movie'},
            templates={
                "language": ["$language", "spoken in", "$movie"],
                "movie": ["$movie", "in which", "$language", "is spoken"],
            }
        ),
        "oscar": Table(
            name="oscar",
            attributes={
                "id": PrimaryKey(),
                "year": Date(name="year", participle="awarded"),
                "number": Numeric(name="number"),
                "category": Text(name="category", is_default=True),
                "sub_category": Text(name="sub-category"),
            },
        ),
        "oscar_nominee": RelationTable(
            name="oscar_nominee",
            pretty_name="oscar nominee",
            attributes={
                "oscar_id": ForeignKey(name="oscar_id", target_table="oscar"),
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "person_id": ForeignKey(name="person_id", target_table="person"),
            },
            relation_components={'oscar', 'movie', 'person'},
            templates={
                "oscar": [
                    "$oscar",  "for which", "$person",
                    "were nominated for their contribution to", "$movie",
                ],
                "movie": [
                    "$movie", "for which", "$person",
                    "were nominated for", "$oscar",
                ],
                "person": [
                    "$person", "who were nominated for", "$oscar",
                    "for their contribution to", "$movie",
                ]
            },
        ),
        "oscar_winner": RelationTable(
            name="oscar_winner",
            pretty_name="oscar winner",
            attributes={
                "oscar_id": ForeignKey(name="oscar_id", target_table="oscar"),
                "movie_id": ForeignKey(name="movie_id", target_table="movie"),
                "person_id": ForeignKey(name="person_id", target_table="person"),
            },
            relation_components={'oscar', 'movie', 'person'},
            templates={
                "oscar": [
                    "$oscar", "which", "$person",
                    "won for their contribution to", "$movie",
                ],
                "movie": [
                    "$movie", "for which", "$person", "won", "$oscar"],
                "person": [
                    "$person", "who won", "$oscar",
                    "for their contribution to", "$movie",
                ]
            },
        ),
    },
}
