from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

class WithEpisode(VerbPhrase):
    def __init__(self, name):
        super(WithEpisode, self).__init__(
            name=name,
            auxiliary="contain",
            participle="the",
            preposition="episode",
            is_default=False,
        )


TVSHOW_DB = {
    "tables": {
        "tv_channel": Table(
            name='tv_channel',
            pretty_name='tv channel',
            attributes={
                "id": PrimaryKey(),
                "series_name": Text(name='series name'),
                "country": Text(name='country'),
                "language": Text(name='language'),
                "content": Text(name='content'),
                "pixel_aspect_ratio_par": Text(name='aspect ratio'),
                "hight_definition_tv": Text(name='high definition status'),
                "pay_per_view_ppv": Text(name='pay per view status'),
                "package_option": Text(name='package option'),
            },
        ),
        "tv_series": PartialRelation(
            name='tv_series',
            pretty_name='TV series',
            attributes={
                "id": PrimaryKey(),
                "episode": WithEpisode(name='episode'),
                "rating": Numeric(name='rating'),
                "share": Numeric(name='share'),
                "18_49_rating_share": Numeric(name='share of 18 to 49 years old'),
                "viewers_m": Numeric(name='male viewers'),
                "weekly_rank": Numeric(name='weekly rank'),
                "air_date": Date(name='airdate', participle='aired'),
                "channel": ForeignKey(name='channel', target_table='tv_channel'),
            },
            templates={
                'channel': {
                    'tv_channel': ["$tv_channel", "which air", "$tv_series"],
                    'tv_series': ["$tv_series", "aired on", "$tv_channel"],
                }
            }
        ),
        "cartoon": PartialRelation(
            name='cartoon',
            pretty_name='cartoon',
            attributes={
                "id": PrimaryKey(),
                "title": Text(name='title'),
                "directed_by": Text(name='director'),
                "written_by": Text(name='writer'),
                "production_code": Numeric(name='production code'),
                "original_air_date": Date(name='airdate', participle='aired'),
                "channel": ForeignKey(name='channel', target_table='tv_channel'),
            },
            templates={
                'channel': {
                    'tv_channel': ["$tv_channel", "which air", "$cartoon"],
                    'cartoon': ["$cartoon", "aired by", "$tv_channel"],
                }
            }
        ),
    },
}
