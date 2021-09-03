from semql.to_text.attribute import *
from semql.to_text.table import Table, RelationTable, PartialRelation

WTA_DB = {
    "tables": {
        "players": Table(
            name='players',
            pretty_name='player',
            attributes={
                "*": Name(),
                "player_id": PrimaryKey(),
                "first_name": Text(name='first name'),
                "last_name": Text(name='last name'),
                "hand": Text(name='hand'),
                "country_code": Text(name='country code'),
                "birth_date": Date(name='birth date', participle='born'),
            },
        ),
        "matches": PartialRelation(
            name='matches',
            pretty_name='match',
            attributes={
                "best_of": Text(name='best of'),
                "draw_size": Numeric(name='draw size'),
                "loser_age": Numeric(name="loser age"),
                "loser_entry": Text(name="loser entry"),
                "loser_hand": Text(name="loser hand"),
                "loser_ht": Numeric(name="loser ht"),
                "loser_ioc": Text(name="loser ioc"),
                "loser_name": Text(name="loser name"),
                "loser_rank": Numeric(name="loser rank"),
                "loser_rank_points": Numeric(name="loser rank points"),
                "loser_seed": Numeric(name="loser seed"),
                "match_num": Numeric(name="match number"),
                "minutes": Numeric(name="minutes"),
                "round": Text(name="round"),
                "score": Text(name="score"),
                "surface": Text(name="surface"),
                "tourney_id": Text(name="tourney id"),
                "tourney_level": Text(name="tourney level"),
                "tourney_name": Text(name="tourney name"),
                "winner_age": Numeric(name="winner age"),
                "winner_entry": Text(name="winner entry"),
                "winner_hand": Text(name="winner hand"),
                "winner_ht": Numeric(name="winner ht"),
                "winner_ioc": Numeric(name="winner ioc"),
                "winner_name": Text(name="winner name"),
                "winner_rank": Numeric(name="winner rank"),
                "winner_rank_points": Numeric(name="winner rank points"),
                "winner_seed": Numeric(name="winner seed"),
                "year": Numeric(name="year"),
                "tourney_date": Date(name="tourney date", participle="played on"),
                "loser_id": ForeignKey(name="loser", target_table='players'),
                "winner_id": ForeignKey(name="winner", target_table='players'),
            },
            templates={
                'winner_id': {
                    'matches': ["$matches", "where", "$players", 'won'],
                    'players': ["$players", "that won", "$matches"],
                }, 'loser_id': {
                    'matches': ["$matches", "where", "$players", 'lost'],
                    'players': ["$players", "that lost", "$matches"],
                },
            }
        ),
        "rankings": PartialRelation(
            name='rankings',
            pretty_name='ranking',
            attributes={
                "ranking": Numeric(name="ranking"),
                "ranking_points": Numeric(name="ranking points"),
                "tours": Numeric(name="tours"),
                "ranking_date": Date(name='ranking date', participle='ranked'),
                "player_id": ForeignKey(name='player', target_table='players'),
            },
            templates={
                'player_id': {
                    'rankings': ["$rankings", "of", "$players"],
                    'players': ["$players", "ranked as", "$rankings"],
                }
            }
        ),
    }
}
