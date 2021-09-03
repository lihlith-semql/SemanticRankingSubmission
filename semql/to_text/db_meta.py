
from semql.to_text.db_metadata.moviedata import MOVIE_DB
from semql.to_text.db_metadata.chinook import CHINOOK_DB
from semql.to_text.db_metadata.flights_2 import FLIGHTS2_DB
from semql.to_text.db_metadata.car_1 import CAR1_DB
from semql.to_text.db_metadata.cre_doc_template_mgt import CREDOCTEMPLATE
from semql.to_text.db_metadata.student_transcripts_tracking import STUDENT_TRANSCRIPTS_TRACKING
from semql.to_text.db_metadata.dog_kennels import DOG_KENNEL
from semql.to_text.db_metadata.tvshow import TVSHOW_DB
from semql.to_text.db_metadata.wta_1 import WTA_DB
from semql.to_text.db_metadata.poker_player import POKER_PLAYER_DB
from semql.to_text.db_metadata.world_1 import WORLD_1_DB
from semql.to_text.db_metadata.pets_1 import PETS_1DB
from semql.to_text.db_metadata.battle_death import BATTLE_DEATH_DB
from semql.to_text.db_metadata.concert_singer import CONCERT_SINGER_DB
from semql.to_text.db_metadata.course_teach import COURSE_TEACH_DB
from semql.to_text.db_metadata.employee_hire_evaluation import EMPLOYEE_HIRE_EVALUATION_DB
from semql.to_text.db_metadata.museum_visit import MUSEUM_VISIT_DB
from semql.to_text.db_metadata.orchestra import ORCHESTRA_DB
from semql.to_text.db_metadata.singer import SINGER_DB
from semql.to_text.db_metadata.voter_1 import VOTER_1
from semql.to_text.db_metadata.real_estate_properties import REAL_ESTATE_PROPS

DB_META_MAP = {
    'moviedata': MOVIE_DB,
    'moviedata_beta': MOVIE_DB,
    'moviedata_synthetic': MOVIE_DB,
    'chinook_1': CHINOOK_DB,
    'chinook_synthetic': CHINOOK_DB,
    'driving_school': {},
    'college_2': {},
    'formula_1': {},
    'flight_2': FLIGHTS2_DB,
    'car_1': CAR1_DB,
    'cre_Doc_Template_Mgt': CREDOCTEMPLATE,
    'student_transcripts_tracking': STUDENT_TRANSCRIPTS_TRACKING,
    'dog_kennels': DOG_KENNEL,
    'tvshow': TVSHOW_DB,
    'wta_1': WTA_DB,
    'poker_player': POKER_PLAYER_DB,
    'world_1': WORLD_1_DB,
    'pets_1': PETS_1DB,
    'battle_death': BATTLE_DEATH_DB,
    'concert_singer': CONCERT_SINGER_DB,
    'course_teach': COURSE_TEACH_DB,
    'employee_hire_evaluation': EMPLOYEE_HIRE_EVALUATION_DB,
    'museum_visit': MUSEUM_VISIT_DB,
    'orchestra': ORCHESTRA_DB,
    'singer': SINGER_DB,
    'voter_1': VOTER_1,
    'real_estate_properties': REAL_ESTATE_PROPS,
}
