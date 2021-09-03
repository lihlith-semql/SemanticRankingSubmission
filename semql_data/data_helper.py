import os

from typing import List

BASE_DATA_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__)
    )
)

BASE_PATH_SAMPLED_FILES = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'sampled_tree_files'
    )
)

BASE_PATH_CONFIG_FILES = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'config'
    )
)

BASE_PATH_FIXTURES = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'test_fixtures'
    )
)

BASE_PATH_ANNOTATED_FILES = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'annotated_tree_files'
    )
)

BASE_PATH_DATABASE_METADATA = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'database_metadata'
    )
)


def get_absolute_path_of_data_folder() -> str:
    return BASE_DATA_PATH


def get_path_to_db_file(db_name: str) -> str:
    """
    Returns the path to the folder where the dump of the specified database is stored.
    """
    db_name += '.db'
    db_path = os.path.join(
        BASE_DATA_PATH, 'database_files', 'sqllite', db_name
    )

    _check_if_path_exists(db_path)

    return db_path


def get_sampling_config_path_for_domain(domain_name: str):
    sampling_config_path = os.path.join(BASE_DATA_PATH, 'config', 'sampling_configs', domain_name)
    _check_and_create_path(sampling_config_path)
    return sampling_config_path


def get_metadata_filepath_for_db(db_name: str, metadata_file: str):
    """
    Returns the path to the folder where the specified metadata file of the given database is stored.
    """
    metadata_file_path = os.path.join(
        BASE_DATA_PATH, 'database_metadata', db_name, metadata_file
    )

    _check_if_path_exists(metadata_file_path)

    return metadata_file_path


def get_metadata_domain_names():
    metadata_path = os.path.join(BASE_DATA_PATH, 'database_metadata')
    _check_if_path_exists(metadata_path)

    return list(os.listdir(metadata_path))


def get_sampled_files_path_for_db(db_name: str) -> str:
    """
    Returns the path to the folder where the sampled files for the given database are stored.
    """
    path = os.path.join(
        BASE_PATH_SAMPLED_FILES, db_name
    )

    _check_if_path_exists(path)

    return path


def get_list_of_sampled_files_for_db(db_name: str) -> List[str]:
    """
    Returns a list of file names of the sampled files.
    """
    path = os.path.join(BASE_PATH_SAMPLED_FILES, db_name)

    _check_if_path_exists(path)

    return os.listdir(path)


def get_path_to_annotated_single_files(db_name: str):
    path = os.path.join(
        BASE_PATH_ANNOTATED_FILES, 'single_files', db_name
    )

    _check_if_path_exists(path)

    return path


def get_path_to_annotated_file_repo(db_name: str) -> str:
    file_name = f'default_{db_name}.json'

    path = os.path.join(
        BASE_PATH_ANNOTATED_FILES, file_name
    )

    _check_if_path_exists(path)

    return path


def get_config_file_path(file_name: str) -> str:
    """
    Returns the path to the folder where all the config files are stored.
    """
    path = os.path.join(
        BASE_PATH_CONFIG_FILES, file_name
    )

    _check_if_path_exists(path)

    return path


def get_task_sequence_file(subtask: int, seq_name, is_train=False) -> str:
    if is_train:
        path = os.path.join(
            BASE_PATH_CONFIG_FILES, 'test_sequences', 'subtask{}'.format(subtask), seq_name, 'train_support_id.txt'
        )
    else:
        path = os.path.join(
            BASE_PATH_CONFIG_FILES, 'test_sequences', 'subtask{}'.format(subtask), seq_name, 'test_id.txt'
        )

    _check_if_path_exists(path)

    return path


def get_task_config_path() -> str:
    path = os.path.join(
        BASE_PATH_CONFIG_FILES, 'task_config.json'
    )

    _check_if_path_exists(path)

    return path


def get_fixture_path() -> str:
    return BASE_PATH_FIXTURES


def _check_and_create_path(path: str):
    if not os.path.exists(path):
        os.mkdir(path)


def _check_if_path_exists(path: str):
    if not os.path.exists(path):
        raise ValueError('The following file does not exist:', path)
