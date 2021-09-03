from typing import Dict


def dict_merge(source: Dict, destination: Dict) -> Dict:
    """
    Merges the source and destination dicts and returns the merged dict. This
    means that the values from `source` are set in `destination` before it is
    returned.

    Source is from https://stackoverflow.com/questions/20656135.

    :return: The merged dict
    """
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            dict_merge(value, node)
        else:
            destination[key] = value

    return destination
