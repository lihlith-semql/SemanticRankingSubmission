
from pathlib import Path
import json
import copy

import spacy

from semql.core.ast import Operation
from semql.core.parser import TreeParser

from semql.to_text.generator import generator
from semql.to_text.db_meta import DB_META_MAP
from semql.to_text.util import accept_tree

NLP = spacy.load("en_core_web_sm")


def main(src: Path, tgt: Path):
    if not tgt.exists():
        tgt.mkdir()

    src_data_name = src.name
    tgt_data_name = tgt.name

    db_meta = DB_META_MAP[src_data_name]

    for sample_tree_file in src.glob('*.json'):
        with sample_tree_file.open('r') as fin:
            sampled_data = json.load(fin)

        out_data = copy.deepcopy(sampled_data)

        for node in out_data['tree']:
            if 'data_source' in node['arguments']:
                node['arguments']['data_source'] = tgt_data_name

        tree: Operation = TreeParser(
            sampled_data['tree'],
            Operation.get_op_dict(),
        ).parse_dict()

        # skip if using any features that the generator cant handle
        if not accept_tree(tree, db_meta):
            continue

        generated = generator(tree, db_meta)
        tokens = [t.text for t in NLP(generated)]

        out_data['sample_id'] = out_data['_id']
        out_data['question'] = generated
        out_data['tokenized_question'] = tokens
        out_data['start_time_token'] = 0
        out_data['user_name_token'] = "tree2text_v3"
        out_data['user_name'] = "tree2text_v3"
        out_data['data_name'] = src_data_name
        out_data['elapsed_time'] = 0
        out_data['elapsed_time_token'] = 0
        out_data['tokens_assigned'] = False
        # no idea how to create these
        out_data['query'] = ""
        out_data['sql'] = ""
        out_data['query_toks'] = []

        with (tgt / sample_tree_file.name).open('w') as fout:
            json.dump(obj=out_data, fp=fout, indent=2)


if __name__ == '__main__':
    import sys
    try:
        in_path = sys.argv[1]
        out_path = sys.argv[2]
    except IndexError:
        print(
            "usage: python -m semql.to_text.scripts.generate_synthetic_annotations"
            " /path/to/sampled/trees/ /path/to/synthetic/out/")
        sys.exit(-1)
    main(Path(in_path), Path(out_path))
