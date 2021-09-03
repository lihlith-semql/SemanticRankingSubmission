import argparse, json
from semql.core.ast import Operation
from semql.to_text.db_meta import DB_META_MAP
from semql.to_text.generator import generator
from semql.from_sql.process_sql import get_schemas_from_json, Schema, get_sql
from semql.from_sql.convert_json_to_OT import Converter
from semql_data.data_helper import get_metadata_filepath_for_db
from utils.comparisons import comp_eq
from utils.parse_ot_str import translate_str_to_OT

from tqdm import tqdm


class BackTranslation:

    def __init__(self):
        self.schemas, self.db_names, self.tables = get_schemas_from_json("tables.json")
        self.all_attributes_for_entity_type = {}

    def str_to_ot(self, in_str, db_name, dataset):
        if dataset == 'spider':
            return self.convert_sql(in_str, db_name)
        else:
            ret_ot = translate_str_to_OT(in_str, db_name)
            assert ret_ot.print() == in_str
            return ret_ot


    def convert_sql(self, sql_statement, db_name):
        schema = self.schemas[db_name]
        table = self.tables[db_name]
        schema = Schema(schema, table)
        try:
            sql_label = get_sql(schema, sql_statement)
            c = Converter(schema, db_name, self.all_attributes_for_entity_type[db_name])
            ot = c(sql_label)
            return ot
        except Exception as e:
            return None


    def ot_to_text(self, ot: Operation, db_name):
        try:
            gold_ot = generator(ot, DB_META_MAP[db_name])
            return gold_ot
        except Exception as e:
            return None


    def add_attribute_meta(self, db_name):
        if db_name not in self.all_attributes_for_entity_type.keys():
            with open(get_metadata_filepath_for_db(db_name, 'attributes_for_entity_type.json'), 'rt',
                      encoding='utf-8') as ifile:
                self.all_attributes_for_entity_type[db_name] = json.load(ifile)


    def back_translate(self, in_fname, out_fname, dataset):
        with open(in_fname, 'rt', encoding='utf-8') as fin, open(out_fname, 'wt', encoding='utf-8') as fout:
            for line in tqdm(fin.readlines(), desc=f'Backtranslation for {in_fname}'):
                jdict = json.loads(line.replace('\n', ''))

                beams = jdict['beams']
                db_name = jdict['db_name']
                self.add_attribute_meta(db_name)
                correct_code = beams[0]['correct_code']
                ot = self.str_to_ot(correct_code, db_name, dataset)
                if ot is not None:
                    jdict['gold_sql2ot_fail'] = False
                else:
                    continue
                gold_ot_str = self.ot_to_text(ot, db_name)
                if gold_ot_str is not None:
                    jdict['gold_ot3_fail'] = False
                else:
                    continue

                for beam in beams:
                    inferred_code = beam['inferred_code']
                    inferred_ot = self.str_to_ot(inferred_code, db_name, dataset)
                    if inferred_ot is None:
                        #cannot convert SQL
                        beam['is_correct_ot'] = False
                        beam['inferred_question'] = ""
                        beam['beam_sql2ot_fail'] = True
                    else:
                        beam['beam_sql2ot_fail'] = False
                        comp_eq_score = comp_eq(ot, inferred_ot)
                        inferred_ot_str = self.ot_to_text(inferred_ot, db_name)
                        if inferred_ot_str is None:
                            beam['beam_ot3_fail'] = True
                        else:
                            beam['beam_ot3_fail'] = False

                        beam['is_correct_ot'] = bool(comp_eq_score)
                        beam['inferred_question'] = inferred_ot_str
                fout.write(json.dumps(jdict) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', dest='dataset', type=str, required=True)
    parser.add_argument('-s', '--system', dest='system', type=str, required=True)
    args = parser.parse_args()
    dataset = args.dataset
    system = args.system

    #in_fname = f'outs/{dataset}/{system}/raw_output.txt'
    #out_fname = f'outs/{dataset}/{system}/back-translated_output.txt'

    in_fname = 'outs/moviedata/grammar_net/6/raw_output_0.txt'
    out_fname = 'outs/moviedata/grammar_net/6/back-translated_output.txt'

    bt = BackTranslation().back_translate(in_fname, out_fname, dataset)


