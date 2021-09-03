import argparse, json
from nubia.nubia import Nubia
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction

from tqdm import tqdm

nubia = Nubia()

def compute_semantic_similarity(in_fname, out_fname):
    with open(in_fname, 'rt', encoding='utf-8') as fin, open(out_fname, 'wt', encoding='utf-8') as fout:
        for line in tqdm(fin.readlines(), desc=f'SemSim for {in_fname}'):
            jdict = json.loads(line.replace('\n', ''))
            beams = jdict['beams']
            orig_question = jdict['beams'][0]['orig_question']

            for beam in beams:
                inferred_ot_str = beam['inferred_question']
                if inferred_ot_str is None or inferred_ot_str == '':
                    nubia_score, bleu_score = 0, 0
                else:
                    nubia_score = nubia.score(ref=orig_question, hyp=inferred_ot_str)
                    bleu_score = sentence_bleu([orig_question], inferred_ot_str, smoothing_function=SmoothingFunction().method3)
                beam['beam_nubia_score'] = nubia_score
                beam['beam_bleu_score'] = bleu_score
            fout.write(json.dumps(jdict) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', dest='dataset', type=str, required=True)
    parser.add_argument('-s', '--system', dest='system', type=str, required=True)
    args = parser.parse_args()
    dataset = args.dataset
    system = args.system

    in_fname = f'outs/{dataset}/{system}/back-translated_output.txt'
    out_fname = f'outs/{dataset}/{system}/sem_sim_output.txt'
    compute_semantic_similarity(in_fname, out_fname)