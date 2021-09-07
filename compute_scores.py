import json
import argparse

from utils.conversion_cache import ConvertorCache

from semql.core.ast import *

from utils.comparisons import comp_eq

from sklearn.model_selection import StratifiedKFold
from sklearn.linear_model import LogisticRegression

import numpy as np


def log_confidence_proxy(beam_ix: int, alpha: float = 0.8):
    return np.log(alpha)*(beam_ix+1)


def compute_complexity(tree: Operation):
    def node_score(t: Operation):
        if isinstance(t, GetData):
            return 1.
        elif isinstance(t, Filter):
            return .5
        elif any(isinstance(t, clz) for clz in [Min, Max, Average, Sum, Count]):
            return .5
        elif isinstance(t, ProjectionRoot):
            n_proj = len(t.attrs)
            return max(0, n_proj - 2) * 0.5
        else:
            return 0.

    if isinstance(tree, GetData):
        return node_score(tree)
    else:
        return node_score(tree) + sum(node_score(c) for c in tree.children)


class SpiderLoader:

    def __init__(self, eval_file, rank_score=False, use_comp_eq=True, normalize_len=False):
        with open(eval_file, 'r') as fin:
            samples = [json.loads(line) for line in fin]

        self.samples = samples
        self.cache = ConvertorCache()
        self.rank_score = rank_score
        self.comp_eq = use_comp_eq
        self.normalize_len = normalize_len

    def __iter__(self):
        for sample in self.samples:
            if sample.get('gold_sql2ot_fail', False) \
                    or sample.get('gold_ot3_fail', False):
                continue

            for ix, beam in enumerate(sample['beams']):
                beam['rank_score'] = log_confidence_proxy(ix)

            beams = [
                b
                for b in sample['beams']
                if (not b.get('beam_sql2ot_fail', False))
                and (not b.get('beam_ot3_fail', False))
            ]

            if len(beams) == 0:
                continue

            out = []

            if self.comp_eq:
                conv = self.cache.get(sample['db_name'])
                gold_ot = conv(beams[0]['correct_code'])
            else:
                conv = None
                gold_ot = None

            for b in beams:
                score = b['rank_score'] if self.rank_score else b['score']
                if self.normalize_len:
                    score /= len(b['inferred_code'])
                if self.comp_eq:
                    eq = comp_eq(gold_ot, conv(b['inferred_code']))
                else:
                    eq = b['is_correct']

                nubia = b['beam_nubia_score']
                code_len = len(b['inferred_code'])

                out.append({
                    'conf': np.exp(score),
                    'nubia': nubia,
                    'eq': eq,
                    'log_len': np.log(code_len),
                    'rank_score': np.exp(b['rank_score'])
                })

            yield out


class OttaLoader:

    def __init__(self, eval_file, rank_score=False, use_comp_eq=True):
        with open(eval_file, 'r') as fin:
            samples = [json.loads(line) for line in fin]

        self.samples = samples
        self.cache = ConvertorCache()
        self.rank_score = rank_score
        self.comp_eq = use_comp_eq

    def __iter__(self):
        for sample in self.samples:
            beams = sample['beams']
            out = []
            for ix, b in enumerate(beams):
                score = log_confidence_proxy(ix) if self.rank_score else b['score']
                eq = b['is_correct_ot']
                nubia = b['beam_nubia_score']
                code_len = len(b['inferred_code'])

                out.append({
                    'conf': np.exp(score),
                    'nubia': nubia,
                    'eq': eq,
                    'log_len': np.log(code_len),
                    'rank_score': np.exp(log_confidence_proxy(ix))
                })

            yield out


def select_top_1(beams):
    return beams[0]


def select_highest_conf(beams):
    return max(beams, key=lambda b: b['conf'])


def select_oracle(beams):
    return max(beams, key=lambda b: b['eq'])


def select_nubia(beams):
    return max(beams, key=lambda b: b['nubia'])


def naive_mixer(beams):
    return max(
        beams,
        key=lambda b: b['conf'] * b['nubia'],
    )


def count_correct(entries, selection_fn):
    return sum(map(lambda beam: beam['eq'], map(selection_fn, entries)))


class SelectiveMixer:
    def __init__(self, train_data):
        # self.threshold = 0.90
        highest_confs = [
            select_highest_conf(beams=beam_list)
            for beam_list in train_data
        ]
        all_confs = [
            b['conf']
            for b_lst in train_data
            for b in b_lst
        ]
        percentile = np.percentile(all_confs, 90)
        self.threshold = min(d['conf'] for d in highest_confs if d['eq'] and d['conf'] > percentile) - 0.01
        self.threshold = max(percentile, self.threshold)
        # print(percentile, self.threshold)

    def __call__(self, beam_list):
        if max(b['conf'] for b in beam_list) > self.threshold:
            return select_highest_conf(beam_list)
        else:
            return select_nubia(beam_list)


class OracleSelection:

    def __call__(self, beam_list):
        top_conf = select_highest_conf(beam_list)
        top_nubia = select_nubia(beam_list)

        if top_conf['eq']:
            return top_conf
        else:
            return top_nubia


class ScoreRegressor:

    def __init__(self, train_data):
        x_train = []
        y_train = []
        for beam_list in train_data:
            for beam in beam_list:
                features = [
                    beam['conf'],
                    beam['nubia'],
                    # beam['conf'] * beam['nubia'],
                    # beam['rank_score'],
                    # beam['log_len'],
                ]
                label = beam['eq']

                x_train.append(features)
                y_train.append(label)

        self.logreg = LogisticRegression(
            class_weight='balanced',
        ).fit(x_train, y_train)

    def __call__(self, beam_list):
        feats = []
        for beam in beam_list:
            features = [
                beam['conf'],
                beam['nubia'],
                # beam['conf'] * beam['nubia'],
                # beam['rank_score'],
                # beam['log_len'],
            ]
            feats.append(features)

        ps = self.logreg.predict_proba(feats)

        out_ix = np.argmax(ps[:, 1])

        return beam_list[out_ix]


class ScoreMixer:

    def __init__(self, train_data):
        train_scores = [[b['conf']] for lst in train_data for b in lst]
        train_nubia = [[b['nubia']] for lst in train_data for b in lst]
        labels = [b['eq'] for lst in train_data for b in lst]

        self.score_calib = LogisticRegression(
            class_weight='balance'
        ).fit(train_scores, labels)

        self.nubia_calib = LogisticRegression(
            class_weight='balance'
        ).fit(train_nubia, labels)

    def __call__(self, beam_list):
        scores = [[b['conf']] for b in beam_list]
        nubia = [[b['nubia']] for b in beam_list]

        calib_scores = self.score_calib.predict_proba(scores)[:, 1]
        calib_nubia = self.nubia_calib.predict_proba(nubia)[:, 1]

        mixed = calib_scores * calib_nubia
        # mixed = 1. - (1. - calib_scores)*(1. - calib_nubia)

        selected_ix = np.argmax(mixed)

        return beam_list[selected_ix]


def base_eval(data_iter):
    data = list(data_iter)
    print("FULL")
    print('correct top 1', count_correct(data, select_top_1))
    print('correct confidence', count_correct(data, select_highest_conf))
    print('correct nubia', count_correct(data, select_nubia))
    print('correct oracle', count_correct(data, select_oracle))
    print('correct naive mixer', count_correct(data, naive_mixer))
    print('total', len(data))


def cv_eval(data_iter, random_seed, n_samples=20, console=True):
    data = list(data_iter)
    skf = StratifiedKFold(
        n_splits=len(data) // n_samples,
        random_state=random_seed,
        shuffle=True,
    )
    cv_results = []
    oracle = [select_oracle(beams)['eq'] for beams in data]
    for test_ixs, train_ixs in skf.split(X=data, y=oracle):
        local_train = [e for ix, e in enumerate(data) if ix in train_ixs]
        local_test = [e for ix, e in enumerate(data) if ix in test_ixs]

        local_mixer = ScoreMixer(local_train)
        local_regressor = ScoreRegressor(local_train)
        local_selective = SelectiveMixer(local_train)
        upper_bound = OracleSelection()

        res = {
            'n_test': len(test_ixs),
            'n_train': len(train_ixs),
            'n_confidence': count_correct(local_test, select_top_1),
            'n_nubia': count_correct(local_test, select_nubia),
            'n_naive': count_correct(local_test, naive_mixer),
            'n_calib': count_correct(local_test, local_mixer),
            'n_clf': count_correct(local_test, local_regressor),
            'n_ifelse': count_correct(local_test, local_selective),
            'n_upper_bound': count_correct(local_test, upper_bound),
            'n_oracle': count_correct(local_test, select_oracle),
        }

        cv_results.append(res)

    accuracies = {
        'confidence': [e['n_confidence'] / e['n_test'] for e in cv_results],
        'semantic': [e['n_nubia'] / e['n_test'] for e in cv_results],
        'equal': [e['n_naive'] / e['n_test'] for e in cv_results],
        'calibrated': [e['n_calib'] / e['n_test'] for e in cv_results],
        'learned': [e['n_clf'] / e['n_test'] for e in cv_results],
        'threshold': [e['n_ifelse'] / e['n_test'] for e in cv_results],
        'oracle-sem': [e['n_upper_bound'] / e['n_test'] for e in cv_results],
        'oracle': [e['n_oracle'] / e['n_test'] for e in cv_results],
    }

    if console:
        print(f"name\tmean acc.\tstddev acc.")
        for name, vs in accuracies.items():
            print(f"{name}\t{np.mean(vs):.4f}\t{np.std(vs):.4f}")

    return accuracies


def main(eval_file, corpus, use_comp_eq, use_rank_score, n_samples, seed, len_norm):
    corpus = corpus.lower()
    if corpus == 'spider':
        loader = SpiderLoader(
            eval_file=eval_file,
            rank_score=use_rank_score,
            use_comp_eq=use_comp_eq,
            normalize_len=len_norm
        )
    elif corpus == 'otta':
        loader = OttaLoader(
            eval_file=eval_file,
            rank_score=use_rank_score,
            use_comp_eq=use_comp_eq,
        )
    else:
        raise ValueError(
            f"unknown corpus '{corpus}', use one of ['spider', 'otta']")

    data = list(loader)

    base_eval(data)
    print()
    cv_eval(data, random_seed=seed, n_samples=n_samples)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', dest='file', type=str, required=True)
    parser.add_argument(
        '-C', '--corpus', dest='corpus', type=str, default='spider')
    parser.add_argument(
        '--comp_eq', dest='use_comp_eq', type=bool, default=True)
    parser.add_argument(
        '--rank_score', dest='use_rank_score', default=False)
    parser.add_argument(
        '-N', '--n_samples', dest='n_samples', type=int, default=20)
    parser.add_argument(
        '--seed', dest='seed', type=lambda x: int(x, 0), default=0xdeadbeef)
    parser.add_argument(
        '--len_norm', dest='len_norm', type=bool, default=True)

    args = parser.parse_args()

    main(
        eval_file=args.file,
        corpus=args.corpus,
        use_comp_eq=args.use_comp_eq,
        use_rank_score=args.use_rank_score,
        n_samples=args.n_samples,
        seed=args.seed,
        len_norm=args.len_norm
    )
