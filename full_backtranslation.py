from pathlib import Path
from back_translation import BackTranslation

if __name__ == '__main__':
    datasets = [
        #('spider', 'value_net'),
        #('spider', 'bridge'),
        #('moviedata', 'grammar_net'),
        ('chinook', 'grammar_net')
    ]

    for dataset, system in datasets:
        if dataset == 'spider':
            in_fname = f'outs/{dataset}/{system}/raw_output.txt'
            out_fname = f'outs/{dataset}/{system}/back-translated_output.txt'
            bt = BackTranslation().back_translate(in_fname, out_fname, dataset)
        else:
            pattern = "outs/{}/grammar_net/{}/raw_output_{}.txt"
            pattern_out = "outs/{}/grammar_net/{}/back-translated_output_{}.txt"
            for split in range(1, 11):
                for seed_ix in range(5):
                    in_fname = Path(pattern.format(dataset, split, seed_ix))
                    out_fname = Path(pattern_out.format(dataset, split, seed_ix))
                    if not in_fname.exists():
                        continue
                    bt = BackTranslation().back_translate(in_fname, out_fname, dataset)