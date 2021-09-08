from pathlib import Path
import numpy as np
from compute_scores import OttaLoader, cv_eval, SpiderLoader

if __name__ == '__main__':
    datasets = [
        ('spider', 'value_net'),
        ('spider', 'bridge'),
        ('moviedata', 'grammar_net'),
        ('chinook', 'grammar_net')
    ]

    for dataset, system in datasets:
        if dataset == 'spider':
            in_fname = f'outs/{dataset}/{system}/sem_sim_output.txt'
            norm_len = system == 'value_net'
            data = list(SpiderLoader(str(in_fname), rank_score=False, use_comp_eq=True, normalize_len=norm_len))
            accs = cv_eval(data, random_seed=0xdeadbeef, n_samples=20, console=False)
            print(f"{dataset}-{system}")
            for name, vals in accs.items():
                print(name, '\t', f"{np.mean(vals):.4f}")
            print()
        else:
            pattern = "outs/{}/grammar_net/{}/sem_sim_output_{}.txt"
            accuracies = {
                'confidence': [],
                'semantic': [],
                'equal': [],
                'calibrated': [],
                'learned': [],
                'threshold': [],
                'oracle-sem': [],
                'oracle': [],
            }
            for split in range(1, 11):
                for seed_ix in range(5):
                    p = Path(pattern.format(dataset, split, seed_ix))
                    if not p.exists():
                        continue
                    data = list(OttaLoader(str(p), rank_score=False, use_comp_eq=True))
                    accs = cv_eval(data, random_seed=0xdeadbeef, n_samples=20, console=False)
                    for name in accuracies.keys():
                        accuracies[name].append(np.mean(accs[name]))
            print(f"{dataset}-{system}")
            for name, vals in accuracies.items():
                print(name, '\t', f"{np.mean(vals):.4f}")
            print()
