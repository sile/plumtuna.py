import argparse
from collections import defaultdict
import optuna
import plumtuna
from random import random
import time

def objective(trial):
    for i in range(0,10):
        trial.suggest_uniform('param_{}'.format(i), 0.0, 1.0)

    v = random()
    for i in range(0, 100):
        time.sleep(1)
        trial.report(v, i)
        if trial.should_prune(i):
            raise optuna.structs.TrialPruned("v={}, step={}".format(v, i))

    return v

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, help = "timeout", default=60, required=False)
    parser.add_argument("--study", type=str, help = "study name", default="foo", required=False)
    parser.add_argument("--port", type=int, help = "plumtuna bind port", default=None, required=False)
    parser.add_argument("--contact", type=str, help = "plumtuna contact host", default="localhost", required=False)
    parser.add_argument("--storage", type=str, help = "storage URL", default=None, required=False)
    args = parser.parse_args()

    if args.storage is None:
        storage = plumtuna.PlumtunaStorage(bind_port=args.port, contact_host=args.contact)
    else:
        storage = args.storage

    study = optuna.create_study(
        study_name=args.study,
        load_if_exists=True,
        storage=storage,
        sampler=optuna.samplers.RandomSampler(),
        pruner=optuna.pruners.SuccessiveHalvingPruner()
    )
    study.optimize(objective, timeout=args.timeout)

    pruned_trials = [t for t in study.trials if t.state == optuna.structs.TrialState.PRUNED]
    complete_trials = [t for t in study.trials if t.state == optuna.structs.TrialState.COMPLETE]
    print('Study statistics: ')
    print('  Number of finished trials: ', len(study.trials))
    print('  Number of pruned trials: ', len(pruned_trials))
    print('  Number of complete trials: ', len(complete_trials))

    steps = defaultdict(int)
    losses = defaultdict(float)
    for t in pruned_trials:
        last_step = max(t.intermediate_values.keys())
        steps[last_step] += 1
        losses[last_step] += t.intermediate_values[last_step]

    print('Pruned statistics:')
    for step, count in sorted(list(steps.items())):
        print("  step[{}]: \tpruned_count={}, \tpruned_loss_avg={}".format(
            step, count, losses[step] / count))
