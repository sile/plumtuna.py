import argparse
import optuna
import plumtuna
from random import random
import time

def objective(trial):
    for i in range(0,10):
        trial.suggest_uniform('param_{}'.format(i), 0.0, 1.0)

    for i in range(0, 100):
        time.sleep(1)
        v = random()
        trial.report(v, i)
        if trial.should_prune(i):
            raise optuna.structs.TrialPruned("v={}, step={}".format(v, i))

    return v

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, help = "timeout", default=60, required=False)
    parser.add_argument("--study", type=str, help = "study name", default="foo", required=False)
    parser.add_argument("--contact", type=str, help = "plumtuna contact host", default="localhost", required=False)
    parser.add_argument("--storage", type=str, help = "storage URL", default=None, required=False)
    args = parser.parse_args()

    if args.storage is None:
        storage = plumtuna.PlumtunaStorage(contact_host=args.contact)
    else:
        storage = args.storage

    study = optuna.create_study(
        study_name=args.study,
        load_if_exists=True,
        storage=storage,
        pruner=optuna.pruners.SuccessiveHalvingPruner()
    )
    study.optimize(objective, timeout=args.timeout)
    print('Number of finished trials: {}'.format(len(study.trials)))

    print('Best trial:')
    trial = study.best_trial

    print('  Value: {}'.format(trial.value))

    print('  Params: ')
    for key, value in trial.params.items():
        print('    {}: {}'.format(key, value))
