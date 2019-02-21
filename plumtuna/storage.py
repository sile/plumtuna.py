import copy
from datetime import datetime
import json
from optuna import distributions  # NOQA
from optuna.storages import base
from optuna.storages.base import DEFAULT_STUDY_NAME_PREFIX
from optuna import structs
import requests
import time
import threading
from typing import Any  # NOQA
from typing import Dict  # NOQA
from typing import List  # NOQA
from typing import Optional  # NOQA
import urllib.parse
import urllib.request
import uuid

from plumtuna import PlumtunaServer

class PlumtunaStorage(base.BaseStorage):
    def __init__(self, bind_addr=None, bind_port=None, contact_host=None, contact_port=None):
        self.server = PlumtunaServer(bind_addr, bind_port, contact_host, contact_port)

        # TODO
        time.sleep(1)

        self.http_host = '127.0.0.1'
        self.http_port = self.server.http_port
        self.studies = {}
        self._lock = threading.Lock()

    @property
    def rpc_addr(self):
        return self.server.rpc_addr

    @property
    def rpc_port(self):
        return self.server.rpc_port

    def _get(self, path):
        res = requests.get('http://{}:{}{}'.format(self.http_host, self.http_port, path))
        assert res.status_code is 200, '{}: {}'.format(path, res.text)
        return res.json()

    def _post(self, path, body=None):
        if body is None:
            res = requests.post('http://{}:{}{}'.format(self.http_host, self.http_port, path))
        else:
            res = requests.post('http://{}:{}{}'.format(self.http_host, self.http_port, path),
                                data=json.dumps(body))
        assert res.status_code is 200, '{}: {}'.format(path, res.text)
        return res.json()

    def _post2(self, path, body=None):
        if body is None:
            res = requests.post('http://{}:{}{}'.format(self.http_host, self.http_port, path))
        else:
            res = requests.post('http://{}:{}{}'.format(self.http_host, self.http_port, path),
                                data=json.dumps(body))
        return res.status_code, res.json()

    def _put(self, path, body):
        res = requests.put('http://{}:{}{}'.format(self.http_host, self.http_port, path), data=json.dumps(body))
        assert res.status_code is 200, '{}: {}'.format(path, res.text)
        return res.json()

    def _subscribe(self, study_id, study_name):
        with self._lock:
            if study_id not in self.studies:
                subscribe_id = self._post('/studies/{}/subscribe'.format(study_id))
                self.studies[study_id] = StudyState(study_id, study_name, subscribe_id)

    def _poll(self, study_id):
        with self._lock:
            subscribe_id = self.studies[study_id].subscribe_id
            messages = self._get('/studies/{}/subscribe/{}'.format(study_id, subscribe_id))
            for m in messages:
                self.studies[study_id].handle_message(m)

    def _study_id(self, trial_id):
        return trial_id.split('.')[0]

    def create_new_study_id(self, study_name=None):
        # type: (Optional[str]) -> int

        if study_name is None:
            study_uuid = str(uuid.uuid4())
            study_name = DEFAULT_STUDY_NAME_PREFIX + study_uuid

        status, res = self._post2('/studies', {'study_name': study_name})
        if status == 409:
            raise structs.DuplicatedStudyError

        study_id = res['study_id']
        self._subscribe(study_id, study_name)
        return study_id

    def set_study_user_attr(self, study_id, key, value):
        # type: (int, str, Any) -> None

        self._put('/studies/{}/user_attrs/{}'.format(study_id, urllib.parse.quote_plus(key)), value)

    def set_study_direction(self, study_id, direction):
        # type: (int, structs.StudyDirection) -> None

        if direction == structs.StudyDirection.NOT_SET:
            d = "NOT_SET"
        elif direction == structs.StudyDirection.MINIMIZE:
            d = "MINIMIZE"
        else:
            d = "MAXIMIZE"

        self._put('/studies/{}/direction'.format(study_id), d)

    def set_study_system_attr(self, study_id, key, value):
        # type: (int, str, Any) -> None

        self._put('/studies/{}/system_attrs/{}'.format(study_id, urllib.parse.quote_plus(key)), value)

    # Basic study access

    def get_study_id_from_name(self, study_name):
        # type: (str) -> int

        res = self._get('/study_names/{}'.format(study_name))
        study_id = res['study_id']
        self._subscribe(study_id, study_name)
        return study_id

    def get_study_name_from_id(self, study_id):
        # type: (int) -> str

        # res = self._get('/studies/{}'.format(study_id))
        # return res['study_name']
        return self.studies[study_id].study_name

    def get_study_direction(self, study_id):
        # type: (int) -> structs.StudyDirection

        self._poll(study_id)
        return self.studies[study_id].direction
        # d = self._get('/studies/{}/direction'.format(study_id))
        # if d == 'NOT_SET':
        #     return structs.StudyDirection.NOT_SET
        # elif d == 'MINIMIZE':
        #     return structs.StudyDirection.MINIMIZE
        # else:
        #     return structs.StudyDirection.MAXIMIZE

    def get_study_user_attrs(self, study_id):
        # type: (int) -> Dict[str, Any]

        self._poll(study_id)
        return copy.deepcopy(self.studies[study_id].user_attrs)
        # return self._get('/studies/{}/user_attrs'.format(study_id))

    def get_study_system_attrs(self, study_id):
        # type: (int) -> Dict[str, Any]

        self._poll(study_id)
        return copy.deepcopy(self.studies[study_id].system_attrs)
        # return self._get('/studies/{}/system_attrs'.format(study_id))

    def get_all_study_summaries(self):
        # type: () -> List[structs.StudySummary]

        self._poll(study_id)
        return [s.summary() for s in self.studies.values()]
        # return self._get('/studies')

    # Basic trial manipulation

    def create_new_trial_id(self, study_id):
        # type: (int) -> int

        return self._post('/studies/{}/trials'.format(study_id))

    def set_trial_state(self, trial_id, state):
        # type: (int, structs.TrialState) -> None

        s = trial_state_to_str(state)
        self._put('/trials/{}/state'.format(trial_id), s)

    def set_trial_param(self, trial_id, param_name, param_value_internal, distribution):
        # type: (int, str, float, distributions.BaseDistribution) -> bool

        self._put('/trials/{}/params/{}'.format(trial_id, param_name),
                  {'value': param_value_internal,
                   'distribution': distributions.distribution_to_json(distribution)})
        return True

    def get_trial_param(self, trial_id, param_name):
        # type: (int, str) -> float

        study_id = self._study_id(trial_id)
        self._poll(study_id)
        return self.studies[study_id].trials[trial_id].trial_params[param_name]

        # return self._get('/trials/{}/params/{}'.format(trial_id, param_name))

    def set_trial_value(self, trial_id, value):
        # type: (int, float) -> None

        self._put('/trials/{}/value'.format(trial_id), value)

    def set_trial_intermediate_value(self, trial_id, step, intermediate_value):
        # type: (int, int, float) -> bool

        self._put('/trials/{}/intermediate_values/{}'.format(trial_id, step), intermediate_value)
        return True

    def set_trial_user_attr(self, trial_id, key, value):
        # type: (int, str, Any) -> None

        self._put('/trials/{}/user_attrs/{}'.format(trial_id, urllib.parse.quote_plus(key)), value)

    def set_trial_system_attr(self, trial_id, key, value):
        # type: (int, str, Any) -> None

        self._put('/trials/{}/system_attrs/{}'.format(trial_id, urllib.parse.quote_plus(key)), value)

    # Basic trial access

    def get_trial(self, trial_id):
        # type: (int) -> structs.FrozenTrial

        study_id = self._study_id(trial_id)
        self._poll(study_id)
        return copy.deepcopy(self.studies[study_id].trials[trial_id])
        # return dict_to_trial(self._get('/trials/{}'.format(trial_id)))

    def get_all_trials(self, study_id):
        # type: (int) -> List[structs.FrozenTrial]

        self._poll(study_id)
        return [copy.deepcopy(t) for t in self.studies[study_id].trials.values()]

        # return [dict_to_trial(t) for t in self._get('/studies/{}/trials'.format(study_id))]

    def get_n_trials(self, study_id, state=None):
        # type: (int, Optional[structs.TrialState]) -> int

        # TODO
        if state is None:
            return self._get('/studies/{}/n_trials'.format(study_id))
        else:
            return self._get('/studies/{}/n_trials?state={}'.format(study_id, trial_state_to_str(state)))


def trial_state_to_str(state):
    if state is structs.TrialState.RUNNING:
        return 'RUNNING'
    elif state is structs.TrialState.COMPLETE:
        return 'COMPLETE'
    elif state is structs.TrialState.PRUNED:
        return 'PRUNED'
    else:
        return 'FAIL'

def str_to_trial_state(s):
    if s == 'RUNNING':
        return structs.TrialState.RUNNING
    elif s == 'COMPLETE':
        return structs.TrialState.COMPLETE
    elif s == 'PRUNED':
        return structs.TrialState.PRUNED
    else:
        return structs.TrialState.FAIL

def dict_to_trial(d):
    params = {}
    params_in_internal_repr = {}
    for k,v in d['params'].items():
        distribution = distributions.json_to_distribution(v['distribution'])
        params[k] = distribution.to_external_repr(v['value'])
        params_in_internal_repr[k] = v['value']

    return structs.FrozenTrial(
        trial_id=d['trial_id'],
        state=str_to_trial_state(d['state']),
        params=params,
        user_attrs=d['user_attrs'],
        system_attrs=d['system_attrs'],
        value=d['value'],
        intermediate_values=dict((int(k),v) for k,v in d['intermediate_values'].items()),
        params_in_internal_repr=params_in_internal_repr,
        datetime_start=datetime.fromtimestamp(d['datetime_start']),
        datetime_complete=datetime.fromtimestamp(d['datetime_end']) if d['datetime_end'] else None,
    )

class StudyState(object):
    def __init__(self, study_id, study_name, subscribe_id):
        self.study_id = study_id
        self.study_name = study_name
        self.subscribe_id = subscribe_id
        self.trials = {}
        self.direction = structs.StudyDirection.NOT_SET
        self.user_attrs = {}
        self.system_attrs = {}

    def handle_message(self, message):
        kind, v = next(iter(message.items()))
        if kind == 'SetStudyDirection':
            d = v['direction']
            if d == 'NOT_SET':
                self.direction = structs.StudyDirection.NOT_SET
            elif d == 'MINIMIZE':
                self.direction = structs.StudyDirection.MINIMIZE
            else:
                self.direction = structs.StudyDirection.MAXIMIZE
        elif kind == 'SetStudyUserAttr':
            self.user_attrs[v['key']] = v['value']
        elif kind == 'SetStudySystemAttr':
            self.system_attrs[v['key']] = v['value']
        elif kind == 'CreateTrial':
            t = self._trial(v['trial_id'])
            self.trials[t.trial_id] = t._replace(datetime_start=datetime.fromtimestamp(v['timestamp']['secs']))  # TODO: nanos
        elif kind == 'SetTrialState':
            t = self._trial(v['trial_id'])
            t = t._replace(state=str_to_trial_state(v['state']))
            if v['state'] != 'RUNNING':
                t = t._replace(datetime_complete=datetime.fromtimestamp(v['timestamp']['secs']))  # TODO: nanos
            self.trials[t.trial_id] = t
        elif kind == 'SetTrialParam':
            t = self._trial(v['trial_id'])
            distribution = distributions.json_to_distribution(v['value']['distribution'])
            t.params[v['key']] = distribution.to_external_repr(v['value']['value'])
            t.params_in_internal_repr[v['key']] = v['value']['value']
        elif kind == 'SetTrialValue':
            t = self._trial(v['trial_id'])
            self.trials[t.trial_id] = t._replace(value=v['value'])
        elif kind == 'SetTrialIntermediateValue':
            t = self._trial(v['trial_id'])
            t.intermediate_values[v['step']] = v['value']
        elif kind == 'SetTrialUserAttr':
            t = self._trial(v['trial_id'])
            t.user_attrs[v['key']] = v['value']
        elif kind == 'SetTrialSystemAttr':
            t = self._trial(v['trial_id'])
            t.system_attrs[v['key']] = v['value']
        else:
            raise NotImplementedError(str(message))

    def _trial(self, trial_id):
        if trial_id in self.trials:
            return self.trials[trial_id]

        trial = structs.FrozenTrial(
            trial_id=trial_id,
            state=structs.TrialState.RUNNING,
            params={},
            user_attrs={},
            system_attrs={},
            value=None,
            intermediate_values={},
            params_in_internal_repr={},
            datetime_start=None,
            datetime_complete=None
        )
        self.trials[trial_id] = trial
        return trial
