from contextlib import contextmanager
from copy import deepcopy
from ulta.common.agent import AgentInfo


class LabelContext:
    def __init__(self):
        self._labels: dict[str, str] = {}

    @property
    def labels(self):
        return self._labels

    @contextmanager
    def __call__(self, *, labels: dict[str, str] | None = None):
        with self._new_labels(labels):
            yield

    @contextmanager
    def _new_labels(self, labels: dict | None):
        if labels is None:
            yield
            return

        old_labels = deepcopy(self._labels)
        try:
            self._labels.update(labels)
            yield
        finally:
            self._labels = old_labels

    def agent(self, agent: AgentInfo):
        labels = {'agent_id': agent.id, 'agent_name': agent.name, 'agent_version': agent.version}
        return self(labels={k: '' if v is None else str(v) for k, v in labels.items()})
