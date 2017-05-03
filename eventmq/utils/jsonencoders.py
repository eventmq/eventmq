import json

from ..utils.timeutils import IntervalIter


class EventMQEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, IntervalIter):
            return o.__dict__
