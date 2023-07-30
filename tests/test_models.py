import json

from plebstack.models import OHLCResponse

from .conftest import json_line, example_jl


def test_record():
    path = example_jl["raw"]
    for i, line in json_line(path):
        obj = json.loads(line)
        res = OHLCResponse.select(json.dumps(obj[2]))
        assert len(res.data) == 12

        if i == 0:  # snapshot: 12, update: 3
            res = OHLCResponse.select(json.dumps(obj[11]))
            assert len(res.data) == 1
            res = OHLCResponse.select(json.dumps(obj[12]))
            assert len(res.data) == 1
            res = OHLCResponse.select(json.dumps(obj[13]))
            assert len(res.data) == 1
        elif i == 1:  # snapshot: 12, update: 1
            res = OHLCResponse.select(json.dumps(obj[12]))
            assert len(res.data) == 1
