import time

import websockets

from plebstack import URI, URI_AUTH
from plebstack.models import OHLCReq, OHLCResponse, MsgMethods, SubUnsub, Intervals
from plebstack.parse import records


class Timer:
    def __init__(self, duration: int):
        self._start = time.time()
        self.duration = duration

    def __next__(self):
        if (delta := time.time() - self._start) > self.duration:
            return 0
        else:
            return delta


async def recv(interval: Intervals, duration: int):
    ohlc = OHLCReq(interval=interval)
    async with websockets.connect(URI, ssl=True) as ws:
        msg = SubUnsub(method=MsgMethods.subscribe, req_id=interval, params=ohlc)
        await ws.send(msg.model_dump_json())

        responses = []
        timer = Timer(duration)
        while next(timer):
            responses += [await ws.recv()]
        data = records(responses, OHLCResponse, {"wt_price": "vwap"})

        msg = SubUnsub(method=MsgMethods.unsubscribe, req_id=interval + 1, params=ohlc)
        await ws.send(msg.model_dump_json())

        return data
