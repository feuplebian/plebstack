import json

from rich.console import Console
import websockets

from plebstack import URI, URI_AUTH
from plebstack.models import OHLCReq, OHLCResponse, MsgMethods, SubUnsub
from plebstack.parse import records

console = Console()


async def main():
    ohlc = OHLCReq(interval=1)

    async with websockets.connect(URI, ssl=True) as ws:
        msg = SubUnsub(method=MsgMethods.subscribe, req_id=1, params=ohlc)
        await ws.send(msg.model_dump_json())
        responses = [await ws.recv() for _ in range(20)]
        data = records(responses, OHLCResponse, {"wt_price": "vwap"})

        msg = SubUnsub(method=MsgMethods.unsubscribe, req_id=2, params=ohlc)
        await ws.send(msg.model_dump_json())

        # with open("raw-dump.jsonl", mode="a") as jsonfile:
        #     jsonfile.write(f"[{','.join([i.model_dump_json() for i in data])}]\n")

        return data


if __name__ == "__main__":
    import asyncio

    recs = asyncio.run(main())
    console.print(recs)
