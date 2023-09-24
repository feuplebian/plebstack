import asyncio
from itertools import chain

from rich.console import Console

from plebstack.execution import recv
from plebstack.models import Intervals

console = Console()


async def main():
    tasks = [recv(i, 60 * 10) for i in (Intervals.min_05, Intervals.min_01)]
    data = list(chain.from_iterable(await asyncio.gather(*tasks)))
    with open("raw-dump.jsonl", mode="a") as jsonfile:
        jsonfile.write(f"[{','.join([i.model_dump_json() for i in data])}]\n")
    return data


if __name__ == "__main__":
    recs = asyncio.run(main())
    console.print(recs)
