#!/usr/bin/env python3

import asyncio

from fastapi import FastAPI

from byomod.lib.firehose import Firehose

APP: FastAPI = FastAPI()
FIREHOSE: Firehose = Firehose()


@APP.get('/metrics')
def get_metrics() -> dict[str, str]:
    data = FIREHOSE.counters['posts'].topk(5)
    return {'data': data}


async def main() -> None:
    await FIREHOSE.run()


if __name__ == '__main__':
    asyncio.run(main())
