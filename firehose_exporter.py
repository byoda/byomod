#!/usr/bin/env python3

import asyncio

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from byomod.lib.firehose import Firehose

from byomod.lib.counters import ANNOTATION_SEPARATOR

APP: FastAPI = FastAPI()
FIREHOSE: Firehose = Firehose()


@APP.get('/metrics')
def get_metrics(record_type: str | None = None, max_records: int = 100
                ) -> PlainTextResponse:
    record_types: list[str]
    if record_type:
        record_types = [record_type]
    else:
        record_types = FIREHOSE.counters.keys()

    metrics: str = ''
    rec_type: str
    for rec_type in record_types:
        data: list[tuple[str, int]] = FIREHOSE.counters[rec_type].topk(
            max_records, reset_incremental_access=True
        )
        for key, value in data:
            annotations: list[str] = [f'did="{key}"']
            if rec_type == 'likes':
                cid: str
                url: str
                cid, url = key.split(ANNOTATION_SEPARATOR)
                annotations = [f'cid="{cid}"', f'url="{url}"']

            metric: str = (
                f'byomod_firehose_{rec_type}_top_{max_records} '
                f'{{{', '.join(annotations)}}} {value}\n'
            )
            metrics += metric

    return PlainTextResponse(metrics)


async def main() -> None:
    print('Starting firehose...')
    await FIREHOSE.run()


loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
loop.create_task(main())
