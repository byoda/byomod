#!/usr/bin/env python3

import time
import signal
import asyncio

from types import FrameType
from collections import namedtuple
from collections import defaultdict


from atproto import (
    CAR,
    AtUri,
    models,
    firehose_models,
    parse_subscribe_repos_message,
    AsyncFirehoseSubscribeReposClient,
)

from atproto_client.models.com.atproto.sync.subscribe_repos import (
    Commit,
    Handle,
    Identity,
    Info,
    Migrate,
    Tombstone,
)

_STOP_AFTER_SECONDS: int = 3

_INTERESTED_RECORDS: dict[str, object] = {
    models.ids.AppBskyFeedLike: models.AppBskyFeedLike,
    # models.ids.AppBskyFeedPost: models.AppBskyFeedPost,
    models.ids.AppBskyGraphFollow: models.AppBskyGraphFollow,
}


CallRate = namedtuple('CallRate', ['calls', 'start_time'])


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit
                     ) -> defaultdict:
    operation_by_type = defaultdict(lambda: {'created': [], 'deleted': []})

    car: CAR = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action == 'update':
            # not supported yet
            continue

        uri: AtUri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'create':
            if not op.cid:
                continue

            create_info: dict[str, str] = {
                'uri': str(uri),
                'cid': str(op.cid),
                'author': commit.repo,
                'record': None
            }

            record_raw_data: dict | None = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(
                record_raw_data, strict=False
            )
            record_type: object | None = _INTERESTED_RECORDS.get(
                uri.collection
            )
            if (record_type
                    and models.is_record_type(record, record_type)):
                create_info['record'] = record
                operation_by_type[uri.collection]['created'].append(
                    create_info
                )

        if op.action == 'delete':
            operation_by_type[uri.collection]['deleted'].append(
                {
                    'uri': str(uri)
                }
            )

    return operation_by_type


def measure_events_per_second(func: callable) -> callable:
    def wrapper(*args) -> any:
        wrapper.calls += 1
        cur_time: float = time.time()

        if cur_time - wrapper.start_time >= 1:
            print(f'NETWORK LOAD: {wrapper.calls} events/second')
            wrapper.start_time = cur_time
            wrapper.calls = 0

        return func(*args)

    wrapper.calls = 0
    wrapper.start_time = time.time()

    return wrapper


FOLLOWS: dict[str, (str, time)] = {}


def measure_follows_per_second(did: str, interval: int) -> callable:
    if did not in FOLLOWS:
        FOLLOWS[did] = (0, time.time())


async def main(firehose_client: AsyncFirehoseSubscribeReposClient) -> None:
    @measure_events_per_second
    async def on_message_handler(message: firehose_models.MessageFrame
                                 ) -> None:
        commit: Commit | Handle | Migrate | Tombstone | Info | Identity = \
            parse_subscribe_repos_message(message)

        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        if commit.seq % 20 == 0:
            firehose_client.update_params(
                models.ComAtprotoSyncSubscribeRepos.Params(
                    cursor=commit.seq
                )
            )

        if not commit.blocks:
            return

        # car: CAR = CAR.from_bytes(commit.blocks)
        # print(f'Commit: {car.root.hash}')

        ops: defaultdict = _get_ops_by_type(commit)

        for model in _INTERESTED_RECORDS.keys():
            posts: list[dict] = ops[model]
            for created_post in posts['created']:
                author: any = created_post['author']
                record: any = created_post['record']

                info: str = (
                    f'NEW {model} record [CREATED_AT={record.created_at}]:'
                    f'AUTHOR={author}:'
                )
                if model == models.ids.AppBskyFeedPost:
                    inlined_text = record.text.replace('\n', ' ')
                    info += inlined_text
                elif model == models.ids.AppBskyGraphFollow:
                    info += f'FOLLOW={record.subject}'
                elif model == models.ids.AppBskyFeedLike:
                    info += f'LIKE={record.subject}'
                else:
                    info += f'UNKNOWN={record}'

                print(info)

        # parsed: Commit | Handle | Migrate | Tombstone | Info | Identity = \
        #     parse_subscribe_repos_message(message)
        # print(message.header, parsed)

    await client.start(on_message_handler)


async def signal_handler(_: int, __: FrameType) -> None:
    print('Keyboard interrupt received. Stopping...')

    # Stop receiving new messages
    await client.stop()


if __name__ == '__main__':
    signal.signal(
        signal.SIGINT,
        lambda _, __: asyncio.create_task(
            signal_handler(_, __)
        )
    )

    start_cursor = None

    params = None
    if start_cursor is not None:
        params = models.ComAtprotoSyncSubscribeRepos.Params(
            cursor=start_cursor
        )

    client = AsyncFirehoseSubscribeReposClient(params)

    asyncio.run(main(client))
