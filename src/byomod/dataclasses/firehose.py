'''
Functions to receive BSky messages from the firehose.

'''
import signal
import logging
import asyncio

from types import FrameType
from collections import defaultdict


from atproto import (
    CAR,
    AtUri,
    models,
    firehose_models,
    parse_subscribe_repos_message,
    AsyncFirehoseSubscribeReposClient,
)

from atproto_client.models import AppBskyRichtextFacet as Facet
from atproto_client.models.app.bsky.feed.like import Record as LikeRecord
from atproto_client.models.app.bsky.feed.post import Record as PostRecord
from atproto_client.models.app.bsky.graph.follow import Record as FollowRecord

from atproto_client.models.app.bsky.embed.images import Image as EmbedImage
from atproto_client.models.app.bsky.embed.external import Main as EmbedCard

from atproto_client.models.com.atproto.sync.subscribe_repos import (
    Commit,
    Handle,
    Identity,
    Info,
    Migrate,
    Tombstone,
)

from byomod.dataclasses.counters import BSkyCounter
from byomod.dataclasses.counters import ANNOTATION_SEPARATOR

_INTERESTED_RECORDS: dict[str, object] = {
    models.ids.AppBskyFeedLike: models.AppBskyFeedLike,
    models.ids.AppBskyFeedPost: models.AppBskyFeedPost,
    models.ids.AppBskyGraphFollow: models.AppBskyGraphFollow,
}

DEBUG: bool = False


class Firehose:
    def __init__(self, start_cursor: int = None) -> None:
        params = None
        if start_cursor is not None:
            params = models.ComAtprotoSyncSubscribeRepos.Params(
                cursor=start_cursor
            )

        self.client = AsyncFirehoseSubscribeReposClient(params)
        self.received_messages: int = 0

        self.counters: dict[str, BSkyCounter] = {
            'posts': BSkyCounter(),
            'likes': BSkyCounter(),
            'liked': BSkyCounter(),
            'follows': BSkyCounter(),
            'followed': BSkyCounter(),
        }

    async def run(self) -> None:
        signal.signal(
            signal.SIGINT,
            lambda _, __: asyncio.create_task(
                self.signal_handler(_, __)
            )
        )

        await self.client.start(self.on_message_handler)

    async def on_message_handler(self, message: firehose_models.MessageFrame
                                 ) -> None:
        commit: Commit | Handle | Migrate | Tombstone | Info | Identity = \
            parse_subscribe_repos_message(message)

        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        if commit.seq % 20 == 0:
            self.client.update_params(
                models.ComAtprotoSyncSubscribeRepos.Params(
                    cursor=commit.seq
                )
            )

        if not commit.blocks:
            return

        ops: defaultdict = Firehose._get_ops_by_type(commit)

        for model in _INTERESTED_RECORDS.keys():
            posts: list[dict] = ops[model]
            for created_post in posts['created']:
                self.received_messages += 1
                author: any = created_post['author']
                record: PostRecord | FollowRecord | LikeRecord = \
                    created_post['record']

                if model == models.ids.AppBskyFeedPost:
                    if not self.process_post(record, author):
                        continue
                elif model == models.ids.AppBskyGraphFollow:
                    if not self.process_follow(record, author):
                        continue
                elif model == models.ids.AppBskyFeedLike:
                    if not self.process_like(record, author):
                        continue
                else:
                    logging.debug(f'UNKNOWN: {record}')

            if DEBUG:
                print(
                    f'Messages received {self.received_messages}\r', end=''
                )

    def process_post(self, record: PostRecord, author: str) -> bool:
        self.counters['posts'].increment(author)

        if record.langs and 'en' not in record.langs:
            return False

        images: list[EmbedImage] = []
        external_card: EmbedCard | None = None
        strong_ref: str | None = None
        if record.embed:
            if record.embed.py_type == 'app.bsky.embed.external':
                external_card = record.embed.external
            elif record.embed.py_type == 'app.bsky.embed.images':
                for image in record.embed.images or []:
                    if not isinstance(image, EmbedImage):
                        continue
                    images.append(image)
            elif record.embed.py_type == 'app.bsky.embed.record':
                if record.embed.record.py_type == 'com.atproto.repo.strongRef':
                    strong_ref = record.embed.record.uri
            elif record.embed.py_type == 'app.bsky.embed.recordWithMedia':
                if record.embed.record.py_type == 'app.bsky.embed.record':
                    if (record.embed.record.record.py_type
                            == 'com.atproto.repo.strongRef'):
                        strong_ref = record.embed.record.record.uri
                    else:
                        logging.debug(
                            f'Unknown embedded record type: '
                            f'{record.embed.record.record.py_type}'
                        )
            elif record.embed.py_type == 'app.bsky.embed.video':
                pass
            else:
                logging.debug(f'Unknown embed type: {record.embed.py_type}')

        facet: Facet
        uris: list[str] = []
        tags: list[str] = []
        mentions: list[str] = []
        for facet in record.facets or []:
            feature: Facet.Link | Facet.Tag | Facet.Mention
            for feature in facet.features:
                if isinstance(feature, Facet.Link):
                    uris.append(feature.uri)
                elif isinstance(feature, Facet.Tag):
                    tags.append(feature.tag)
                elif isinstance(feature, Facet.Mention):
                    mentions.append(feature.did)
            if False and uris and tags:
                print()
                print(f'Languages: {', '.join(record.langs or [])}')
                if external_card:
                    print(
                        f'External card: {external_card.title}: '
                        f'{external_card.uri}'
                    )
                if strong_ref:
                    print(f'Strong ref: {strong_ref}')
                print(f'URLs: {', '.join(uris or [])}')
                print(f'Tags: {', '.join(tags or [])}')
                print(f'Labels {', '.join(record.labels or [])}')
                print('    ' + record.text[0:100])

        return True

    def process_like(self, record: LikeRecord, author: str) -> bool:
        annotation: str = (
            record.subject.cid + ANNOTATION_SEPARATOR +
            record.subject.uri
        )

        self.counters['likes'].increment(annotation)
        self.counters['liked'].increment(author)
        return True

    def process_follow(self, record: FollowRecord, author: str) -> bool:
        self.counters['follows'].increment(author)
        self.counters['followed'].increment(record.subject)
        return True

    @staticmethod
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

            elif op.action == 'delete':
                operation_by_type[uri.collection]['deleted'].append(
                    {
                        'uri': str(uri)
                    }
                )

        return operation_by_type

    async def signal_handler(self, _: int, __: FrameType) -> None:
        logging.debug('Keyboard interrupt received. Stopping...')

        # Stop receiving new messages
        await self.client.stop()
