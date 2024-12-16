#!/usr/bin/env python3

import asyncio

# from atproto import Client, client_utils
# from atproto_client.models.app.bsky.actor.defs import ProfileViewDetailed

from byomod.lib.firehose import Firehose

CREDENTIALS_FILE = '.secrets/credentials.json'


async def main() -> None:
    # with open(CREDENTIALS_FILE) as f:
    #     credentials: list[dict[str, str]] = orjson.loads(f.read())
    # credential: dict[str, str] = credentials[0]
    # client = Client()
    # profile: ProfileViewDetailed = client.login(
    #     credential['handle'], credential['password']
    # )
    # print('Welcome,', profile.display_name)

    # text = client_utils.TextBuilder().text('Hello World from ').link('Python SDK', 'https://atproto.blue')
    # post = client.send_post(text)
    # client.like(post.uri, post.cid)

    a= { 'a': 1, 'b': 2, 'c': 3 }
    
    firehose = Firehose()
    await firehose.run()


if __name__ == '__main__':
    asyncio.run(main())
