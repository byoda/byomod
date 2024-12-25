#!/usr/bin/env python3

from byomod.lib.counters import BSkyCounter


def test_bskycounter() -> None:
    c = BSkyCounter()
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')
    c.increment('a')

    c.increment('b')
    c.increment('b')
    c.increment('b')
    c.increment('b')
    c.increment('b')

    c.increment('c')
    c.increment('c')
    c.increment('c')

    c.increment('d')

    result: list[tuple[str, int]] = c.topk(5)
    assert result == [('a', 10), ('b', 5), ('c', 3), ('d', 1)]

    c.increment('a')
    c.increment('a')
    c.increment('a')

    c.increment('b')
    c.increment('b')
    c.increment('b')
    c.increment('b')
    c.increment('b')
    c.increment('b')
    c.increment('b')

    c.increment('c')
    c.increment('c')
    c.increment('c')

    c.increment('d')

    result: list[tuple[str, int]] = c.topk(2)
    # 'b' on top since most increased since last access, even while
    # 'a' has more total counts
    assert result == [('b', 12), ('a', 13)]


if __name__ == '__main__':
    test_bskycounter()
