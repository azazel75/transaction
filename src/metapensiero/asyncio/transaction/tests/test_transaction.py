# -*- coding: utf-8 -*-
# :Project:  metapensiero.asyncio.transaction -- tests
# :Created:  mar 15 dic 2015 15:16:56 CET
# :Author:   Alberto Berti <alberto@metapensiero.it>
# :License:  GNU General Public License version 3 or later
#

import asyncio

import pytest

from metapensiero.asyncio import transaction

@pytest.mark.asyncio
@asyncio.coroutine
def test_transaction_per_task(event_loop):

    tasks_ids = set()
    results = []

    @asyncio.coroutine
    def stashed_coro():
        nonlocal results
        results.append('called stashed_coro')

    def non_coro_func():
        tran = transaction.get(loop=event_loop)
        c = stashed_coro()
        tran.add(c)

    @asyncio.coroutine
    def external_coro():
        nonlocal tasks_ids
        task = asyncio.Task.current_task(loop=event_loop)
        tasks_ids.add(id(task))
        tran = transaction.begin(loop=event_loop)
        # in py3.5
        # async with tran:
        #     non_coro_func()
        non_coro_func()
        yield from tran.end()


    yield from asyncio.gather(
        external_coro(),
        external_coro(),
        loop=event_loop
    )

    assert len(tasks_ids) == 2
    assert len(results) == 2
    assert results == ['called stashed_coro', 'called stashed_coro']

@pytest.mark.asyncio
@asyncio.coroutine
def test_non_closed_transaction(event_loop):

    tasks_ids = set()
    results = []

    event_loop.set_debug(True)

    @asyncio.coroutine
    def stashed_coro():
        nonlocal results
        results.append('called stashed_coro')

    def non_coro_func():
        tran = transaction.get(loop=event_loop)
        c = stashed_coro()
        tran.add(c)

    @asyncio.coroutine
    def external_coro():
        nonlocal tasks_ids
        task = asyncio.Task.current_task(loop=event_loop)
        tasks_ids.add(id(task))
        tran = transaction.begin(loop=event_loop)
        # in py3.5
        # async with tran:
        #     non_coro_func()
        non_coro_func()


    yield from external_coro()
    done, pending = yield from transaction.wait_all()
    # the raise from the callback gets sucked up, this is the only
    # crumb left
    assert len(done) == 1
    assert len(pending) == 0

def test_calling_from_non_task():

    results = []

    @asyncio.coroutine
    def stashed_coro():
        nonlocal results
        results.append('called stashed_coro')

    def non_coro_func():
        tran = transaction.get()
        c = stashed_coro()
        tran.add(c)

    # create a new event loop because i get a closed one with
    # get_event_loop(), probably a conflict with pytest-asyncio
    loop = asyncio.new_event_loop()
    tran = transaction.begin(loop=loop)
    non_coro_func()
    assert len(results) == 0
    loop.run_until_complete(tran.end())
    assert len(results) == 1

@pytest.mark.asyncio
@asyncio.coroutine
def test_transaction_per_task_with_cback(event_loop):

    results = []

    @asyncio.coroutine
    def stashed_coro():
        nonlocal results
        results.append('called stashed_coro')
        return 'result from stashed coro'

    def non_coro_func():
        tran = transaction.get(loop=event_loop)
        c = stashed_coro()
        tran.add(c, cback=_cback)

    def _cback(stashed_task):
        results.append(stashed_task.result())

    @asyncio.coroutine
    def external_coro():
        tran = transaction.begin(loop=event_loop)
        # in py3.5
        # async with tran:
        #     non_coro_func()
        non_coro_func()
        yield from tran.end()

    yield from asyncio.gather(
        external_coro(),
        loop=event_loop
    )

    assert len(results) == 2
    assert results == ['called stashed_coro', 'result from stashed coro']

@pytest.mark.asyncio
@asyncio.coroutine
def test_transaction_per_task_with_cback2(event_loop):

    results = []

    @asyncio.coroutine
    def stashed_coro():
        nonlocal results
        results.append('called stashed_coro')
        return 'result from stashed coro'

    class A:

        def __init__(self):
            tran = transaction.get(loop=event_loop)
            c = stashed_coro()
            tran.add(c, cback=self._init)

        def _init(self, stashed_task):
            nonlocal results
            results.append(stashed_task.result())

    @asyncio.coroutine
    def external_coro():
        tran = transaction.begin(loop=event_loop)
        # in py3.5
        # async with tran:
        #     a = A()
        a = A()
        yield from tran.end()

    yield from asyncio.gather(
        external_coro(),
        loop=event_loop
    )

    assert len(results) == 2
    assert results == ['called stashed_coro', 'result from stashed coro']
