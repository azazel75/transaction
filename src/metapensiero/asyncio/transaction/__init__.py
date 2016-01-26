# -*- coding: utf-8 -*-
# :Project:   metapensiero.asyncio.transaction -- Handle coroutines from synchronous functions or methods (like special methods)
# :Created:   dom 09 ago 2015 12:57:35 CEST
# :Author:    Alberto Berti <alberto@metapensiero.it>
# :License:   GNU General Public License version 3 or later
# :Copyright: Copyright (C) 2015 Alberto Berti
#

import asyncio
import functools
import inspect
import logging
import sys
import weakref

PY34 = sys.version_info >= (3, 4)
PY35 = sys.version_info >= (3, 5)

logger = logging.getLogger(__name__)

TRANSACTIONS = {}

RELAXED_TRANSACTION_END = False

_nodefault = object()

__all__ = ('Transaction', 'get', 'begin', 'end', 'wait_all', 'RELAXED_TRANSACTION_END')

try:
    # travis tests compatibility?!
    from asyncio import ensure_future
except ImportError:
    ensure_future = asyncio.async

class TransactionError(Exception):
    pass


class Transaction:
    """A mechanism to store coroutines and consume them later.
    """

    @classmethod
    def get(cls, default=_nodefault, loop=None, registry=None, task=None):
        """Get the ongoing transaction for the current task. if a current
        transaction is missing either raises an exception or returns
        the passed-in 'default'.
        """
        global TRANSACTIONS
        task = task or asyncio.Task.current_task(loop=loop)
        registry = registry or TRANSACTIONS
        task_id = hash(task)
        trans_list = registry.get(task_id)
        if trans_list:
            result = trans_list[-1]
        else:
            if default is _nodefault:
                raise TransactionError("There's no transaction"
                                       " begun for task %s" % task_id)
            else:
                result = default
        return result

    @classmethod
    def begin(cls, loop=None, registry=None, task=None):
        """Begin a new transaction"""
        global TRANSACTIONS
        task = task or asyncio.Task.current_task(loop=loop)
        registry = registry or TRANSACTIONS
        task_id = hash(task)
        trans_list = registry.get(task_id)
        if not trans_list:
            registry[task_id] = trans_list = []
        trans = cls((task_id, len(trans_list)), loop, registry)
        trans_list.append(trans)
        if task:
            # task my be None because the code isn't scheduled by asyncio
            task.add_done_callback(functools.partial(cls._owner_task_finalization_cb,
                                                 weakref.ref(trans)))
        return trans

    @classmethod
    def _owner_task_finalization_cb(cls, trans_ref, task):
        """Warn about non-automatic one left open.
        """
        trans = trans_ref()
        if trans and trans.open:
            msg = ("A transaction has not been closed: %r", trans)
            if RELAXED_TRANSACTION_END:
                logger.warning(*msg)
            else:
                logger.error(*msg)
                raise TransactionError(msg[0] % msg[1])

    def __init__(self, trans_id, loop=None, registry=None):
        self.registry = registry or TRANSACTIONS
        self.coros = []
        self.loop = loop or asyncio.get_event_loop()
        self.id = trans_id
        self.open = True
        logger.debug('Beginning transaction: %r', self)

    def add(self, *coros, cback=None):
        """Add a coroutine or awaitable to the set managed by this
        transaction.
        """
        for coro in coros:
            if PY35:
                assert inspect.isawaitable(coro)
            if asyncio.iscoroutine(coro):
                task = ensure_future(coro, loop=self.loop)
            else:
                task = coro
            if task not in self.coros:
                if isinstance(task, asyncio.Future):
                    task.add_done_callback(self._task_remove_cb)
                    if cback:
                        task.add_done_callback(cback)
                self.coros.append(task)
                logger.debug("Added task %r to trans %r", task, self)

    @asyncio.coroutine
    def end(self):
        """Close an ongoing transaction."""
        if not self.open:
            raise TransactionError("This transaction is closed already")
        logger.debug('Ending transaction: %r', self)
        result = yield from asyncio.gather(*self.coros, loop=self.loop)
        self.open = False
        self.remove(self)
        del self.registry
        del self.coros
        del self.loop
        return result

    @classmethod
    def remove(cls, trans):
        """Remove a transaction from its registry."""
        registry = trans.registry
        trans_list = registry[trans.id[0]]
        assert len(trans_list) > 0
        top_trans = trans_list.pop()
        assert trans is top_trans
        if len(trans_list) == 0:
            del registry[trans.id[0]]

    @asyncio.coroutine
    def __aenter__(self):
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc, tb):
        if not exc:
            yield from self.end()
        else:
            logger.debug('An error happened on context exit:',
                         exc_info=(exc_type, exc, tb))
        return False

    def __repr__(self):
        return '<%s id: %s number of items: %d state: %s>' % \
            (self.__class__.__name__, self.id, len(self.coros),
             'open' if self.open else 'closed')

    @staticmethod
    @asyncio.coroutine
    def wait_all(timeout=None, loop=None, registry=None):
        """Return a future that will be complete when the pending coros of
        the transactions will complete, effectively ending all of them.
        """
        # TODO: take loop into account
        global TRANSACTIONS
        registry = registry or TRANSACTIONS
        loop = loop or asyncio.get_event_loop()

        # collect pending transactions
        coros = set()
        for task_id, transactions in registry.items():
            for trans in transactions:
                coros.add(trans.end())
        if coros:
            result = asyncio.wait(coros, loop=loop, timeout=timeout)
        else:
            result = None
        return result

    def _task_remove_cb(self, task):
        """Remove a scheduled coroutine from the set of this transaction."""
        self.coros.remove(task)
        logger.debug("Removed task %r from trans %r", task, self)

get = Transaction.get
begin = Transaction.begin

@asyncio.coroutine
def end(loop=None, registry=None, task=None):
    """End the current defined transaction."""
    trans = get(None, loop, registry, task)
    return (yield from trans.end())

wait_all = Transaction.wait_all
