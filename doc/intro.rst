.. -*- coding: utf-8 -*-
.. :Project:   metapensiero.asyncio.transaction -- Introduction
.. :Created:   dom 09 ago 2015 12:57:35 CEST
.. :Author:    Alberto Berti <alberto@metapensiero.it>
.. :License:   GNU General Public License version 3 or later
.. :Copyright: Copyright (C) 2015 Alberto Berti
..

Introduction
============

This package helps handling such cases when there is a side effect
that is coded as a coroutine and the code block that needs it cannot
be run as a coroutine.

The case is e.g. when a coroutine is called from an external package
or a Python special method.

This package will give you the tools to ensure that even if the
computation is not immediate, the execution of the coroutine(s) is
guaranteed to happen in the context of the coroutine that is calling
the special method.

A package that works in tandem with this is `metapensiero.signal`__.

__ https://pypi.python.org/pypi/metapensiero.signal

Usage
~~~~~

Given a scenario where a coroutine is called from the ``__setattr___``
method, this is how to deal with the situation:

.. code:: python

  import asyncio
  from metapensiero.async import transaction

  @asyncio.coroutine
  def publish(value):
     # do something async

  class Example:

      def __setattr__(self, name, value):
          trans = transaction.get()
          trans.add(publish(value))
          super().__setattr__(name, value)

  @asyncio.coroutine
  def external_coro():
      inst = Example()
      trans = transaction.begin()
      inst.foo = 'bar'
      yield from trans.end()

In python 3.5, the ``external_coro`` can be written as:

.. code:: python

  async def external_coro():
      inst = Example()
      async with transaction.begin():
          inst.foo = 'bar'

So by taking advantage of the new ``__aenter__`` and ``__aexit__``
methods and awaitable classes.

The coroutines will be scheduled in the order they have been created.

At a certain point, you may want  to ensure that all the remaining
coroutines are executed you may use the coroutine
``transaction.wait_all()``, doing so will end all the remaining open
transactions.

When your code add a coro to the transaction it can also pass a
callback as the ``cback`` keyword parameter, so it will be called when
the stashed coro completes (or is cancelled). This way you can use the
return value of the coro like:

.. code:: python

  import asyncio
  from metapensiero.asyncio import transaction

  results = []

  @asyncio.coroutine
  def stashed_coro():
      nonlocal results
      results.append('called stashed_coro')
      return 'result from stashed coro'

  class A:

      def __init__(self):
          tran = transaction.get()
          c = stashed_coro()
          tran.add(c, cback=self._init)

      def _init(self, stashed_task):
          nonlocal results
          results.append(stashed_task.result())

  @asyncio.coroutine
  def external_coro():
      tran = transaction.begin()
      # in py3.5
      # async with tran:
      #     a = A()
      a = A()
      yield from tran.end()

  yield from asyncio.gather(
      external_coro()
  )

  assert len(results) == 2
  assert results == ['called stashed_coro', 'result from stashed coro']
