.. -*- coding: utf-8 -*-
.. :Project:   metapensiero.asyncio.transaction -- Handle coroutines from synchronous functions or methods (like special methods)
.. :Created:   dom 09 ago 2015 12:57:35 CEST
.. :Author:    Alberto Berti <alberto@metapensiero.it>
.. :License:   GNU General Public License version 3 or later
.. :Copyright: Copyright (C) 2015 Alberto Berti
..

==================================
 metapensiero.asyncio.transaction
==================================

 :author: Alberto Berti
 :contact: alberto@metapensiero.it
 :license: GNU General Public License version 3 or later

Handle coroutines from synchronous functions or methods (like special methods)
==============================================================================

Goals
+++++

This package helps handling such cases when there is a side effect
that is coded as a coroutine and the code block that needs it cannot
be run as a coroutine.

The case is e. g. when a coroutine is called from an external package
or a Python special method.

This package will give you the tools to ensure that even if the
computation is not immediate, the execution of the coroutine(s) is
guranteed to happen in the context of the coroutine that is calling
the special method.

Usage
+++++

Given a scenario where a coroutine is called from the ``__setattr___``
method, this is how to deal with the situation::

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

In python 3.5, the ``external_coro`` can be written as::

  async def external_coro():
      inst = Example()
      async with transaction.begin():
          inst.foo = 'bar'

So by taking advantage of the new ``__aenter__`` and ``__aexit__``
methods and awaitable classes.

The coroutines will be scheduled in the order they have been created.

At a certain point, you may want  to ensure that all the remaining
coroutines are executed you mai use the coroutine ``transaction.wait_all()``.

Build status
++++++++++++

.. image:: https://travis-ci.org/azazel75/transaction.svg?branch=master
    :target: https://travis-ci.org/azazel75/transaction
