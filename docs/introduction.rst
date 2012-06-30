.. _introduction:

=================
Introducing Kazoo
=================

Kazoo is a Python library designed to make working with :term:`Zookeeper` a
more hassle-free experience less prone to errors. Using :term:`Zookeeper` in a
safe manner can be difficult due to the variety of edge-cases in
:term:`Zookeeper` and other bugs that have been present in the Python C
binding. Due to how the C library utilizes a separate C thread for
:term:`Zookeeper` communication some libraries like `gevent`_ also don't work
properly by default. Kazoo handles all of these cases and provides a new
asynchronous API which is consistent when using threads or `gevent`_ greenlets.

## TODO: Add History of merging, Zookeeper overview

.. _gevent: http://gevent.org/
