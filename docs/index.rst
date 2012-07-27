=====
kazoo
=====

Kazoo is a Python library designed to make working with :term:`Zookeeper` a
more hassle-free experience and less prone to errors. Using :term:`Zookeeper`
in a safe manner can be difficult due to the variety of edge-cases in
:term:`Zookeeper` and other bugs that have been present in the Python C
binding. Due to how the C library utilizes a separate C thread for
:term:`Zookeeper` communication some libraries like `gevent`_ also don't work
properly by default. Kazoo handles all of these cases and provides a new
asynchronous API which is consistent when using threads or `gevent`_ greenlets.

Kazoo is heavily inspired by `Netflix Curator`_ simplifications and helpers.

Installing
==========

kazoo can be installed via ``pip`` or ``easy_install``:

.. code-block:: bash

    $ pip install kazoo

You will also need the Python Zookeeper C binding installed. There's an
easy to install statically compiled version:

.. code-block:: bash

    $ pip install zc-zookeeper-static

Or if your OS distribution includes a python-zookeeper package, that can
be installed.

.. warning::

    Unless you're using a recent Ubuntu, its recommended that you use the
    static library above as it includes some memory leak patches that are not
    necessarily in Python Zookeeper OS distributions.

Reference Material
==================

Reference material includes documentation for using `kazoo` and every
`kazoo` API.

.. note::

    It's generally assumed that you are familiar with Zookeeper and have read
    the `Zookeeper Programmers Guide`_ before using `kazoo`.

.. toctree::
   :maxdepth: 1

   basic_usage
   api
   Changelog <changelog>

Source Code
===========

All source code is available on `github under kazoo <https://github.com/python-zk/kazoo>`_.

Reporting Bugs & Getting Help
=============================

Bugs and support issues should be reported on the `kazoo github issue tracker <https://github.com/python-zk/kazoo/issues>`_.

The developers of ``kazoo`` can frequently be found on the Freenode IRC
network in the #zookeeper channel.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`glossary`

.. toctree::
   :hidden:

   glossary

License
=======

``kazoo`` is offered under the Apache License 2.0.

Authors
=======

``kazoo`` started under the `Nimbus Project`_ and through collaboration with
the open-source community has been merged with code from `Mozilla`_ and the
`Zope Corporation`_.

.. _Apache Zookeeper: http://zookeeper.apache.org/
.. _Zookeeper Programmers Guide: http://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
.. _Zookeeper Recipes: http://zookeeper.apache.org/doc/current/recipes.html#sc_recoverableSharedLocks
.. _Nimbus Project: http://www.nimbusproject.org/
.. _Zope Corporation: http://zope.com/
.. _Mozilla: http://www.mozilla.org/
.. _Netflix Curator: https://github.com/Netflix/curator
.. _gevent: http://gevent.org/
