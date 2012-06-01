=====
kazoo
=====

`kazoo` simplifies communication with `Apache Zookeeper`_ from Python making it
easier to write code that's less error-prone than using the basic Python
Zookeeper library. It implements patterns from `Netflix Curator`_, in a more
Pythonic manner, and is compatible with async environments like `gevent`_.

Installing
==========

kazoo can be installed via ``easy_install`` or ``pip``:

.. code-block:: bash

    $ easy_install kazoo

You will also need the Python zookeeper C binding installed. There's an
easy to install staticly compiled version:

.. code-block:: bash

    $ easy_install zc-zookeeper-static

Or if your OS distribution includes a python-zookeeper package, that can
be installed.

Reference Material
==================

Reference material includes documentation for every `kazoo` API.

.. toctree::
   :maxdepth: 1

   api
   Changelog <changelog>

Source Code
===========

All source code is available on `github under kazoo <https://github.com/python-zk/kazoo>`_.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`glossary`

License
=======

``kazoo`` is offered under the Apache License 2.0.

Authors
=======

``kazoo`` started under the `Nimbus Project`_ and through collaboration with
the open-source community has been merged with code from `Mozilla`_ and the
`Zope Corporation`_.

.. _Apache Zookeeper: http://zookeeper.apache.org/
.. _Zookeeper Recipes: http://zookeeper.apache.org/doc/current/recipes.html#sc_recoverableSharedLocks
.. _Nimbus Project: http://www.nimbusproject.org/
.. _Zope Corporation: http://zope.com/
.. _Mozilla: http://www.mozilla.org/
.. _Netflix Curator: https://github.com/Netflix/curator
.. _gevent: http://gevent.org/
