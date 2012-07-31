.. _install:

==========
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

    Unless you're using a recent Ubuntu (12.04+), its recommended that you use
    the `zc-zookeeper-static` static library as it includes some memory leak
    patches that are not necessarily in Python Zookeeper OS distributions. The
    patches are available upstream in 3.3.6 and 3.4.4.
