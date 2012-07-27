.. _basic_usage:

===========
Basic Usage
===========

.. code-block:: python

    import json
    import kazoo.client

    # No parameters assumes you want to connect to zookeeper on localhost:2181
    # Make sure zookeeper is actually running there.
    # For more information:
    # http://kazoo.readthedocs.org/en/latest/api/client.html#kazoo.client.KazooClient
    zk = kazoo.client.KazooClient()
    zk.connect()

    node_name = '/kazoo/test'
    zk.ensure_path(node_name)
    (json_data, zstat) = zk.get(node_name)
    if json_data:
        data = json.loads(json_data)
        print 'Found goodies at', node_name
        print data
    else:
        print 'No goodies found, setting some'
        blob = {
            'name': 'Nicholas',
            'age': 1,
        }
        zk.set(node_name, json.dumps(blob))
