.. _api:

Command Methods
===============

Sections

* `Alias`_
* `Allocation`_
* `Bloom`_
* `Close`_
* `Delete`_
* `Open`_
* `Optimize`_
* `Replicas`_
* `Show`_
* `Snapshot`_

Alias
-----

alias
+++++
.. automethod:: curator.api.alias

add_to_alias
++++++++++++
.. automethod:: curator.api.add_to_alias

remove_from_alias
+++++++++++++++++
.. automethod:: curator.api.remove_from_alias

Allocation
----------

allocation
++++++++++
.. automethod:: curator.api.allocation

apply_allocation_rule
+++++++++++++++++++++
.. automethod:: curator.api.apply_allocation_rule


Bloom
-----

bloom
+++++
.. automethod:: curator.api.bloom

disable_bloom_filter
++++++++++++++++++++
.. automethod:: curator.api.disable_bloom_filter

loop_bloom
++++++++++
.. automethod:: curator.api.loop_bloom


Close
-----

close
+++++
.. automethod:: curator.api.close

close_indices
+++++++++++++
.. automethod:: curator.api.close_indices


Delete
------

delete
++++++
.. automethod:: curator.api.delete

delete_indices
++++++++++++++
.. automethod:: curator.api.delete_indices


Open
----

opener
++++++
.. automethod:: curator.api.opener

open_indices
++++++++++++
.. automethod:: curator.api.open_indices


Optimize
--------

optimize
++++++++
.. automethod:: curator.api.optimize

optimize_index
++++++++++++++
.. automethod:: curator.api.optimize_index


Replicas
--------

replicas
++++++++
.. automethod:: curator.api.replicas

change_replicas
+++++++++++++++
.. automethod:: curator.api.change_replicas


Snapshot
--------

create_snapshot
+++++++++++++++
.. automethod:: curator.api.create_snapshot

delete_snapshot
+++++++++++++++
.. automethod:: curator.api.delete_snapshot
