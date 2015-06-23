.. _commands:

Command Methods
===============

Sections

* `Aliasing Indices`_
* `Index Routing Allocation`_
* `Disabling Bloom Filters`_
* `Closing Indices`_
* `Deleting Indices`_
* `Opening Indices`_
* `Optimizing Indices`_
* `Changing Index Replica Count`_
* `Show Indices`_
* `Snapshot Indices`_

Aliasing Indices
----------------

alias
+++++
.. automethod:: curator.api.alias

add_to_alias
++++++++++++
.. automethod:: curator.api.add_to_alias

remove_from_alias
+++++++++++++++++
.. automethod:: curator.api.remove_from_alias

Index Routing Allocation
------------------------

allocation
++++++++++
.. automethod:: curator.api.allocation

apply_allocation_rule
+++++++++++++++++++++
.. automethod:: curator.api.apply_allocation_rule


Disabling Bloom Filters
-----------------------

bloom
+++++
.. automethod:: curator.api.bloom

disable_bloom_filter
++++++++++++++++++++
.. automethod:: curator.api.disable_bloom_filter

loop_bloom
++++++++++
.. automethod:: curator.api.loop_bloom


Closing Indices
---------------

close
+++++
.. automethod:: curator.api.close

close_indices
+++++++++++++
.. automethod:: curator.api.close_indices


Deleting Indices
----------------

delete
++++++
.. automethod:: curator.api.delete

delete_indices
++++++++++++++
.. automethod:: curator.api.delete_indices


Opening Indices
---------------

opener
++++++
.. automethod:: curator.api.opener

open_indices
++++++++++++
.. automethod:: curator.api.open_indices


Optimizing Indices
------------------

optimize
++++++++
.. automethod:: curator.api.optimize

optimize_index
++++++++++++++
.. automethod:: curator.api.optimize_index


Changing Index Replica Count
----------------------------

replicas
++++++++
.. automethod:: curator.api.replicas

change_replicas
+++++++++++++++
.. automethod:: curator.api.change_replicas

Sealing (Synced Flush) Indices
------------------------------

seal
++++
.. automethod:: curator.api.seal

seal_indices
++++++++++++
.. automethod:: curator.api.seal_indices

Show Indices
------------

show
++++
.. automethod:: curator.api.show


Snapshot Indices
----------------

create_snapshot
+++++++++++++++
.. automethod:: curator.api.create_snapshot

delete_snapshot
+++++++++++++++
.. automethod:: curator.api.delete_snapshot
