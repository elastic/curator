.. _actionclasses:

Action Classes
==============

.. seealso:: It is important to note that each action has a `do_action()`
          method, which accepts no arguments.  This is the means by which all
          actions are executed.

* `Alias`_
* `Allocation`_
* `Close`_
* `ClusterRouting`_
* `CreateIndex`_
* `DeleteIndices`_
* `DeleteSnapshots`_
* `ForceMerge`_
* `IndexSettings`_
* `Open`_
* `Reindex`_
* `Replicas`_
* `Restore`_
* `Rollover`_
* `Shrink`_
* `Snapshot`_


Alias
-----
.. autoclass:: curator.actions.alias.Alias
   :members:

Allocation
----------
.. autoclass:: curator.actions.allocation.Allocation
  :members:

Close
-----
.. autoclass:: curator.actions.close.Close
  :members:

ClusterRouting
--------------
.. autoclass:: curator.actions.cluster_routing.ClusterRouting
  :members:

CreateIndex
--------------
.. autoclass:: curator.actions.create_index.CreateIndex
  :members:

DeleteIndices
-------------
.. autoclass:: curator.actions.delete_indices.DeleteIndices
   :members:

DeleteSnapshots
---------------
.. autoclass:: curator.actions.snapshot.DeleteSnapshots
  :members:

ForceMerge
----------
.. autoclass:: curator.actions.forcemerge.ForceMerge
  :members:

IndexSettings
--------------
.. autoclass:: curator.actions.index_settings.IndexSettings
  :members:

Open
----
.. autoclass:: curator.actions.open.Open
   :members:

Reindex
--------
.. autoclass:: curator.actions.reindex.Reindex
  :members:

Replicas
--------
.. autoclass:: curator.actions.replicas.Replicas
  :members:

Restore
--------
.. autoclass:: curator.actions.snapshot.Restore
  :members:

Rollover
--------
.. autoclass:: curator.actions.rollover.Rollover
  :members:

Shrink
--------
.. autoclass:: curator.actions.shrink.Shrink
  :members:

Snapshot
--------
.. autoclass:: curator.actions.snapshot.Snapshot
  :members:
