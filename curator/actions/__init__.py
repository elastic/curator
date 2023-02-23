"""Use __init__ to make these not need to be nested under lowercase.Capital"""
from curator.actions.alias import Alias
from curator.actions.allocation import Allocation
from curator.actions.close import Close
from curator.actions.cluster_routing import ClusterRouting
from curator.actions.cold2frozen import Cold2Frozen
from curator.actions.create_index import CreateIndex
from curator.actions.delete_indices import DeleteIndices
from curator.actions.forcemerge import ForceMerge
from curator.actions.index_settings import IndexSettings
from curator.actions.open import Open
from curator.actions.reindex import Reindex
from curator.actions.replicas import Replicas
from curator.actions.rollover import Rollover
from curator.actions.shrink import Shrink
from curator.actions.snapshot import Snapshot, DeleteSnapshots, Restore

CLASS_MAP = {
    'alias' : Alias,
    'allocation' : Allocation,
    'close' : Close,
    'cluster_routing' : ClusterRouting,
    'cold2frozen': Cold2Frozen,
    'create_index' : CreateIndex,
    'delete_indices' : DeleteIndices,
    'delete_snapshots' : DeleteSnapshots,
    'forcemerge' : ForceMerge,
    'index_settings' : IndexSettings,
    'open' : Open,
    'reindex' : Reindex,
    'replicas' : Replicas,
    'restore' : Restore,
    'rollover' : Rollover,
    'snapshot' : Snapshot,
    'shrink' : Shrink,
}
