"""Use __init__ to make these not need to be nested under lowercase.Capital"""
from curator.cli_singletons.alias import alias
from curator.cli_singletons.allocation import allocation
from curator.cli_singletons.close import close
from curator.cli_singletons.delete import delete_indices, delete_snapshots
from curator.cli_singletons.forcemerge import forcemerge
from curator.cli_singletons.open_indices import open_indices
from curator.cli_singletons.replicas import replicas
from curator.cli_singletons.restore import restore
from curator.cli_singletons.rollover import rollover
from curator.cli_singletons.shrink import shrink
from curator.cli_singletons.snapshot import snapshot
