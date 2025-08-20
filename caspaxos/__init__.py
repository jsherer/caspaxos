"""
CASPaxos: A Python implementation of the CASPaxos consensus protocol.

CASPaxos is a replicated state machine protocol that provides strongly consistent
operations without the complexity of leader election and log replication.
"""

from .caspaxos import CASPaxos, KeyValueStore, CASKeyValue

__version__ = "0.1.0"
__all__ = ["CASPaxos", "KeyValueStore", "CASKeyValue"]