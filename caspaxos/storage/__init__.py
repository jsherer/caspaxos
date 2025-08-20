"""Storage backends for CASPaxos acceptors."""

from .acceptor import Acceptor, AcceptorState
from .memory import InMemoryAcceptor

__all__ = ["Acceptor", "AcceptorState", "InMemoryAcceptor"]