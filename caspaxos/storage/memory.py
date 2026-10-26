"""In-memory acceptor using the shared protocol implementation."""

from ..acceptor import BaseAcceptor
from .backends.memory import InMemoryStorage


class InMemoryAcceptor(BaseAcceptor):
    """Acceptor with its own in-memory storage backend.

    Atomic protocol transitions and state access are inherited from BaseAcceptor.
    """

    def __init__(self, node_id: str = "acceptor"):
        super().__init__(node_id, storage=InMemoryStorage())