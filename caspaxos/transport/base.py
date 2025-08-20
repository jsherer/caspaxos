"""
Abstract transport interface for CASPaxos network communication.

This module defines the transport abstraction that allows proposers and
acceptors to communicate over different protocols (in-memory, HTTP, gRPC, etc).
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse


class Transport(ABC):
    """
    Abstract base class for network transport.
    
    Implementations of this interface handle the communication between
    proposers and acceptors, allowing the protocol to work across
    different network protocols and topologies.
    """
    
    @abstractmethod
    async def send_prepare(
        self,
        acceptor_id: str,
        request: PrepareRequest
    ) -> PrepareResponse:
        """
        Send a prepare request to an acceptor.
        
        Args:
            acceptor_id: Identifier of the target acceptor
            request: The prepare request to send
            
        Returns:
            PrepareResponse from the acceptor
            
        Raises:
            TransportError: If communication fails
        """
        pass
    
    @abstractmethod
    async def send_accept(
        self,
        acceptor_id: str,
        request: AcceptRequest
    ) -> AcceptResponse:
        """
        Send an accept request to an acceptor.
        
        Args:
            acceptor_id: Identifier of the target acceptor
            request: The accept request to send
            
        Returns:
            AcceptResponse from the acceptor
            
        Raises:
            TransportError: If communication fails
        """
        pass
    
    @abstractmethod
    async def register_acceptor(
        self,
        acceptor_id: str,
        prepare_handler: Callable[[PrepareRequest], PrepareResponse],
        accept_handler: Callable[[AcceptRequest], AcceptResponse]
    ) -> None:
        """
        Register an acceptor's handlers with this transport.
        
        This allows the transport to route incoming requests to the
        appropriate acceptor handlers.
        
        Args:
            acceptor_id: Identifier for this acceptor
            prepare_handler: Async function to handle prepare requests
            accept_handler: Async function to handle accept requests
        """
        pass
    
    @abstractmethod
    async def unregister_acceptor(self, acceptor_id: str) -> None:
        """
        Unregister an acceptor from this transport.
        
        Args:
            acceptor_id: Identifier of the acceptor to unregister
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """
        Close the transport and clean up resources.
        """
        pass


class TransportError(Exception):
    """Base exception for transport-related errors."""
    
    def __init__(self, message: str, acceptor_id: Optional[str] = None):
        super().__init__(message)
        self.acceptor_id = acceptor_id