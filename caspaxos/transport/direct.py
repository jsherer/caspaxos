"""
Direct in-memory transport for CASPaxos.

This transport implementation provides direct, in-memory communication
between proposers and acceptors running in the same process. It's the
default transport and is useful for testing and single-node deployments.
"""

from typing import Callable, Dict, Tuple
from .base import Transport, TransportError
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse


class DirectTransport(Transport):
    """
    In-memory transport using direct function calls.
    
    This transport maintains a registry of acceptor handlers and routes
    requests directly to them without any network overhead. All communication
    happens synchronously within the same process.
    """
    
    def __init__(self):
        """Initialize empty handler registry."""
        # Maps acceptor_id to (prepare_handler, accept_handler) tuple
        self.handlers: Dict[str, Tuple[Callable, Callable]] = {}
    
    async def send_prepare(
        self,
        acceptor_id: str,
        request: PrepareRequest
    ) -> PrepareResponse:
        """
        Send a prepare request directly to an acceptor's handler.
        
        Args:
            acceptor_id: Identifier of the target acceptor
            request: The prepare request to send
            
        Returns:
            PrepareResponse from the acceptor
            
        Raises:
            TransportError: If acceptor not found
        """
        if acceptor_id not in self.handlers:
            raise TransportError(
                f"Acceptor '{acceptor_id}' not found in transport",
                acceptor_id
            )
        
        prepare_handler, _ = self.handlers[acceptor_id]
        
        try:
            # Call the handler directly (it's async)
            response = await prepare_handler(request)
            return response
        except Exception as e:
            raise TransportError(
                f"Failed to send prepare to '{acceptor_id}': {e}",
                acceptor_id
            ) from e
    
    async def send_accept(
        self,
        acceptor_id: str,
        request: AcceptRequest
    ) -> AcceptResponse:
        """
        Send an accept request directly to an acceptor's handler.
        
        Args:
            acceptor_id: Identifier of the target acceptor
            request: The accept request to send
            
        Returns:
            AcceptResponse from the acceptor
            
        Raises:
            TransportError: If acceptor not found
        """
        if acceptor_id not in self.handlers:
            raise TransportError(
                f"Acceptor '{acceptor_id}' not found in transport",
                acceptor_id
            )
        
        _, accept_handler = self.handlers[acceptor_id]
        
        try:
            # Call the handler directly (it's async)
            response = await accept_handler(request)
            return response
        except Exception as e:
            raise TransportError(
                f"Failed to send accept to '{acceptor_id}': {e}",
                acceptor_id
            ) from e
    
    async def register_acceptor(
        self,
        acceptor_id: str,
        prepare_handler: Callable[[PrepareRequest], PrepareResponse],
        accept_handler: Callable[[AcceptRequest], AcceptResponse]
    ) -> None:
        """
        Register an acceptor's handlers.
        
        Args:
            acceptor_id: Identifier for this acceptor
            prepare_handler: Async function to handle prepare requests
            accept_handler: Async function to handle accept requests
            
        Raises:
            TransportError: If acceptor already registered
        """
        if acceptor_id in self.handlers:
            raise TransportError(
                f"Acceptor '{acceptor_id}' already registered",
                acceptor_id
            )
        
        self.handlers[acceptor_id] = (prepare_handler, accept_handler)
    
    async def unregister_acceptor(self, acceptor_id: str) -> None:
        """
        Unregister an acceptor.
        
        Args:
            acceptor_id: Identifier of the acceptor to unregister
        """
        self.handlers.pop(acceptor_id, None)
    
    async def close(self) -> None:
        """
        Close the transport and clear all handlers.
        """
        self.handlers.clear()
    
    def list_acceptors(self) -> list[str]:
        """
        List all registered acceptor IDs.
        
        Returns:
            List of acceptor IDs
        """
        return list(self.handlers.keys())