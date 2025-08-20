"""
Remote acceptor proxy for transport-based communication.

This module provides a proxy that makes remote acceptors (accessed via
transport) appear as local acceptor objects to the proposer.
"""

from typing import Optional
from .base import Transport
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse
from ..storage.acceptor import Acceptor, AcceptorState


class RemoteAcceptor(Acceptor):
    """
    Proxy for a remote acceptor accessed via transport.
    
    This class implements the Acceptor interface but forwards all requests
    through a transport layer to a remote acceptor. It allows proposers to
    work with remote acceptors as if they were local objects.
    """
    
    def __init__(self, acceptor_id: str, transport: Transport):
        """
        Initialize the remote acceptor proxy.
        
        Args:
            acceptor_id: Identifier of the remote acceptor
            transport: Transport to use for communication
        """
        self.acceptor_id = acceptor_id
        self.transport = transport
    
    async def prepare(self, request: PrepareRequest) -> PrepareResponse:
        """
        Forward a prepare request to the remote acceptor.
        
        Args:
            request: The prepare request
            
        Returns:
            PrepareResponse from the remote acceptor
        """
        return await self.transport.send_prepare(self.acceptor_id, request)
    
    async def accept(self, request: AcceptRequest) -> AcceptResponse:
        """
        Forward an accept request to the remote acceptor.
        
        Args:
            request: The accept request
            
        Returns:
            AcceptResponse from the remote acceptor
        """
        return await self.transport.send_accept(self.acceptor_id, request)
    
    async def get_state(self, key: str) -> Optional[AcceptorState]:
        """
        Get state is not supported for remote acceptors.
        
        This operation would require an additional protocol method
        which is not part of the core CASPaxos protocol.
        
        Raises:
            NotImplementedError: Always, as this is not supported
        """
        raise NotImplementedError(
            "get_state is not supported for remote acceptors. "
            "This operation is only available for local acceptors."
        )
    
    def get_node_id(self) -> str:
        """Get the ID of the remote acceptor."""
        return self.acceptor_id
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"RemoteAcceptor(id='{self.acceptor_id}')"