"""
Message types for the CASPaxos protocol.

These dataclasses represent the messages exchanged between proposers and acceptors
during the two-phase CASPaxos protocol.
"""

from dataclasses import dataclass
from typing import Any, Optional
from .ballot import BallotNumber


@dataclass
class PrepareRequest:
    """
    Prepare phase request from proposer to acceptor.
    
    Attributes:
        key: The key being operated on
        ballot: The ballot number for this proposal
        extra: Optional extra data for extensions
    """
    key: str
    ballot: BallotNumber
    extra: Optional[Any] = None


@dataclass
class PrepareResponse:
    """
    Prepare phase response from acceptor to proposer.
    
    Attributes:
        is_prepared: True if prepare succeeded
        is_conflict: True if prepare failed due to conflict
        is_error: True if prepare failed due to error
        ballot: The highest ballot seen by acceptor
        value: The accepted value (if any)
    """
    is_prepared: bool = False
    is_conflict: bool = False
    is_error: bool = False
    ballot: Optional[BallotNumber] = None
    value: Optional[Any] = None
    
    def __bool__(self) -> bool:
        """Response is truthy if prepare succeeded."""
        return self.is_prepared


@dataclass
class AcceptRequest:
    """
    Accept phase request from proposer to acceptor.
    
    Attributes:
        key: The key being operated on
        ballot: The ballot number for this proposal
        value: The value to accept
        promise: The promise ballot (next ballot to be used)
        extra: Optional extra data for extensions
    """
    key: str
    ballot: BallotNumber
    value: Any
    promise: BallotNumber
    extra: Optional[Any] = None


@dataclass
class AcceptResponse:
    """
    Accept phase response from acceptor to proposer.
    
    Attributes:
        is_ok: True if accept succeeded
        is_conflict: True if accept failed due to conflict
        is_error: True if accept failed due to error
        ballot: The conflicting ballot (if conflict)
    """
    is_ok: bool = False
    is_conflict: bool = False
    is_error: bool = False
    ballot: Optional[BallotNumber] = None
    
    def __bool__(self) -> bool:
        """Response is truthy if accept succeeded."""
        return self.is_ok