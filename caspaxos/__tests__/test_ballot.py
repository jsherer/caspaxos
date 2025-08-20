"""Tests for BallotNumber class."""

import pytest
from ..core.ballot import BallotNumber


class TestBallotNumber:
    """Test suite for BallotNumber."""
    
    def test_zero_ballot(self):
        """Test zero ballot creation and detection."""
        ballot = BallotNumber.zero()
        assert ballot.counter == 0
        assert ballot.node_id == ""
        assert ballot.is_zero()
        
        non_zero = BallotNumber(1, "node1")
        assert not non_zero.is_zero()
    
    def test_ballot_comparison(self):
        """Test ballot comparison operations."""
        b1 = BallotNumber(1, "node1")
        b2 = BallotNumber(2, "node1")
        b3 = BallotNumber(1, "node2")
        b4 = BallotNumber(1, "node1")
        
        # Counter comparison
        assert b1 < b2
        assert b2 > b1
        assert b1 <= b2
        assert b2 >= b1
        
        # Node ID comparison (when counters equal)
        assert b1 < b3  # "node1" < "node2"
        assert b3 > b1
        
        # Equality
        assert b1 == b4
        assert b1 != b2
        assert b1 != b3
    
    def test_ballot_increment(self):
        """Test ballot increment operations."""
        ballot = BallotNumber(5, "node1")
        
        # Increment
        new_ballot = ballot.increment()
        assert new_ballot.counter == 6
        assert new_ballot.node_id == "node1"
        assert ballot.counter == 5  # Original unchanged
        
        # Next
        next_ballot = ballot.next()
        assert next_ballot.counter == 6
        assert next_ballot.node_id == "node1"
    
    def test_ballot_fastforward(self):
        """Test fast-forward operation."""
        ballot = BallotNumber(5, "node1")
        other = BallotNumber(10, "node2")
        
        ballot.fastforward_after(other)
        assert ballot.counter == 11  # max(5, 10) + 1
        assert ballot.node_id == "node1"  # Node ID unchanged
        
        # Fast-forward when already ahead
        ballot2 = BallotNumber(15, "node1")
        ballot2.fastforward_after(other)
        assert ballot2.counter == 16  # max(15, 10) + 1
    
    def test_ballot_serialization(self):
        """Test ballot string serialization."""
        ballot = BallotNumber(42, "node1")
        
        # Stringify
        text = ballot.stringify()
        assert text == "42,node1"
        assert str(ballot) == "42,node1"
        
        # Parse
        parsed = BallotNumber.parse(text)
        assert parsed.counter == 42
        assert parsed.node_id == "node1"
        assert parsed == ballot
        
        # Parse zero ballot
        zero = BallotNumber.parse("0,")
        assert zero.is_zero()
    
    def test_ballot_hash(self):
        """Test ballot hashing for use in sets/dicts."""
        b1 = BallotNumber(1, "node1")
        b2 = BallotNumber(1, "node1")
        b3 = BallotNumber(2, "node1")
        
        # Equal ballots have same hash
        assert hash(b1) == hash(b2)
        
        # Can use in set
        ballot_set = {b1, b2, b3}
        assert len(ballot_set) == 2  # b1 and b2 are same
        
        # Can use as dict key
        ballot_dict = {b1: "value1", b3: "value2"}
        assert ballot_dict[b2] == "value1"  # b2 equals b1
    
    def test_ballot_repr(self):
        """Test ballot representation."""
        ballot = BallotNumber(42, "node1")
        assert repr(ballot) == "BallotNumber(42, 'node1')"
        
        zero = BallotNumber.zero()
        assert repr(zero) == "BallotNumber(0, '')"
    
    def test_compare_to_method(self):
        """Test compare_to method directly."""
        b1 = BallotNumber(1, "node1")
        b2 = BallotNumber(2, "node1")
        b3 = BallotNumber(1, "node1")
        
        assert b1.compare_to(b2) == -1
        assert b2.compare_to(b1) == 1
        assert b1.compare_to(b3) == 0