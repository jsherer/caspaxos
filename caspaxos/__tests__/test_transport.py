"""Tests for transport abstraction."""

import pytest
from ..transport.direct import DirectTransport
from ..transport.proxy import RemoteAcceptor
from ..transport.base import TransportError
from ..acceptor import BaseAcceptor
from ..storage.backends.memory import InMemoryStorage
from ..core import Proposer, ProposerError
from ..core.ballot import BallotNumber
from ..core.messages import PrepareRequest, PrepareResponse, AcceptRequest, AcceptResponse


class TestDirectTransport:
    """Test suite for DirectTransport."""
    
    @pytest.mark.asyncio
    async def test_register_acceptor(self):
        """Test registering an acceptor."""
        transport = DirectTransport()
        acceptor = BaseAcceptor("test_acceptor", InMemoryStorage())
        
        # Register acceptor
        await transport.register_acceptor(
            "test_acceptor",
            acceptor.prepare,
            acceptor.accept
        )
        
        # Verify it's registered
        assert "test_acceptor" in transport.list_acceptors()
    
    @pytest.mark.asyncio
    async def test_duplicate_registration(self):
        """Test that duplicate registration raises error."""
        transport = DirectTransport()
        acceptor = BaseAcceptor("test_acceptor", InMemoryStorage())
        
        # First registration should succeed
        await transport.register_acceptor(
            "test_acceptor",
            acceptor.prepare,
            acceptor.accept
        )
        
        # Second registration should fail
        with pytest.raises(TransportError) as exc_info:
            await transport.register_acceptor(
                "test_acceptor",
                acceptor.prepare,
                acceptor.accept
            )
        assert "already registered" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_send_prepare(self):
        """Test sending prepare request."""
        transport = DirectTransport()
        acceptor = BaseAcceptor("acceptor1", InMemoryStorage())
        
        # Register acceptor
        await transport.register_acceptor(
            "acceptor1",
            acceptor.prepare,
            acceptor.accept
        )
        
        # Send prepare
        request = PrepareRequest("key1", BallotNumber(1, "proposer"))
        response = await transport.send_prepare("acceptor1", request)
        
        # Verify response
        assert isinstance(response, PrepareResponse)
        assert response.is_prepared
        assert response.ballot.is_zero()
        assert response.value is None
    
    @pytest.mark.asyncio
    async def test_send_accept(self):
        """Test sending accept request."""
        transport = DirectTransport()
        acceptor = BaseAcceptor("acceptor1", InMemoryStorage())
        
        # Register acceptor
        await transport.register_acceptor(
            "acceptor1",
            acceptor.prepare,
            acceptor.accept
        )
        
        # First do prepare
        ballot = BallotNumber(1, "proposer")
        prepare_req = PrepareRequest("key1", ballot)
        await transport.send_prepare("acceptor1", prepare_req)
        
        # Then accept
        accept_req = AcceptRequest("key1", ballot, "value1", ballot)
        response = await transport.send_accept("acceptor1", accept_req)
        
        # Verify response
        assert isinstance(response, AcceptResponse)
        assert response.is_ok
    
    @pytest.mark.asyncio
    async def test_send_to_unregistered(self):
        """Test that sending to unregistered acceptor raises error."""
        transport = DirectTransport()
        
        request = PrepareRequest("key1", BallotNumber(1, "proposer"))
        
        with pytest.raises(TransportError) as exc_info:
            await transport.send_prepare("nonexistent", request)
        assert "not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_unregister_acceptor(self):
        """Test unregistering an acceptor."""
        transport = DirectTransport()
        acceptor = BaseAcceptor("acceptor1", InMemoryStorage())
        
        # Register
        await transport.register_acceptor(
            "acceptor1",
            acceptor.prepare,
            acceptor.accept
        )
        
        assert "acceptor1" in transport.list_acceptors()
        
        # Unregister
        await transport.unregister_acceptor("acceptor1")
        
        assert "acceptor1" not in transport.list_acceptors()
        
        # Should not be able to send to it anymore
        request = PrepareRequest("key1", BallotNumber(1, "proposer"))
        with pytest.raises(TransportError):
            await transport.send_prepare("acceptor1", request)
    
    @pytest.mark.asyncio
    async def test_close_transport(self):
        """Test closing transport clears all handlers."""
        transport = DirectTransport()
        
        # Register multiple acceptors
        for i in range(3):
            acceptor = BaseAcceptor(f"acceptor{i}", InMemoryStorage())
            await transport.register_acceptor(
                f"acceptor{i}",
                acceptor.prepare,
                acceptor.accept
            )
        
        assert len(transport.list_acceptors()) == 3
        
        # Close transport
        await transport.close()
        
        assert len(transport.list_acceptors()) == 0


class TestRemoteAcceptor:
    """Test suite for RemoteAcceptor proxy."""
    
    @pytest.mark.asyncio
    async def test_remote_acceptor_prepare(self):
        """Test prepare through remote acceptor."""
        # Setup transport and real acceptor
        transport = DirectTransport()
        real_acceptor = BaseAcceptor("remote1", InMemoryStorage())
        
        await transport.register_acceptor(
            "remote1",
            real_acceptor.prepare,
            real_acceptor.accept
        )
        
        # Create remote proxy
        proxy = RemoteAcceptor("remote1", transport)
        
        # Send prepare through proxy
        request = PrepareRequest("key1", BallotNumber(1, "proposer"))
        response = await proxy.prepare(request)
        
        assert isinstance(response, PrepareResponse)
        assert response.is_prepared
    
    @pytest.mark.asyncio
    async def test_remote_acceptor_accept(self):
        """Test accept through remote acceptor."""
        # Setup transport and real acceptor
        transport = DirectTransport()
        real_acceptor = BaseAcceptor("remote1", InMemoryStorage())
        
        await transport.register_acceptor(
            "remote1",
            real_acceptor.prepare,
            real_acceptor.accept
        )
        
        # Create remote proxy
        proxy = RemoteAcceptor("remote1", transport)
        
        # Prepare first
        ballot = BallotNumber(1, "proposer")
        prepare_req = PrepareRequest("key1", ballot)
        await proxy.prepare(prepare_req)
        
        # Then accept
        accept_req = AcceptRequest("key1", ballot, "value1", ballot)
        response = await proxy.accept(accept_req)
        
        assert isinstance(response, AcceptResponse)
        assert response.is_ok
    
    @pytest.mark.asyncio
    async def test_remote_acceptor_get_state_not_supported(self):
        """Test that get_state is not supported for remote acceptors."""
        transport = DirectTransport()
        proxy = RemoteAcceptor("remote1", transport)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await proxy.get_state("key1")
        assert "not supported" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_remote_acceptor_node_id(self):
        """Test getting node ID from remote acceptor."""
        transport = DirectTransport()
        proxy = RemoteAcceptor("remote1", transport)
        
        assert proxy.get_node_id() == "remote1"
    
    def test_remote_acceptor_repr(self):
        transport = DirectTransport()
        east = repr(RemoteAcceptor("east-replica", transport))
        west = repr(RemoteAcceptor("west-replica", transport))

        assert "east-replica" in east
        assert "west-replica" not in east
        assert "west-replica" in west
        assert "east-replica" not in west



class TestTransportIntegration:
    """Integration tests for transport with proposer."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("local_count", [3, 0, 1], ids=["local", "remote", "mixed"])
    @pytest.mark.parametrize("scenario", ["alternating_updates", "read_after_other_writer"])
    async def test_multiple_proposers_make_progress(self, local_count, scenario):
        transport = DirectTransport()
        acceptors = [BaseAcceptor(str(i), InMemoryStorage()) for i in range(3)]
        try:
            for acceptor in acceptors[local_count:]:
                await transport.register_acceptor(
                    acceptor.node_id, acceptor.prepare, acceptor.accept
                )
            options = {
                "acceptors": acceptors[:local_count],
                "transport": transport,
                "remote_acceptor_ids": [
                    acceptor.node_id for acceptor in acceptors[local_count:]
                ],
            }
            first = Proposer("p", **options)
            second = Proposer("q", **options)
            if scenario == "read_after_other_writer":
                await first.put("key", 1)
                await second.put("key", 2)
                assert await first.get("key") == 2
            else:
                calls = 0

                def increment(value):
                    nonlocal calls
                    calls += 1
                    return (value or 0) + 1

                for expected in range(1, 21):
                    writer = first if expected % 2 else second
                    assert await writer.change("key", increment) == expected
                assert calls == 20
                assert await first.get("key") == 20
        finally:
            await transport.close()
    

    @pytest.mark.asyncio
    async def test_proposer_with_remote_acceptors(self):
        """Test proposer working with remote acceptors."""
        
        # Setup transport
        transport = DirectTransport()
        
        # Create and register real acceptors
        for i in range(3):
            acceptor = BaseAcceptor(f"acceptor{i}", InMemoryStorage())
            await transport.register_acceptor(
                f"acceptor{i}",
                acceptor.prepare,
                acceptor.accept
            )
        
        # Create proposer with remote acceptors
        proposer = Proposer(
            "proposer1",
            transport=transport,
            remote_acceptor_ids=[f"acceptor{i}" for i in range(3)]
        )
        
        # Perform consensus
        result = await proposer.put("test_key", "test_value")
        assert result == "test_value"
        
        # Read back
        value = await proposer.get("test_key")
        assert value == "test_value"
        
        # Clean up
        await transport.close()
    
    @pytest.mark.asyncio
    async def test_mixed_local_and_remote(self):
        """Test proposer with mixed local and remote acceptors."""
        
        # Setup transport
        transport = DirectTransport()
        
        # Create local acceptor
        local_acceptor = BaseAcceptor("local1", InMemoryStorage())
        
        # Create and register remote acceptors
        for i in range(2):
            acceptor = BaseAcceptor(f"remote{i}", InMemoryStorage())
            await transport.register_acceptor(
                f"remote{i}",
                acceptor.prepare,
                acceptor.accept
            )
        
        # Create proposer with mixed acceptors
        proposer = Proposer(
            "proposer1",
            acceptors=[local_acceptor],
            transport=transport,
            remote_acceptor_ids=[f"remote{i}" for i in range(2)]
        )
        
        # Perform consensus
        result = await proposer.put("mixed_key", {"type": "mixed"})
        assert result == {"type": "mixed"}
        
        # Compare and swap
        success, value = await proposer.compare_and_swap(
            "mixed_key",
            {"type": "mixed"},
            {"type": "updated"}
        )
        assert success
        assert value == {"type": "updated"}
        
        # Clean up
        await transport.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("unavailable", ["local", "remote0", "remote1"])
    async def test_mixed_cluster_keeps_membership_during_outages(self, unavailable):
        class SwitchableAcceptor(BaseAcceptor):
            available = True

            async def prepare(self, request):
                if not self.available:
                    raise ConnectionError("replica unavailable")
                return await super().prepare(request)

            async def accept(self, request):
                if not self.available:
                    raise ConnectionError("replica unavailable")
                return await super().accept(request)

        transport = DirectTransport()
        replicas = {
            node_id: SwitchableAcceptor(node_id)
            for node_id in ("local", "remote0", "remote1")
        }
        try:
            for node_id in ("remote0", "remote1"):
                replica = replicas[node_id]
                await transport.register_acceptor(node_id, replica.prepare, replica.accept)
            proposer = Proposer(
                "p",
                acceptors=[replicas["local"]],
                transport=transport,
                remote_acceptor_ids=["remote0", "remote1"],
            )
            replicas[unavailable].available = False
            survivors = [
                replica for node_id, replica in replicas.items() if node_id != unavailable
            ]
            assert await proposer.change("key", lambda _: "confirmed", retries=1) == "confirmed"
            for replica in survivors:
                assert (await replica.get_state("key")).accepted_value == "confirmed"

            # A second outage must not shrink the configured quorum to one.
            survivors[0].available = False
            with pytest.raises(ProposerError) as error:
                await proposer.change("key", lambda _: "unconfirmed", retries=1)
            assert error.value.code == "MaxRetriesError"

            survivors[0].available = True
            assert await proposer.get("key", retries=1) == "confirmed"
        finally:
            await transport.close()