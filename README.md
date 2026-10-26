# CASPaxos Python Implementation

A clean, educational Python implementation of the CASPaxos consensus protocol. CASPaxos is a replicated state machine protocol that provides strongly consistent operations without the complexity of leader election and log replication.

## Features

- **Simple API**: Core `change(key, update_fn)` operation for all state modifications
- **No Leader Election**: Symmetric peer-to-peer design avoids single points of failure
- **Optimistic Concurrency**: Compare-and-swap operations for conflict resolution
- **Modular Design**: Separate acceptor, proposer, and storage components
- **Async/Await**: Built on Python's asyncio for concurrent operations
- **In-Memory Storage**: Default in-memory acceptors for development and testing
- **SQLite Storage**: Opt-in durable acceptor state using the standard library
- **Thread-Safe**: Per-key locking ensures consistency

## Installation

The implementation is self-contained with no external dependencies for the core functionality:

```python
# No installation needed, just import from the caspaxos directory
from caspaxos import CASPaxos, KeyValueStore, CASKeyValue
```

## Quick Start

### Basic Usage

```python
import asyncio
from caspaxos import CASPaxos

async def main():
    # Create CASPaxos instance with 3 acceptors (tolerates 1 failure)
    paxos = CASPaxos(node_id="node1", num_acceptors=3)
    
    # Core operation: change(key, update_function)
    # Set initial value
    await paxos.change("counter", lambda x: 0)
    
    # Increment counter
    result = await paxos.change("counter", lambda x: x + 1)
    print(f"Counter: {result}")  # Counter: 1
    
    # Convenience methods
    await paxos.put("key1", "value1")
    value = await paxos.get("key1")
    print(f"Value: {value}")  # Value: value1

asyncio.run(main())
```

### Durable SQLite Storage

The default acceptors remain in-memory. Use `SQLiteStorage(path, *, timeout=5.0)`
with `BaseAcceptor` for durable state; no additional dependencies are required.
Reopen the same files after restarting the application:

```python
import asyncio
from contextlib import AsyncExitStack
from pathlib import Path

from caspaxos import CASPaxos
from caspaxos.acceptor import BaseAcceptor
from caspaxos.storage.backends import SQLiteStorage

async def main():
    directory = Path("caspaxos-data")
    directory.mkdir(exist_ok=True)
    async with AsyncExitStack() as stack:
        acceptors = []
        for node_id in ("a", "b", "c"):
            storage = await stack.enter_async_context(
                SQLiteStorage(directory / f"{node_id}.sqlite")
            )
            acceptors.append(BaseAcceptor(node_id, storage))

        paxos = CASPaxos("p", acceptors=acceptors)
        print(await paxos.change("counter", lambda value: (value or 0) + 1))

asyncio.run(main())
```

Each run increments the persisted counter. Use one database file per logical
acceptor, not per proposer. Different acceptor identities must not share one
file and then be counted as independent quorum participants. Three files on
one host do not protect against loss of that host or disk.

SQLite commits complete state—promised ballot, accepted ballot, and accepted
value—before acknowledging a write. It uses WAL mode with `synchronous=FULL`;
durability depends on the filesystem and hardware honoring synchronization.
Use local storage that supports SQLite WAL, not a network filesystem.
`:memory:` databases are rejected because they cannot provide this WAL-backed
durability.

`update_state` uses `BEGIN IMMEDIATE` to serialize transitions across connections
and processes. SQLite permits one writer per database at a time, including
updates to different keys. SQLite I/O and storage transition callbacks run in
a dedicated worker thread; these callbacks must not depend on an asyncio event
loop. Application callbacks passed to `CASPaxos.change` still run on the event
loop. `timeout` bounds SQLite lock waits, independently of proposer phase timeouts.

**Only open trusted database files.** State is serialized with Python pickle;
loading a malicious pickle can execute arbitrary code. Values must be pickleable
as well as supporting the API's `deepcopy` isolation contract. Reopening files
requires compatible Python classes for stored objects.

The caller owns storage lifetime: use `async with` or `await storage.close()`.
Close drains submitted work before closing the connection. Cancellation of an
operation does not undo its submitted work, which may still commit; cancelling
a close waiter does not stop cleanup, and another `close()` can await completion.
Do not copy only the main database file while it is open: committed data can
still reside in its WAL. Use SQLite's backup facilities or close all connections
before copying. Deleting or clearing a live acceptor's state discards promises
and can break consensus safety.

### Local and Remote Acceptors

`caspaxos.core.Proposer` is the single consensus implementation. Both it and
`CASPaxos` expose `change`, `get`, `put`, and `compare_and_swap`, using the same
`caspaxos.core.ProposerError` exception.

Pass `acceptors=[...]` for local participants, `transport` and
`remote_acceptor_ids` for remote participants, or both for a mixed cluster.
The default quorum is a majority of the combined membership.

```python
import asyncio
from caspaxos.core import Proposer
from caspaxos.acceptor import BaseAcceptor
from caspaxos.transport.direct import DirectTransport

async def main():
    transport = DirectTransport()
    try:
        remote_ids = ["a", "b", "c"]
        for node_id in remote_ids:
            acceptor = BaseAcceptor(node_id)
            await transport.register_acceptor(
                node_id, acceptor.prepare, acceptor.accept
            )
        proposer = Proposer(
            "p", transport=transport, remote_acceptor_ids=remote_ids
        )
        await proposer.put("counter", 1)
        print(await proposer.compare_and_swap("counter", 1, 2))  # (True, 2)
        print(await proposer.get("counter"))  # 2
    finally:
        await transport.close()

asyncio.run(main())
```

`DirectTransport` uses in-process handlers; it does not provide network I/O.
The caller owns the transport and must close it. `CASPaxos` accepts the same
`acceptors`, `transport`, `remote_acceptor_ids`, `quorum_size`, and `phase_timeout`
options. It creates default local acceptors only when neither local acceptors
nor remote IDs are supplied; a remote-only cluster gets no implicit local replicas.

Both APIs support `change(key, update_fn, *, extra=None, retries=10)`.
`extra` is forwarded to protocol requests; `retries` bounds prepare attempts,
not accept attempts. The versioned `CASKeyValue.read` / `write` API is unchanged.
Both optional arguments must be named: use `retries=3` for a prepare-attempt
budget or `extra=metadata` for protocol extension data. A third positional
argument, such as `change(key, update_fn, 3)`, raises `TypeError` before protocol
I/O rather than silently being interpreted as metadata.

`get(key, *, retries=10)` instead bounds total read attempts across both phases.
Failed reads prepare afresh before retrying; exhausting the budget raises
`MaxRetriesError`. The asynchronous and synchronous `CASPaxos` wrappers expose the
same read budget.

### Key-Value Store

```python
from caspaxos import KeyValueStore

async def main():
    kv = KeyValueStore()
    
    # Store data
    await kv.put("user:1", {"name": "Alice", "age": 30})
    
    # Retrieve data
    user = await kv.get("user:1")
    print(user)  # {'name': 'Alice', 'age': 30}
    
    # Delete data
    await kv.delete("user:1")

asyncio.run(main())
```

`KeyValueStore.delete(key)` returns `True` only when it removes a live entry.
Deleting a missing or already-deleted key returns `False`. A stored `None` is
still a live entry, and `put` recreates a previously deleted key.

### Compare-And-Swap Operations

```python
from caspaxos import CASKeyValue

async def main():
    cas = CASKeyValue()
    
    # Versioned updates
    doc = await cas.read("doc1")
    print(f"Version: {doc['version']}")  # Version: 0
    
    # Update with version check
    await cas.write("doc1", 0, "content v1")
    
    # Atomic counter
    count = await cas.increment("views")
    print(f"Views: {count}")  # Views: 1

asyncio.run(main())
```

For value-based CAS, `Proposer.compare_and_swap(key, expected, new_value)`
returns `(matched, resulting_value)` after consensus. The boolean reports whether
the current value matched `expected`, not whether the result equals `new_value`.
A mismatched expectation returns `False` even if the replacement is already
stored; a matching no-op returns `True`. Consensus failures still raise rather
than reporting a comparison result. `CASPaxos.compare_and_swap` exposes the same operation.

## Architecture

### Core Components

1. **BallotNumber** (`core/ballot.py`)
   - Totally ordered ballot numbers using (counter, node_id) pairs
   - Ensures global ordering of proposals

2. **Proposer** (`core/proposer.py`)
   - Coordinates the two-phase consensus protocol
   - Manages prepare and accept phases
   - Maintains per-key locks for serialization

3. **Acceptor** (`storage/acceptor.py`, `acceptor.py`)
   - `Acceptor` defines the protocol interface and acceptor state
   - `BaseAcceptor` implements atomic protocol transitions with pluggable storage
   - `InMemoryAcceptor` (`storage/memory.py`) inherits that implementation and supplies its own `InMemoryStorage` backend

4. **Messages** (`core/messages.py`)
   - PrepareRequest/Response
   - AcceptRequest/Response

### Protocol Flow

```
Client -> Proposer -> Acceptors
           |            |
           |-- Phase 1: Prepare -->
           |<-- Promise + Value ---
           |            |
           |-- Phase 2: Accept --->
           |<-- Accepted ---------
           |
        Result -> Client
```

## Advanced Examples

### Distributed Lock

```python
class DistributedLock:
    def __init__(self, paxos, lock_name):
        self.paxos = paxos
        self.lock_name = f"lock:{lock_name}"
    
    async def acquire(self, owner):
        def try_acquire(current):
            if current is None:
                return {"owner": owner, "time": time.time()}
            raise ValueError("Lock held")
        
        try:
            await self.paxos.change(self.lock_name, try_acquire)
            return True
        except:
            return False
```

### Concurrent Operations

```python
async def concurrent_increments():
    paxos = CASPaxos(num_acceptors=3)
    await paxos.put("counter", 0)
    
    async def increment():
        for _ in range(100):
            await paxos.change("counter", lambda x: x + 1)
    
    # Run 10 workers concurrently
    await asyncio.gather(*[increment() for _ in range(10)])
    
    # Result will be exactly 1000
    final = await paxos.get("counter")
    assert final == 1000
```

## Testing

Regression tests check quorum behavior at the acceptance boundary: majority
quorums for three-, four-, and five-replica clusters, plus a custom four-of-five
quorum. Both protocol phases must reject one-too-few available replicas and
succeed once the threshold is reached.

Mixed-cluster tests verify that every pair of participants can form a quorum,
that two outages cannot shrink membership to a single-replica quorum, and that
recovery preserves the confirmed value. Partial-accept and cancellation tests
also cover recovery after an earlier acknowledged write, without depending on
private cache state.

Remote proxy diagnostics are checked for the correct acceptor identity in
`repr`, without requiring a particular punctuation format.

### Using Make (Recommended)

A Makefile is provided for convenience:

```bash
# Show all available targets
make help

# Run all tests
make test

# Run tests with verbose output
make test-verbose

# Run specific test suites
make test-ballot     # Test ballot number implementation
make test-acceptor   # Test acceptor implementation
make test-proposer   # Test proposer implementation
make test-caspaxos   # Test main CASPaxos API

# Run examples
make example         # or 'make demo'

# Run everything (tests + examples)
make all

# Clean cache files
make clean
```

### Manual Testing

```bash
# Set Python path
export PYTHONPATH=/app

# Run all tests
env/bin/python -m pytest caspaxos/caspaxos/__tests__/

# Run specific test file
env/bin/python -m pytest caspaxos/caspaxos/__tests__/test_ballot.py

# Run with coverage
env/bin/python -m pytest --cov=caspaxos caspaxos/caspaxos/__tests__/
```

### Running Examples

```bash
# Using make
make example

# Or manually
PYTHONPATH=/app env/bin/python -m caspaxos.caspaxos.example
```

## Implementation Notes

### Differences from Original Gryadka

- **Python Async**: Uses asyncio instead of JavaScript promises
- **Type Hints**: Full type annotations for better IDE support
- **Dataclasses**: Message types use Python dataclasses
- **Testing**: pytest-based test suite

### Performance Considerations

- **Memory**: All acceptor state is in-memory by default
- **Concurrency**: Per-key locking allows parallel operations on different keys
- **Protocol Rounds**: Each operation runs prepare and accept; no cached single-phase fast path
- **Quorum**: Default majority quorum (N/2 + 1) for N acceptors

### Quorum Completion and Timeouts

The proposer finishes each prepare or accept phase as soon as a quorum succeeds.
It also stops early when the remaining requests cannot form a quorum.
A stalled minority no longer prevents progress.

The quorum collector classifies protocol responses once and returns both the
completed responses and the classified successes. The proposer uses that same
success list rather than evaluating the success predicate again. Conflict and
error responses remain available for ballot advancement and failure diagnostics.

`CASPaxos` and `Proposer` accept a positive, finite `phase_timeout` in seconds,
defaulting to `5.0`. It bounds each phase, not the whole change operation;
each phase of each retry gets a new deadline. For example,
`CASPaxos(phase_timeout=0.5)` uses a half-second deadline
for each prepare or accept phase.

Outstanding requests are cancelled on completion, failure, deadline expiry, or
caller cancellation. Phase completion does not wait for slow cancellation
cleanup; late results and exceptions are collected without changing the returned
outcome. Cancellation is not a rollback: an accept may already have persisted.
Custom acceptors and transports must yield during I/O and honor cancellation to
release resources; requests that delay cancellation remain tracked until they
finish.

### Mutable Values

Values must support `copy.deepcopy`, including independent copies of mutable
contents. Update functions receive detached values and may mutate and return
them; this preserves the previous value if a callback mutates its argument and
then raises. The proposer snapshots the callback result before accept I/O so
caller-owned objects cannot change a proposal while requests are pending.
The returned value is a separate copy: quorum completion can leave minority
accept requests running, and mutating a result must not change what those
replicas eventually accept.

The in-memory backend snapshots complete acceptor state on write and read,
including ballots and nested values. Custom storage backends must provide the
same ownership isolation. If an update callback raises, the proposer attempts
to commit the unchanged value. A confirmed quorum then raises `ProposerError`
with `code == "UpdateError"` and the original exception as its cause. If that
accept cannot be confirmed, it reports `CommitError` instead. Other unexpected
exceptions are wrapped with `code == "UnexpectedError"`.

### Atomic Acceptor Transitions

`BaseAcceptor` implements prepare and accept using
`StorageBackend.update_state(key, transition)` to read state, evaluate the
protocol rule, and persist the result atomically. `InMemoryAcceptor` inherits
these transitions and the state-access methods, supplying an independent
in-memory backend for each instance rather than maintaining duplicate methods.
The synchronous transition receives a detached state snapshot and returns
`(replacement, result)`; a `None` replacement means no write. A transition that
raises must leave stored state unchanged. The in-memory backend snapshots both
the transition input and the committed replacement. The first snapshot prevents
mutations from leaking through failed or rejected transitions; the second
prevents a retained transition result from mutating stored state. Simply
removing either snapshot does not preserve the backend's ownership contract.

Custom backends must implement this operation and serialize it against other
writes to the same stored key, including access through multiple acceptors.
Separate `get_state` and `set_state` calls are not an atomic transition.
Persistent backends need a transaction or equivalent conditional update.

The in-memory backend holds a per-key asyncio lock across the transition and
snapshot write; unrelated keys remain independent. Locks are retained across
deletion and clearing so queued operations keep using the same lock.

### Proposer Error Codes

Protocol operations raise `caspaxos.core.ProposerError` with a CamelCase `code`:

| Code | Meaning |
| --- | --- |
| `CommitError` | A write's accept quorum was not confirmed. The write may already have been applied; this is not a rollback guarantee. |
| `MaxRetriesError` | The write's prepare budget or the read's shared attempt budget was exhausted. |
| `UpdateError` | The update callback raised, and the unchanged value was committed. The callback exception is available through `__cause__`. |
| `UnexpectedError` | An unexpected exception was wrapped with its original cause. This does not guarantee rollback or retry safety. |

`CommitError` restores the original code used by existing write-error handlers.
Applications using the interim codes must replace `INDETERMINATE` with
`CommitError` and `MAX_RETRIES` with `MaxRetriesError`. These are canonical
string values, not aliases; `UpdateError` and `UnexpectedError` are unchanged.

### Read Retries

`get` retries failed prepares and unconfirmed accepts, including conflicts, lost
replies, and phase deadlines. Each retry prepares a fresh ballot and obtains the
current value again; it never reuses a value captured before an intervening write.

The `retries` argument is one shared attempt budget: an attempt performs a
prepare and, if preparation succeeds, an accept. Prepare failures and accept
failures both consume an attempt; there are no nested retry budgets. Exhaustion
raises `ProposerError` with `code == "MaxRetriesError"`, meaning no read result was
obtained, not that a client update has an indeterminate outcome. Caller
cancellation propagates rather than triggering another attempt.

### Failed or Cancelled Writes

Every `CASPaxos` / `Proposer` operation, including `get`, prepares a fresh ballot
and reads the accepted value from a quorum. No values or next-round promises are
cached. This avoids stale cached ballots when proposers alternate operations on
the same key, at the cost of a prepare phase even for repeated accesses by one
proposer. Accept requests promise only their current round's ballot.

For writes, if acceptance fails or the operation is cancelled, the next explicitly
requested operation prepares afresh. A competing write between prepare and accept
can still cause an indeterminate outcome; the update is not automatically replayed.

A failed or cancelled write may already have reached some acceptors; it is not
a rollback guarantee. Retrying a non-idempotent update is not guaranteed to
apply it exactly once.

`change(..., retries=10)` retries only preparation, before applying the update
or sending accept requests. Exhausting the prepare budget raises
`caspaxos.core.ProposerError` with `code == "MaxRetriesError"`. If an accept quorum
cannot be confirmed, it raises the same exception type with
`code == "CommitError"` and does not reapply the update. This includes accept
conflicts: other replicas may already have accepted the proposed value.
A confirmed quorum still succeeds when a minority of responses are lost.

`put` also does not retry an unconfirmed accept. A constant assignment is
idempotent in isolation, but repeating it across another writer's update is not
generally linearizable: `put(A)` may become visible, another writer may publish
`B`, and retrying `put(A)` would restore `A`, making one logical write take effect
twice. Read retries avoid this problem by obtaining the current value again.

The proposer maintains per-key ballot counters and remembers the highest
conflicting counter observed during either phase, including minority conflicts
in a successful quorum. Its next ballot exceeds that counter while retaining its own node ID.
Prepare retries therefore jump past observed promises rather than counting up
one at a time. Write accept conflicts still report an indeterminate outcome;
the remembered counter is used by the next explicit operation or automatic read retry.

An indeterminate outcome does not mean the write failed. Recover with a
consistent read and application-specific reconciliation rather than blindly
repeating a non-idempotent update. Exactly-once retries require application-level
deduplication.

### Limitations

- **Single Machine**: Current implementation runs on a single machine
- **Default Volatile Storage**: In-memory acceptors lose state on restart; opt into SQLite for persistence
- **No Network**: Acceptors communicate via method calls, not network
- **No Reconfiguration**: Static acceptor membership

## Future Enhancements

1. **Redis Backend**: Persistent acceptor state using Redis
2. **Network Communication**: TCP/HTTP based acceptor protocol
3. **Cluster Membership**: Dynamic addition/removal of acceptors
4. **Monitoring**: Metrics and observability
5. **Optimizations**: Batching, pipelining, and lease-based reads

## References

- [CASPaxos Paper (arXiv:1802.07000)](https://arxiv.org/abs/1802.07000)
- [Gryadka - JavaScript Implementation](https://github.com/gryadka/js)
- [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)

## License

See [LICENSE](LICENSE)

## Contributing

This is an educational implementation. Contributions that improve clarity, correctness, or educational value are welcome!

1. Write tests for new features
2. Maintain clean, readable code
3. Add docstrings and type hints
4. Update documentation
