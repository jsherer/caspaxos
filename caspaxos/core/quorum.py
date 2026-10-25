"""Collect a quorum without waiting for outstanding minority requests."""

import asyncio
from typing import Any, Awaitable, Callable, List, Set


# Keep requests alive until they actually finish, including handlers that delay
# cancellation. Their eventual exceptions must never become unhandled task errors.
_outstanding: Set[asyncio.Future] = set()


def _finished(request: asyncio.Future) -> None:
    _outstanding.discard(request)
    if not request.cancelled():
        request.exception()


async def collect_responses(
    requests: List[Awaitable[Any]],
    quorum_size: int,
    is_success: Callable[[Any], bool],
    timeout: float,
) -> List[Any]:
    """Return completed responses on quorum, impossibility, or phase deadline.

    Exceptions count as failures and remain in the response list. A request
    cancelled independently contributes no response. Caller cancellation still
    propagates. Outstanding calls are cancelled without awaiting their cleanup,
    so a slow cancellation handler cannot hold up a completed phase.
    """
    pending = {asyncio.ensure_future(request) for request in requests}
    _outstanding.update(pending)
    for request in pending:
        request.add_done_callback(_finished)

    responses = []
    successful = 0
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        while pending and successful < quorum_size:
            if successful + len(pending) < quorum_size:
                break
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break
            for request in done:
                if request.cancelled():
                    continue
                try:
                    response = request.result()
                except Exception as error:
                    response = error
                responses.append(response)
                if not isinstance(response, Exception) and is_success(response):
                    successful += 1
        return responses
    finally:
        for request in pending:
            request.cancel()
