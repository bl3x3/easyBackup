"""In-process event broker for WebSocket progress delivery."""

from __future__ import annotations

import asyncio
import itertools
from datetime import datetime, timezone
from typing import Any


class EventBus:
    def __init__(self, queue_size: int = 256):
        self.queue_size = queue_size
        self._subscribers: set[asyncio.Queue] = set()
        self._sequence = itertools.count(1)

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def publish(self, event_type: str, data: dict[str, Any]) -> None:
        event = {
            "type": event_type,
            "sequence": next(self._sequence),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        for queue in tuple(self._subscribers):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

