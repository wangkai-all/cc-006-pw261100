"""Pointwise device metrics service.

Continuously ingests JSONL device events, deduplicates them by event id,
tracks per-device/per-minute aggregate metrics under an event-time watermark,
versions every metric change, and finalizes windows once the watermark has
passed the window end plus a grace period.
"""

__all__ = ["schema", "models", "processor", "tailer", "server", "cli"]
