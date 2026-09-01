"""Parse a placement ring from free-text into [(shard, cell_id, region)] slots."""
from __future__ import annotations


def parse_ring(text: str) -> list[tuple[int, str, str]] | None:
    """Slots are separated by commas, spaces, or newlines; each is ``shard=cellId[@region]``
    (e.g. ``0=cellA, 1=cellB`` or one per line). Returns None on any malformed token."""
    ring: list[tuple[int, str, str]] = []
    for tok in text.replace(",", " ").split():
        if "=" not in tok:
            return None
        shard, rest = tok.split("=", 1)
        cell, region = (rest.split("@", 1) + [""])[:2]
        if not cell.strip():
            return None
        try:
            ring.append((int(shard.strip()), cell.strip(), region.strip()))
        except ValueError:
            return None
    return ring or None
