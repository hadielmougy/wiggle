"""Regression: a comma-separated ring on one line (`0=cellA,1=cellB`) must parse into TWO slots,
not one bogus slot with cell_id 'cellA,1=cellB' (which poisoned the ring and broke allocate)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wigglelab.ringspec import parse_ring


def test_comma_separated_one_line():
    assert parse_ring("0=cellA,1=cellB") == [(0, "cellA", ""), (1, "cellB", "")]


def test_spaces_and_newlines_and_regions():
    assert parse_ring("0=cellA  1=cellB") == [(0, "cellA", ""), (1, "cellB", "")]
    assert parse_ring("0=cellA\n1=cellB") == [(0, "cellA", ""), (1, "cellB", "")]
    assert parse_ring("0=cellA@eu, 1=cellB@us") == [(0, "cellA", "eu"), (1, "cellB", "us")]


def test_malformed_returns_none():
    assert parse_ring("cellA") is None        # no '='
    assert parse_ring("x=cellA") is None       # non-int shard
    assert parse_ring("0=") is None            # empty cell
    assert parse_ring("") is None              # empty


if __name__ == "__main__":
    test_comma_separated_one_line()
    test_spaces_and_newlines_and_regions()
    test_malformed_returns_none()
    print("ok")
