import pytest
from parsing.parse import Parser

def test_get_orders():
    p = Parser()
    assert p.get_serialized_offers()