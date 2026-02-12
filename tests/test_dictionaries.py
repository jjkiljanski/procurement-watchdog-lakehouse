"""Tests for the BZP dictionary loader."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.dictionaries import client_type_names, order_type_names, province_names


class TestProvinceNames:
    def test_returns_16_provinces(self):
        d = province_names()
        assert len(d) == 16

    def test_known_mapping(self):
        d = province_names()
        assert d["PL14"] == "mazowieckie"
        assert d["PL02"] == "dolnośląskie"

    def test_all_keys_start_with_pl(self):
        for key in province_names():
            assert key.startswith("PL")


class TestClientTypeNames:
    def test_has_entries(self):
        d = client_type_names()
        assert len(d) > 30

    def test_known_leaf(self):
        d = client_type_names()
        assert d["1.1.2"] == "jednostka samorządu terytorialnego"

    def test_known_intermediate_node(self):
        d = client_type_names()
        assert d["1"] == "Zamawiający publiczny"

    def test_deeply_nested(self):
        d = client_type_names()
        assert "1.1.1.1" in d


class TestOrderTypeNames:
    def test_returns_3_types(self):
        d = order_type_names()
        assert len(d) == 3

    def test_known_mapping(self):
        d = order_type_names()
        assert d["Delivery"] == "Dostawy"
        assert d["Services"] == "Usługi"
        assert d["Works"] == "Roboty budowlane"
