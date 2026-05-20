import pytest
import sys
from pathlib import Path

import importlib

# Dynamically import the module since it starts with a number
module = importlib.import_module("src.01_prepare_splink")
_strip_diacriticals = module._strip_diacriticals
_tokens = module._tokens
arc_name_arrays = module.arc_name_arrays
oax_name_arrays = module.oax_name_arrays

def test_strip_diacriticals():
    assert _strip_diacriticals("Müller") == "Muller"
    assert _strip_diacriticals("François") == "Francois"
    assert _strip_diacriticals("El Niño") == "El Nino"
    assert _strip_diacriticals("Smith-Jones") == "Smith-Jones"
    # Test exotic hyphens
    assert _strip_diacriticals("Smith‑Jones") == "Smith-Jones"
    assert _strip_diacriticals("") == ""
    assert _strip_diacriticals(None) == ""

def test_tokens():
    assert _tokens("Jean-Luc") == ["jean", "luc"]
    assert _tokens("O'Connor") == ["o", "connor"]
    assert _tokens("D'Arcy-Smith") == ["d", "arcy", "smith"]
    assert _tokens("  Dr. John   Doe, Jr. ") == ["dr", "john", "doe", "jr"]

def test_arc_name_arrays():
    # Basic
    res = arc_name_arrays("Jean-Luc", "Picard")
    assert set(res["first_names"]) == {"jean", "luc", "j", "l", "p"}
    assert set(res["family_names"]) == {"picard"}
    
    # Missing first name (Mononym behavior)
    res = arc_name_arrays("", "Cher")
    assert set(res["first_names"]) == {"c", "cher"}
    assert set(res["family_names"]) == {"cher"}

    # Complex name
    res = arc_name_arrays("Mary Jane", "Watson-Parker")
    assert set(res["first_names"]) == {"mary", "jane", "m", "j", "w", "p"}
    assert set(res["family_names"]) == {"watson", "parker"}

def test_oax_name_arrays():
    # Basic
    res = oax_name_arrays("Jean-Luc Picard", ["J.L. Picard", "JL Picard"])
    assert "jean" in res["first_names"]
    assert "luc" in res["first_names"]
    assert "j" in res["first_names"]
    assert "l" in res["first_names"]
    assert "picard" in res["family_names"]
    
    # Mononym logic (where nameparser puts the only token into last)
    # The UDF should add its initial to first_names just in case
    res = oax_name_arrays("Cher", [])
    assert "cher" in res["family_names"]
    assert "c" in res["first_names"]

def test_oax_name_arrays_empty():
    res = oax_name_arrays(None, None)
    assert res["first_names"] == []
    assert res["family_names"] == []
