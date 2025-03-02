import pytest
import xml.etree.ElementTree as ET

import readabs.abs_query as module

def test_abs_query_exists():
    
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query, module.ABSQuery)

def test_inappropriate_catNo():
    with pytest.raises(module.ABSQueryError):
        abs_query: module.ABSQuery = module.ABSQuery("4349")

def test_both_none():
    with pytest.raises(module.ABSQueryError):
        abs_query: module.ABSQuery = module.ABSQuery()

def test_xml_query():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query._construct_ts_dict_query(), str)

def test_xml_type():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query._get_ts_dict_xml(), ET.Element)

def test_xml_return():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")
    error_str: str = '<?xml version="1.0" encoding="utf-8" ?><Error>Invalid query.</Error>\r\n'

    assert abs_query._get_ts_dict_xml().__str__ != error_str 

def test_xml_series():
    abs_query: module.ABSQuery = module.ABSQuery("6401.0")

    assert isinstance(abs_query.getSeriesList(), list)
    assert abs_query.getSeriesList() != []
