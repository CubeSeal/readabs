import pytest

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

def test_xml_string():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query._get_ts_dict_xml(), str)

def test_xml_return():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")
    error_str: str = '<?xml version="1.0" encoding="utf-8" ?><Error>Invalid query.</Error>\r\n'

    assert abs_query._get_ts_dict_xml() != error_str 
