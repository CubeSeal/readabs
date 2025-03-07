import pytest

import pandas as pd
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

def test_abs_query():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query._construct_query(), str)

def test_xml_type():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query._get_timeseries_dict_xml(), ET.Element)

def test_xml_return():
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")
    error_str: str = '<?xml version="1.0" encoding="utf-8" ?><Error>Invalid query.</Error>\r\n'

    assert abs_query._get_timeseries_dict_xml().__str__ != error_str 

def test_get_serieslist():
    abs_query: module.ABSQuery = module.ABSQuery("6401.0")
    series_list: list[dict[str, str]] = abs_query._get_serieslist()
    
    assert series_list != []

def test_get_table_names():
    abs_query: module.ABSQuery = module.ABSQuery("6401.0")
    
    assert abs_query.get_table_names() != []

# def test_get_table_links():
#     abs_query: module.ABSQuery = module.ABSQuery("6401.0")
#     table_links: dict[str, str] = abs_query.get_table_links()
# 
#     assert isinstance(table_links, dict)
#     assert table_links # Zero length test
# 
# def test_get_dataframes():
#     abs_query: module.ABSQuery = module.ABSQuery("6401.0")
#     table_links: dict[str, str] = abs_query.get_table_links()
# 
#     for _, value in table_links.items():
#         df: list[pd.DataFrame] = abs_query.get_dataframes(value)
#         assert isinstance(df, list)
