import pandas as pd
import xml.etree.ElementTree as ET
import requests as req

import pytest

import readabs.abs_query as module

ABS_URL = "https://ausstats.abs.gov.au/servlet/TSSearchServlet?"

def test_get_data():
    response: req.models.Response = req.get(ABS_URL)

    assert response is not None

def test_get_data_type():
    response: req.models.Response = req.get(ABS_URL)

    assert isinstance(response, req.models.Response)

def test_abs_conn_status():
    response: req.models.Response = req.get(ABS_URL)

    assert response.status_code == 200

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
    abs_query._get_serieslist()

    series_list = abs_query.series_list
    
    assert series_list != []

def test_get_table_names():
    abs_query: module.ABSQuery = module.ABSQuery("6401.0")
    
    # Empty check for sets
    assert abs_query.get_table_names()

def test_get_table_link():
    abs_query: module.ABSQuery = module.ABSQuery("6401.0")
    table_links: dict[str, str] | None = abs_query.get_table_link("TABLES 1 and 2.")

    assert isinstance(table_links, dict)
    assert table_links # Zero length test

def test_get_dataframe():
    abs_query: module.ABSQuery = module.ABSQuery("6401.0")
    table_link: dict[str, str] | None = abs_query.get_table_link("TABLES 1 and 2.")

    if table_link:
        for _, value in table_link.items():
            df: pd.DataFrame = abs_query.get_dataframe(value)

            print(df.head())
            assert isinstance(df, pd.DataFrame)
    else:
            raise AssertionError("table_link is None")
