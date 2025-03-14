from pathlib import Path

import pandas as pd
import xml.etree.ElementTree as ET
import requests as req
import pytest_mock as mock

import pytest
import readabs.abs_query as module

ABS_URL: str = "https://ausstats.abs.gov.au/servlet/TSSearchServlet?"
EXAMPLE_CAT_NO: str = "6401.0"
EXAMPLE_TABLE: str = "TABLES 1 and 2."
RES_PATH: Path = Path(__file__).parent / 'res' / 'sample_timeseries_dict.xml'
XML_TEXT: ET.Element = ET.fromstring(RES_PATH.read_text())

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
    
    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)

    assert isinstance(abs_query, module.ABSQuery)

def test_inappropriate_catNo():
    with pytest.raises(module.ABSQueryError):
        module.ABSQuery("4349")

def test_both_none():
    with pytest.raises(module.ABSQueryError):
        module.ABSQuery()

def test_construct_query():
    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)

    assert isinstance(abs_query._construct_query(), str)

def test_timeseries_dict_xml(mocker: mock.MockerFixture):
    mock_response = XML_TEXT

    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)
    mocker.patch.object(abs_query, attribute='_get_timeseries_dict_xml', return_value = mock_response)

    assert isinstance(abs_query._get_timeseries_dict_xml(), ET.Element)

def test_xml_return(mocker: mock.MockerFixture):
    """FIXME: Add alternate file for this function, because right now this tests fucking nothing."""
    mock_response = XML_TEXT

    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)
    mocker.patch.object(abs_query, attribute='_get_timeseries_dict_xml', return_value = mock_response)
    error_str: str = '<?xml version="1.0" encoding="utf-8" ?><Error>Invalid query.</Error>\r\n'

    assert abs_query._get_timeseries_dict_xml().__str__ != error_str 

def test_get_serieslist(mocker: mock.MockerFixture):
    mock_response = XML_TEXT

    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)
    mocker.patch.object(abs_query, attribute='_get_timeseries_dict_xml', return_value = mock_response)
    abs_query._get_serieslist()

    series_list = abs_query.series_list
    
    assert series_list != []

def test_get_table_names(mocker: mock.MockerFixture):
    mock_response = XML_TEXT

    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)
    mocker.patch.object(abs_query, attribute='_get_timeseries_dict_xml', return_value = mock_response)
    table_names: set | None = abs_query.get_table_names()
    
    # Empty check for sets
    assert table_names

def test_get_table_links(mocker: mock.MockerFixture):
    mock_response = XML_TEXT

    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)
    mocker.patch.object(abs_query, attribute='_get_timeseries_dict_xml', return_value = mock_response)
    table_links: dict[str, str] | None = abs_query.get_table_links(EXAMPLE_TABLE)

    assert isinstance(table_links, dict)
    assert table_links # Zero length test

def test_get_dataframe():
    table_link: dict[str, str] = {
        'TABLES 1 and 2. CPI: All Groups, Index Numbers and Percentage Changes'
        : 'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia/latest-release/640101.xlsx' 
    }

    abs_query: module.ABSQuery = module.ABSQuery(EXAMPLE_CAT_NO)

    if table_link:
        for _, value in table_link.items():
            df: pd.DataFrame = abs_query.get_dataframe(value)

            print(df.head())
            assert isinstance(df, pd.DataFrame)
    else:
            raise AssertionError("table_link is None")
