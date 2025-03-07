from __future__ import annotations
from typing import NewType
from io import BytesIO

import openpyxl as xlsx
import pandas as pd
import readabs.connection as conn
import xml.etree.ElementTree as ET

import re

# Types
CatNo = NewType('CatNo', str)
SeriesID = NewType('SeriesID', str)
ABSXML = NewType('ABSXML', ET.Element)

# Exception Class
class ABSQueryError(Exception):
    pass

# Class

class ABSQuery:
    _base_query: str = r"https://abs.gov.au:443/servlet/TSSearchServlet\?"

    def __init__(self: ABSQuery, catno: str | None = None, seriesID: str | None = None):

        self.catno: CatNo | None = None
        self.seriesID: SeriesID | None = None
       
        # These should be mutually exclusive.
        if catno is not None:
            if re.search(r"\.0$", catno) is None:
                raise ABSQueryError("catno must end in '.0'")

            self.catno = CatNo(catno)
            self.seriesID = None

        else:
            if seriesID is not None:
                self.catno = None
                self.seriesID = SeriesID(seriesID)
            else:
                raise ABSQueryError("Either catno or seriesID must be provided")

    def _construct_query(self: ABSQuery, ttitle: str | None = None, pg: int | None = None) -> str:
        out_str: list[str | None] = []

        if ttitle is not None:
            out_str.append(f"ttitle={ttitle}")

        if pg is not None:
            out_str.append(f"pg={pg}")

        if self.catno is not None:
            out_str.append(f"catno={self.catno}")

        if self.seriesID is not None:
            out_str.append(f"sid={self.seriesID}")

        return self._base_query + '&'.join([e for e in out_str if e is not None])

    def _get_timeseries_dict_xml(self: ABSQuery) -> ABSXML:
        xml_query: str = self._construct_query()
        pg_1: ABSXML = ABSXML(ET.fromstring(conn._get_data(xml_query).text))
        return_element: ABSXML = pg_1

        num_pages_elem: ET.Element | None = pg_1.find('NumPages')
        num_pages: str | None = num_pages_elem.text if num_pages_elem is not None else None
        if num_pages is not None:
            print(f"\nFound {num_pages} pages for this id in the ABS time series dictionary. Downloading all pages...")

            for i in range (2, int(num_pages) + 1):
                print(f"{i}, ", end = '')
                _xml_query: str = self._construct_query(pg = i)
                _xml_result: str = conn._get_data(_xml_query).text
                return_element.append(ET.fromstring(_xml_result))
                
        return return_element

    def _get_serieslist(self: ABSQuery) -> list[dict[str, str]]:
        series_list: list[dict[str, str]] = []
        xml: ET.Element = self._get_timeseries_dict_xml()

        for series in xml.iter('Series'):
            series_dict: dict[str, str] = {}

            for child in series:
                if child.text is not None:
                    series_dict[child.tag] = child.text 
                else:
                    raise ABSQueryError(f"No text found for child tag {child.tag}")

            series_list.append(series_dict)

        return series_list

    def get_table_names(self: ABSQuery) -> list[str]:
        return [elem['TableTitle'] for elem in self._get_serieslist()]

    def get_table_link(self: ABSQuery, table_title: str) -> dict[str, str]:
        series_list: list[dict[str,str]] = self._get_serieslist()

        return {elem['TableTitle']: elem['TableURL'] for elem in series_list if table_title in elem['TableTitle']} 

    def get_dataframes(self: ABSQuery, table_url: str) -> list[pd.DataFrame]:
        workbook_bytes: BytesIO = BytesIO(conn._get_data(table_url).content)
        workbook: xlsx.Workbook = xlsx.load_workbook(workbook_bytes)

        print("\nThe sheet names in the excel are below:")
        for names in workbook.sheetnames:
            print(names)

        print("\nFiltering all with 'Data'")

        df_list: list[pd.DataFrame] = [pd.read_excel(workbook_bytes, sheet_name = s) for s in workbook.sheetnames if 'Data' in s]

        remove_headers: list[pd.DataFrame] = [df.drop(index = df.index[1:9]).reset_index() for df in df_list] #type: ignore

        return remove_headers
