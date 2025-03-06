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
ABSXML = NewType('ABSXML', str)

# Exception Class
class ABSQueryError(Exception):
    pass

# Class

class ABSQuery:
    _base_query: str = r"https://abs.gov.au:443/servlet/TSSearchServlet\?"

    def __init__(self: ABSQuery, catNo: str | None = None, seriesID: str | None = None):

        self.catNo: CatNo | None = None
        self.seriesID: SeriesID | None = None
       
        # These should be mutually exclusive.
        if catNo is not None:
            if re.search(r"\.0$", catNo) is None:
                raise ABSQueryError("catNo must end in '.0'")

            self.catNo = CatNo(catNo)
            self.seriesID = None

        else:
            if seriesID is not None:
                self.catNo = None
                self.seriesID = SeriesID(seriesID)
            else:
                raise ABSQueryError("Either catNo or seriesID must be provided")

    def _construct_ts_dict_query(self: ABSQuery) -> str:
        id_str: str = f"catno={self.catNo}" if self.catNo is not None else f"sid={self.seriesID}"

        return self._base_query + id_str

    def _get_ts_dict_xml(self: ABSQuery) -> ET.Element:
        xml_query: str = self._construct_ts_dict_query()

        return_xml: str = ABSXML(conn._get_data(xml_query).text)
        return ET.fromstring(return_xml)

    def _get_serieslist(self: ABSQuery) -> list[dict[str, str]]:
        series_list: list[dict[str, str]] = []
        xml: ET.Element = self._get_ts_dict_xml()

        for series in xml:
            series_dict: dict[str, str] = {}

            for child in series:
                if child.text is not None:
                    series_dict[child.tag] = child.text 
                else:
                    raise ABSQueryError(f"No tag found for child {child}")

            series_list.append(series_dict)

        return series_list

    def get_table_links(self: ABSQuery) -> dict[str, str]:
        # Goofy shit python.
        series_list: list[dict[str, str]] = [e for e in self._get_serieslist() if e]
        return_dict: dict[str, str] = {}

        for series in series_list:
            table_name: str = series['TableTitle']
            table_url: str = series['TableURL']

            return_dict[table_name] = table_url

        return return_dict

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
