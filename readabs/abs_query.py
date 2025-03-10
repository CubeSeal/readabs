from __future__ import annotations
from typing import NewType, Type
from io import BytesIO

import openpyxl as xlsx
import pandas as pd
import xml.etree.ElementTree as ET
import requests as req

import re
import datetime

# Types
ABSXML = NewType('ABSXML', ET.Element)
ABSSeries = NewType('ABSSeries', dict[str, str])

# ID Types
class CatNo:
    def __init__(self: CatNo, catno: str) -> None:
        self.catno = catno

class SeriesID:
    def __init__(self: SeriesID, series_id: str) -> None:
        self.series_id = series_id
            
# Exception Class
class ABSQueryError(Exception):
    pass

# Class
class ABSQuery:
    _base_query: str = r"https://abs.gov.au:443/servlet/TSSearchServlet\?"

    def __init__(self: ABSQuery, catno: str | None = None, seriesID: str | None = None, table_title: str | None = None):

        self.id: CatNo | SeriesID
        self.table_title: str | None = table_title
        # Not set in __init__, but in _get_serieslist() because expensive.
        self.series_list: list[ABSSeries] | None = None
       
        # Checking sum types here
        # Alternative: enforce the sum type in the call (a bit annoying to use though, but no exceptions).
        if catno:
            if re.search(r"\.0$", catno):
                self.id = CatNo(catno)
            else:
                raise ABSQueryError("catno must end in '.0'")

        elif seriesID:
            self.id = SeriesID(seriesID)

        else:
            raise ABSQueryError("Either catno or seriesID must be provided")

    def _construct_query(self: ABSQuery, pg: int | None = None) -> str:
        out_str: list[str | None] = []

        if self.table_title:
            out_str.append(f"ttitle={self.table_title}")

        if pg:
            out_str.append(f"pg={pg}")

        if isinstance(self.id, CatNo):
            out_str.append(f"catno={self.id.catno}")

        if isinstance(self.id, SeriesID):
            out_str.append(f"sid={self.id.series_id}")

        return self._base_query + '&'.join([e for e in out_str if e])

    def _get_timeseries_dict_xml(self: ABSQuery) -> ABSXML:
        xml_query: str = self._construct_query()
        pg_1: ABSXML = ABSXML(ET.fromstring(req.get(xml_query).text))
        return_element: ABSXML = pg_1

        num_pages_elem: ET.Element | None = pg_1.find('NumPages')
        num_pages: str | None = num_pages_elem.text if num_pages_elem else None

        if num_pages:
            print(f"\nFound {num_pages} pages for this id in the ABS time series dictionary. Downloading all pages...", end = '')

            for i in range (2, int(num_pages) + 1):
                print(f"{i}, ", end = '')
                _xml_query: str = self._construct_query(pg = i)
                _xml_result: str = req.get(_xml_query).text
                return_element.append(ET.fromstring(_xml_result))
                
        return return_element

    def _get_serieslist(self: ABSQuery) -> None:
        series_list: list[ABSSeries] = []
        
        if not self.series_list:
            xml: ET.Element = self._get_timeseries_dict_xml()

            for series in xml.iter('Series'):
                series_dict: ABSSeries = ABSSeries({})

                for child in series:
                    if child.text:
                        series_dict[child.tag] = child.text 
                    else:
                        raise ABSQueryError(f"No text found for child tag {child.tag}")

                series_list.append(series_dict)

            self.series_list = series_list

    def get_table_names(self: ABSQuery) -> set[str] | None:
        self._get_serieslist()

        return set([elem['TableTitle'] for elem in series_list]) if (series_list := self.series_list) else None

    def get_table_link(self: ABSQuery, table_title: str) -> dict[str, str] | None:
        self._get_serieslist()
        return_value: dict[str, str] | None = None

        if series_list := self.series_list:
            return_value = {elem['TableTitle']: elem['TableURL'] for elem in series_list if table_title in elem['TableTitle']}
        else:
            return_value = None

        return return_value

    @classmethod
    def get_dataframe(cls: Type[ABSQuery], table_url: str) -> pd.DataFrame:
        workbook_bytes: BytesIO = BytesIO(req.get(table_url).content)
        workbook: xlsx.Workbook = xlsx.load_workbook(workbook_bytes)

        print("\nThe sheet names in the excel are below:")
        for names in workbook.sheetnames:
            print(names)

        print("\nFiltering all with 'Data'")
        df_list: list[pd.DataFrame] = [pd.read_excel(workbook_bytes, sheet_name = s) for s in workbook.sheetnames if 'Data' in s]

        print("\nFormatting dataframes...")
        remove_headers: list[pd.DataFrame] = [cls._format_ABS_df(df) for df in df_list]

        return pd.concat(remove_headers, axis = 1)

    @classmethod
    def _format_ABS_df(cls: Type[ABSQuery], df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(cls._rename_cols)
            .pipe(cls._remove_ABS_headers)
        )

    @staticmethod
    def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename({"Unnamed: 0": "Date"}, axis = 1)

    @staticmethod
    def _remove_ABS_headers(df: pd.DataFrame) -> pd.DataFrame:
        date_col: pd.Series | None = s if isinstance((s := df['Date']), pd.Series) else None

        if isinstance(date_col, pd.Series):
            headers_to_keep: list[bool] = [isinstance(e, datetime.datetime) for e in date_col]
            return df.loc[headers_to_keep]
        else:
            raise ABSQueryError("Can't find renamed date_col date column from _rename_cols().")
