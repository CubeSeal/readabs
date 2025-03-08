from __future__ import annotations
from typing import NewType
from io import BytesIO

import openpyxl as xlsx
import pandas as pd
import readabs.connection as conn
import xml.etree.ElementTree as ET

import re
import datetime

# Types
CatNo = NewType('CatNo', str)
SeriesID = NewType('SeriesID', str)
ABSXML = NewType('ABSXML', ET.Element)
ABSSeries = NewType('ABSSeries', dict[str, str])

# Exception Class
class ABSQueryError(Exception):
    pass

# Class

class ABSQuery:
    _base_query: str = r"https://abs.gov.au:443/servlet/TSSearchServlet\?"

    def __init__(self: ABSQuery, catno: str | None = None, seriesID: str | None = None, table_title: str | None = None):

        self.catno: CatNo | None = None
        self.seriesID: SeriesID | None = None
        self.table_title: str | None = table_title

        # Not set in __init__, but in _get_serieslist() because expensive.
        self.series_list: list[ABSSeries] | None = None
       
        # These should be mutually exclusive.
        if catno is not None:
            if re.search(r"\.0$", catno) is None:
                raise ABSQueryError("catno must end in '.0'")

            self.catno = CatNo(catno)
            self.seriesID = None

        elif seriesID is not None:
            self.catno = None
            self.seriesID = SeriesID(seriesID)
        else:
            raise ABSQueryError("Either catno or seriesID must be provided")

    def _construct_query(self: ABSQuery, pg: int | None = None) -> str:
        out_str: list[str | None] = []

        if self.table_title is not None:
            out_str.append(f"ttitle={self.table_title}")

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
            print(f"\nFound {num_pages} pages for this id in the ABS time series dictionary. Downloading all pages...", end = '')

            for i in range (2, int(num_pages) + 1):
                print(f"{i}, ", end = '')
                _xml_query: str = self._construct_query(pg = i)
                _xml_result: str = conn._get_data(_xml_query).text
                return_element.append(ET.fromstring(_xml_result))
                
        return return_element

    def _get_serieslist(self: ABSQuery) -> None:
        series_list: list[ABSSeries] = []
        
        if self.series_list is None:
            xml: ET.Element = self._get_timeseries_dict_xml()

            for series in xml.iter('Series'):
                series_dict: ABSSeries = ABSSeries({})

                for child in series:
                    if child.text is not None:
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

    @staticmethod
    def get_dataframe(table_url: str) -> pd.DataFrame:
        workbook_bytes: BytesIO = BytesIO(conn._get_data(table_url).content)
        workbook: xlsx.Workbook = xlsx.load_workbook(workbook_bytes)

        print("\nThe sheet names in the excel are below:")
        for names in workbook.sheetnames:
            print(names)

        print("\nFiltering all with 'Data'")

        df_list: list[pd.DataFrame] = [pd.read_excel(workbook_bytes, sheet_name = s) for s in workbook.sheetnames if 'Data' in s]

        remove_headers: list[pd.DataFrame] = [ABSQuery._format_ABS_df(df) for df in df_list]
        
        concat_df: pd.DataFrame = pd.concat(remove_headers, axis = 1)

        return concat_df

    @staticmethod
    def _format_ABS_df(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(ABSQuery._rename_cols)
            .pipe(ABSQuery._remove_ABS_headers)
        )

    @staticmethod
    def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename({"Unnamed: 0": "Date"}, axis = 1)

    @staticmethod
    def _remove_ABS_headers(df: pd.DataFrame) -> pd.DataFrame:
        date_col: pd.Series | None = s if isinstance((s := df['Date']), pd.Series) else None

        if date_col is not None:
            headers_to_drop: list[bool] = [isinstance(e, datetime.datetime) for e in date_col]
            return df.loc[headers_to_drop]
        else:
            raise ABSQueryError("Can't find renamed date_col date column from _rename_cols().")
