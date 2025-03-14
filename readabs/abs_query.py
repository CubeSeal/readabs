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
    """Type for Catalogue No."""
    def __init__(self: CatNo, catno: str) -> None:
        self.catno = catno

class SeriesID:
    """Type for Series ID."""
    def __init__(self: SeriesID, series_id: str) -> None:
        self.series_id = series_id
            
# Exception Class
class ABSQueryError(Exception):
    """Exception for ABSQuery class."""
    pass

# Class
class ABSQuery:
    """
    Class provides methods to handle ABS Time Series data in python.
    Can profile ABS timeseries dictionary "https://abs.gov.au:443/servlet/TSSearchServlet" for approrpriate ABS series,
    get table names and download time series data as a pandas dataframe.

    Attributes:
        id: A type-guarded attribute for either the Catalogue No. or the Series ID. If both are provided then Catalogue
            No. is preferred.
        table_tile: The table title (if provided) in the format the ABS API likes. 
        series_list: list of series returned by the call to the ABS time series dictionary. 

    Methods:
        get_table_names(): Get the list of available tables in the series list.
        get_table_links(): Get the download link for a given table name.
        get_dataframe(): Download the dataframe using the link.  

    Typical usage example:
        # Create ABSQuery object with either Catalogue No. or Series ID. 
        cpi_query: ABSQuery = ABSQuery("6401.0")

        # Inspect the table names available for CPI data.
        table_names: set[str] = cpi_query.get_table_names()
        print(table_names)

        # Get link and download.
        table_link: dict[str, str] = cpi_query.get_table_links("TABLES 1 and 2.")
        data_list: list[pd.DataFrame] 

        for _, values in table_link:
            data_list.append(cpi_query.get_dataframe(values))
        
    """
    _base_query: str = r"https://abs.gov.au:443/servlet/TSSearchServlet\?"

    def __init__(self: ABSQuery, catno: str | None = None, seriesID: str | None = None, table_title: str | None = None) -> None:
        """
        ABSQuery constructor.

        Args:
            * Note if both catno and seriesID are None then an exception will be raised.
            catno (Optional): String for ABS Catalogue No. Must contain '.0' as suffix. E.g. "6401.0". Preferred over `seriesID` if both
                supplied.
            seriesID (Optional): String for Series ID. 
            table_title (Optional): String in the format the Timeseries Dictionary likes.
        """
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
        """
        Construct query url from internal attributes and supplied page number. 

        Args:
            pg (Optional): Page Number.

        Returns:
            A url string with the appropriate query parameters.
        """
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
        """
        Gets the XML response for a given ABSQuery() object.

        Returns:
            ElementTree XML object with data from all pages of timeseries dict for ABSQuery().
        """
        xml_query: str = self._construct_query()

        pg_1: ABSXML = ABSXML(ET.fromstring(req.get(xml_query).text))
        return_element: ABSXML = pg_1

        num_pages_elem: ET.Element | None = pg_1.find('NumPages')
        num_pages: str | None = num_pages_elem.text if num_pages_elem is not None else None

        if num_pages is not None:
            print(f"\nFound {num_pages} pages for this id in the ABS time series dictionary. Downloading all pages...", end = '')

            for i in range (2, int(num_pages) + 1):
                print(f"{i}, ", end = '')
                _xml_query: str = self._construct_query(pg = i)
                _xml_result: str = req.get(_xml_query).text
                return_element.append(ET.fromstring(_xml_result))
                
        return pg_1

    def _get_serieslist(self: ABSQuery) -> None:
        """
        Gets list of dictionaries, each representing one series in the Timeseries Dictionary. Include all parameters
        available in the xml dynamically. (TODO: Maybe statically enforce this?). Modifies self.series_list directly.
        Called only once because it is expensive.

        Raises:
            ABSQueryError: If entry in series is not populated.
        """
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
        """
        Get all available table names for an ABSQuery object.

        Returns:
            A set with all unique table names available in the Timeseries Dictionary for the given ABSQuery parameters.
        """
        self._get_serieslist()

        return set([elem['TableTitle'] for elem in series_list]) if (series_list := self.series_list) else None

    def get_table_links(self: ABSQuery, table_title: str) -> dict[str, str] | None:
        """
        Gets all the table links for a given table_title query. Will try to match everything where table_title is in
        TimeSeries Dictionary.

        Args:
            table_title: A string. Is matched against table titles with `in` keywords
        
        Returns:
            A dictionary of TableTitle's and TableURL's.
        """
        self._get_serieslist()
        return_value: dict[str, str] | None = None

        if series_list := self.series_list:
            return_value = {elem['TableTitle']: elem['TableURL'] for elem in series_list if table_title in elem['TableTitle']}
        else:
            return_value = None

        return return_value

    @classmethod
    def get_dataframe(cls: Type[ABSQuery], table_url: str) -> pd.DataFrame:
        """
        Downloads pandas dataframe with given table_url.
        *Note: I deliberately kept this separate from the object to enforce a distinction between getting the table
           url and downloading the data. Not exactly sure why though, maybe I'll remember.

        Args:
            table_url: URL string to excel file to download.

        Returns:
            A Pandas Dataframe with the contents of the excel file stitched together.
        """
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
        """Formatting wrapper for ABS Excel Sheets, which are quite funky."""
        return (
            df
            .pipe(cls._rename_cols)
            .pipe(cls._remove_ABS_headers)
        )

    @staticmethod
    def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
        """Renames the weird auto-name for the data column"""
        return df.rename({"Unnamed: 0": "Date"}, axis = 1)

    @staticmethod
    def _remove_ABS_headers(df: pd.DataFrame) -> pd.DataFrame:
        """Removes obnoxious headers in ABS Excel sheets."""
        date_col: pd.Series | None = s if isinstance((s := df['Date']), pd.Series) else None

        if date_col is not None:
            rows_not_headers: list[bool] = [isinstance(e, datetime.datetime) for e in date_col]
            return df.loc[rows_not_headers]
        else:
            raise ABSQueryError("Can't find renamed date_col date column from _rename_cols().")
