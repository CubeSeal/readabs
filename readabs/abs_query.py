from __future__ import annotations
from typing import NewType, Type
from io import BytesIO
from functools import reduce

import openpyxl as xlsx
import pandas as pd
import xml.etree.ElementTree as ET

import re
import datetime
import asyncio
import nest_asyncio

import readabs.connection as conn

# Nesting asyncio to get it working in Jupyter notebooks.
nest_asyncio.apply()

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
        get_dataframe(): Searched for table name and downloads matching dataframes.
        get_first_dataframe(): Simple wrapper that gets the first dataframe from get_dataframe().

    Typical usage example:
        # Create ABSQuery object with either Catalogue No. or Series ID. 
        cpi_query: ABSQuery = ABSQuery("6401.0")

        # Inspect the table names available for CPI data.
        table_names: set[str] = cpi_query.get_table_names()
        print(table_names)

        # Get links
        table_links: dict[str, str] = cpi_query.get_table_links()
        print(table_links)

        # Download dataframes
        data: pd.DataFrame = query.get_first_dataframe("TABLES 1 and 2.")
    """
    _base_query: str = r"https://abs.gov.au:443/servlet/TSSearchServlet\?"

    def __init__(
        self: ABSQuery,
        catno: str | None = None,
        seriesID: str | None = None,
        table_title: str | None = None,
        getter: conn.HTTPGetter = conn.AsyncGetter
    ) -> None:
        """
        ABSQuery constructor.

        Args:
            * Note if both catno and seriesID are None then an exception will be raised.
            catno (Optional): String for ABS Catalogue No. Must contain '.0' as suffix. E.g. "6401.0". Preferred over
                `seriesID` if both supplied.
            seriesID (Optional): String for Series ID. 
            table_title (Optional): String in the format the Timeseries Dictionary likes.
        """
        self.id: CatNo | SeriesID
        self.table_title: str | None = table_title
        self.getter: conn.HTTPGetter = getter
        # Not set in __init__, but in _get_serieslist() because expensive.
        self.series_list: list[ABSSeries] | None = None
        # Not set in __init__, but in get_table_links() because it depends on above.
        self.table_info: dict[str, str] | None = None

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
    
    def __repr__(self: ABSQuery) -> str:
        """
        Detailed string representation of class.
        """

        def trunc_str(o: object) -> str:
            print_str: str = o.__str__()
            trunc_length: int = 100

            return print_str[:trunc_length] + '...' if len(print_str) > trunc_length else print_str

        return(
            f"ABSQuery Object:\n"
            f"Catalogue No.: {self.id.catno if isinstance(self.id, CatNo) else None}\n"
            f"Series ID: {self.id.series_id if isinstance(self.id, SeriesID) else None}\n"
            f"Series List: {trunc_str(self.series_list) if self.series_list is not None else None}\n"
            f"Table Info: {trunc_str(self.table_info) if self.table_info is not None else None}\n"
        ) 

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
        response: str = self.getter.get_one(xml_query)
        return_element: ABSXML = ABSXML(ET.fromstring(response))

        # Handle additional pages.
        if (num_pages_elem := return_element.find('NumPages')) is not None:
            if num_pages_str := num_pages_elem.text:
                num_pages_int: int = int(num_pages_str)

                if isinstance(num_pages_int, int) and num_pages_int > 1:
                    urls: list[str] = [self._construct_query(pg = i) for i in range(2, num_pages_int + 1)]
                    response_many: list[str] = self.getter.get_many(urls)
                    
                    additional_pages: list[ET.Element] = [ET.fromstring(r) for r in response_many]
                    return_element.extend(additional_pages)

        return ABSXML(return_element)

    def _get_serieslist(self: ABSQuery) -> list[ABSSeries]:
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

        return self.series_list

    def _get_table_info(self: ABSQuery) -> dict[str, str]:
        """
        Calls ._get_serieslist and sets table info based on the .series_list property. 

        Raises:
            ABSQueryError: If .series_list is not set properly. Since it is a must have dependency.
        """
        series_list: list[ABSSeries] = self._get_serieslist()

        if series_list != [] and self.table_info is None:
            self.table_info = {elem['TableTitle']: elem['TableURL'] for elem in series_list}
            return self.table_info

        elif self.table_info is not None:
            return self.table_info

        else:
            raise ABSQueryError("._get_serieslist() did not set .series_list properly.")

    def get_table_names(self: ABSQuery) -> set[str] | None:
        """
        Get all available table names for an ABSQuery object.

        Returns:
            A set with all unique table names available in the Timeseries Dictionary for the given ABSQuery parameters.
        """
        table_info: dict[str, str] = self._get_table_info()
        
        return set(table_info.keys()) if table_info else None

    def get_table_links(self: ABSQuery, table_title: str) -> dict[str, str]:
        """
        Gets all the table links for a given table_title query. Will try to match everything where table_title is in
        TimeSeries Dictionary.
        *Note: Can return empty dictionary if there's no match.

        Args:
            table_title: A string. Is matched against table titles with `in` keywords

        Returns:
            A dictionary of TableTitle's and TableURL's.
        """
        table_info: dict[str, str] = self._get_table_info()

        if table_info:
            return {k:v for k, v in table_info.items() if table_title in k}
        else:
            raise ABSQueryError("._self_table_info() did not set .table_info properly.")

    def get_dataframe(self: ABSQuery, table_str: str) -> dict[str, pd.DataFrame]:
        """
        Downloads pandas dataframe by searching for table_str.

        Args:
            table_str: string to search self.table_info for.

        Returns:
            A dictionary of table names and pandas DataFrames that matched the search.
            *Note: Can return empty dictionary if there's no match.
        """
        table_links: dict[str, str] = self.get_table_links(table_str)

        return_dict: dict[str, pd.DataFrame] = {}

        for name, url in table_links.items():
            print(f"Getting data for table: {name}")

            response: bytes = asyncio.run(conn.get_one_bytes(url))
            return_dict[name] = self._process_dataframes(response)

        return return_dict

    def get_first_dataframe(self: ABSQuery, table_str: str) -> pd.DataFrame:
        """
        Simple wrapper that gets the first dataframe from get_dataframe().

        Args:
            table_str: string to search self.table_info for.

        Returns:
            A dictionary of table names and pandas DataFrames that matched the search.
        """
        try:
            first_df: pd.DataFrame = self.get_dataframe(table_str).popitem()[1]
        except KeyError:
            raise ABSQueryError("No dataframes found for table_str.")

        return first_df

    @classmethod
    def _process_dataframes(cls: Type[ABSQuery], response: bytes) -> pd.DataFrame:
        """Converts bytes to final dataframe. Has to do concatenation for multiple sheets."""
        workbook_bytes: BytesIO = BytesIO(response)
        workbook: xlsx.Workbook = xlsx.load_workbook(workbook_bytes)

        # Format and combine since ABS Excel workbooks usually come with multiple sheets of data.
        df_list: list[pd.DataFrame] = \
            [pd.read_excel(workbook_bytes, sheet_name = s) for s in workbook.sheetnames if 'Data' in s]
        remove_headers: list[pd.DataFrame] = [cls._format_ABS_df(df) for df in df_list]

        return reduce(lambda x, y: pd.merge(x, y, on = 'Date'), remove_headers)

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
