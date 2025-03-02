from __future__ import annotations
from typing import NewType

import re
import xml.etree.ElementTree as ET

import readabs.connection as conn

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

        self.xml: ET.Element = self._get_ts_dict_xml()

    def _construct_ts_dict_query(self: ABSQuery) -> str:
        id_str: str = f"catno={self.catNo}" if self.catNo is not None else f"sid={self.seriesID}"

        return self._base_query + f"{id_str}" 

    def _get_ts_dict_xml(self: ABSQuery) -> ET.Element:
        xml_query: str = self._construct_ts_dict_query()

        return_xml: str = ABSXML(conn._get_data(xml_query).text)
        return ET.fromstring(return_xml)

    def getSeriesList(self: ABSQuery) -> list[dict[str, str | None]]:
        series_list: list[dict[str, str | None]] = []

        for series in self.xml.iter("Series"):
            series_dict: dict [str, str | None] = {}

            for child in series:
                if child.tag is not None:
                    series_dict[child.tag] = child.text 

            series_list.append(series_dict)

        return series_list
