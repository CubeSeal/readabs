from __future__ import annotations
from typing import NewType

# Types
CatNo = NewType('CatNo', str)
SeriesID = NewType('SeriesID', str)
type ABSID = CatNo | SeriesID

# Class
class ABSQuery:
    def __init__(self: ABSQuery, catNo: str | None = None, seriesID: str | None = None):
       
        if catNo is not None:
            self.catNo = CatNo(catNo)

        else:
            if seriesID is not None:
                self.seriesID = SeriesID(seriesID)
            else:
                raise ValueError("Either catNo or seriesID must be provided")
