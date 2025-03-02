import re
import requests as req

import readabs.connection as conn

def _getDataflowList() -> list[str]:

    url: str = "https://data.api.abs.gov.au/rest/dataflow/ABS"
    response: req.models.Response = conn._get_data(url)

    parsed_dataflow_list: list[str] = _parse_dataflow_list(response.text)

    return parsed_dataflow_list

def _parse_dataflow_list(response_text: str) -> list[str]:
    dataflows: list[str] = re.findall(r'Dataflow id="(.+?)"', response_text)
    
    return dataflows
