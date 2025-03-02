import requests as req

from readabs import connection as con

def test_abs_conn():
    response: req.Response = con.get_data("https://data.api.abs.gov.au/rest/")

    assert response is not None

