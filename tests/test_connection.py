import requests as req

from readabs import connection as con

url: str = "https://abs.gov.au"

def test_get_data():
    response: req.models.Response = con.get_data(url)

    assert response is not None

def test_get_data_type():
    response: req.models.Response = con.get_data(url)

    assert isinstance(response, req.models.Response)

def test_abs_conn_status():
    response: req.models.Response = con.get_data(url)

    assert response.status_code == 200
