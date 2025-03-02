import requests as req

from readabs import connection as con

abs_url = "https://ausstats.abs.gov.au/servlet/TSSearchServlet?"

def test_get_data():
    response: req.models.Response = con._get_data(abs_url)

    assert response is not None

def test_get_data_type():
    response: req.models.Response = con._get_data(abs_url)

    assert isinstance(response, req.models.Response)

def test_abs_conn_status():
    response: req.models.Response = con._get_data(abs_url)

    assert response.status_code == 200
