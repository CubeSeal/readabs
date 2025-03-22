import asyncio

import readabs.connection as conn

ABS_URL: str = "https://ausstats.abs.gov.au/servlet/TSSearchServlet?"

def test_get_data():
    response: str = asyncio.run(conn.get_one(ABS_URL))

    assert response is not None

def test_get_data_type():
    response: str = asyncio.run(conn.get_one(ABS_URL))

    assert isinstance(response, str)
