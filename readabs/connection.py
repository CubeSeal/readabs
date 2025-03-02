import requests as req

def _get_data(url: str) -> req.models.Response:
    response = req.get(url)

    return response
