import readabs.abs_query as module

def test_abs_query_exists():
    
    abs_query: module.ABSQuery = module.ABSQuery("5340.0")

    assert isinstance(abs_query, module.ABSQuery)
