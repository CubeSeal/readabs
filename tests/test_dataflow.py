import readabs.dataflow as module

def test_dataflow_list():
    dataflows: list[str] = module.getDataflowList()

    assert isinstance(dataflows, list)
