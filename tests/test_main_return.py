import pytest

from readabs import main

def test_main():
    assert main.main() is not None

