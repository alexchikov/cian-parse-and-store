import pytest
from pipelines.dags.parsing.parse import Parser
from pipelines.dags.upload_to_s3 import upload_to_s3

def test_get_offers():
    p = Parser()
    assert p.get_offers()
    
def test_upload_to_s3():
    assert upload_to_s3() == 1