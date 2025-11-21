from sneldb_client.parser import extract_error_message, parse_and_normalize_response
from sneldb_client.transport.base import TransportResponse


def test_parse_json_array() -> None:
    response = TransportResponse(status=200, body=b'[{"id": 1}, {"id": 2}]', headers={})
    records = parse_and_normalize_response(response)
    assert len(records) == 2
    assert records[0]["id"] == 1


def test_parse_pipe_table() -> None:
    body = b"OK\nID|NAME\n1|Alice\n2|Bob"
    response = TransportResponse(status=200, body=body, headers={})
    records = parse_and_normalize_response(response)
    assert records == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]


def test_extract_error_message_prefers_json() -> None:
    body = '{"message":"boom","code":400}'
    assert extract_error_message(body) == "boom"
    assert extract_error_message("nonsense") == "nonsense"
