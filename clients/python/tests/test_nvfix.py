from cqclient import nvfix


def test_roundtrip_string_fields():
    data = {"a": "hello", "b": "world"}
    bytes_ = nvfix.encode(data)
    back = nvfix.decode(bytes_)
    assert back == {"a": "hello", "b": "world"}


def test_decode_typed_recovers_numbers_and_bools():
    bytes_ = b"qty=100\x01price=99.5\x01active=true\x01name=Alice\x01"
    m = nvfix.decode_typed(bytes_)
    assert m == {"qty": 100, "price": 99.5, "active": True, "name": "Alice"}


def test_encode_rejects_illegal_field_name():
    import pytest
    with pytest.raises(nvfix.NvFixError):
        nvfix.encode({"bad=name": "x"})


def test_encode_rejects_nested():
    import pytest
    with pytest.raises(nvfix.NvFixError):
        nvfix.encode({"k": {"nested": 1}})


def test_null_round_trips_as_empty_string():
    bytes_ = nvfix.encode({"absent": None})
    assert bytes_ == b"absent=\x01"
    back = nvfix.decode(bytes_)
    assert back == {"absent": ""}
