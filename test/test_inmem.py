import pytest
from sep_python._data import inmem_portion


def test_init_with_sz():
    mem_portion = inmem_portion(sz=10)
    assert len(mem_portion._byte_array) == 10


def test_init_with_byte_array():
    byte_array = bytearray(b'Test data')
    mem_portion = inmem_portion(byte_array=byte_array)
    assert mem_portion._byte_array == byte_array


def test_init_no_options():
    with pytest.raises(Exception):
        inmem_portion()


def test_read_portion():
    byte_array = bytearray(b'Test data')
    mem_portion = inmem_portion(byte_array=byte_array)
    result = mem_portion.read([1, 5], [3, 2])
    assert result == bytearray(b'estda')


def test_init_exception():
    with pytest.raises(Exception) as e_info:
        portion = inmem_portion()


def test_write():
    byte_array = bytearray(50)
    portion = inmem_portion(byte_array=byte_array)
    portion.write([0], [9], b'Test data')
    assert portion.read([0], [9]) == b'Test data'