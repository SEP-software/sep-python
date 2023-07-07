import pytest
import io
import copy
import numpy as np
import tempfile
from sep_python._data import (inmem_portion, concat,
                              inmem_data, text_data)
from sep_python._sep_description import inmem_description


def test_inmem_portion():
    # Initialize inmem_portion with sz
    portion = inmem_portion(sz=10)
    assert len(portion._byte_array) == 10

    # Initialize inmem_portion with byte_array
    portion = inmem_portion(byte_array=bytearray(b'Test'))
    assert portion._byte_array == bytearray(b'Test')

    # Test read_portion
    assert portion.read([0, 2], [2, 2]) == bytearray(b'Test')

    # Test write_portion
    portion.write([0], [7], bytearray(b'NewTest'))
    assert portion._byte_array == bytearray(b'NewTest')


def test_inmem_data():
    with pytest.raises(Exception):
        inmem_data("filename", "intent")

    with pytest.raises(Exception):
        inmem_data("filename", "intent", description={})


def test_inmem_portion_init():

    ar = np.array([1., 2., 3.])
    byt = ar.tobytes()

    inmem_desc = inmem_description(io.BytesIO(), "OUTPUT",
                                   array=ar,
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])

    bin_path = inmem_data.get_data_path("junk", "INPUT")

    dat = inmem_data(bin_path, "INPUT", description=inmem_desc,
                     mem=ar)

    assert len(dat._portion._byte_array) == 24
    assert dat._portion._byte_array == byt

    buf = dat.read([0, 8, 16], 8)
    assert buf == byt


def test_concat_data():
    ar = np.array([2., 5., 6.])
    byt = ar.tobytes()

    inmem_desc = inmem_description(io.BytesIO(), "OUTPUT",
                                   array=ar,
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])

    bin_path = inmem_data.get_data_path("junk", "INPUT")

    dat_orig = inmem_data(bin_path, "INPUT", description=inmem_desc,
                          mem=ar)

    out_ptr = io.BytesIO()
    outmem_desc = inmem_description(out_ptr, "OUTPUT",
                                    array=ar,
                                    os=[0.1], ds=[0.2], labels=["label1"],
                                    units=["unit1"])
    dat_out = concat(bin_path, "OUTPUT", description=inmem_desc,
                     file_ptr=out_ptr, mem=ar)

    outmem_desc.set_binary_path("follow_hdr")
    outmem_desc.write_description()
    dat_out.write([0, 8, 16], 8, dat_orig._portion._byte_array)

    in_ptr = copy.deepcopy(out_ptr)
    in_ptr.seek(0)
    (valid, dictionary, binary,
     file_ptr, eot) = inmem_description.read_description(in_ptr)
    des = inmem_description(in_ptr, "INPUT", dictionary=dictionary)
    dat_in = concat(bin_path, "INPUT", binary=binary,
                    description=des, file_ptr=out_ptr)
    byt = dat_in.read([0], 24)

    assert dat_orig._portion._byte_array == byt


def test_text_data():
    ar = np.array([4., 5., 6.])
    byt = ar.tobytes()

    inmem_desc = inmem_description(io.BytesIO(), "OUTPUT",
                                   array=ar,
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])
    fl = tempfile.NamedTemporaryFile().name
    bin_path = text_data.get_data_path(
        fl, "INPUT")

    dat = text_data(bin_path, "OUTPUT", description=inmem_desc,
                    mem=ar)

    dat.close()

    dat2 = text_data(bin_path, "INPUT", description=inmem_desc)
    assert len(dat._portion._byte_array) == 24
    assert dat._portion._byte_array == byt

    buf = dat2.read([0, 8, 16], 8)
    assert buf == byt
