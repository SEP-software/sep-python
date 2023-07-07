import pytest
import numpy as np
import io
import tempfile
from sep_python._file_data import (file_portion, file_dataset, file_datasets)
from sep_python._sep_description import inmem_description


def test_file_portion():
    # Initialize inmem_portion with sz
    fl = tempfile.NamedTemporaryFile().name

    portion = file_portion(fl)
    ar = np.array([1., 2., 3.])
    byt = ar.tobytes()

    portion.write([0, 8, 16], [8, 8, 8], byt)
    portion2 = file_portion(fl)
    byt2 = portion2.read([0], [24])

    assert byt2 == byt


def get_binary_path(fl, nfiles=1):
    if nfiles == 1:
        return f"{fl}.dat"
    bins = []
    for i in range(nfiles):
        bins.append(f"{fl}.dat{i}")
    return bins


def test_file_data():

    ar = np.array([1., 2., 3.])
    byte_array = ar.tobytes()

    inmem_desc = inmem_description(io.BytesIO(), "OUTPUT",
                                   array=ar,
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])

    fl = tempfile.NamedTemporaryFile().name

    inmem_desc.set_binary_path_func(get_binary_path)
    bin_path = file_dataset.get_data_path(
        fl, "OUTPUT", description=inmem_desc,
        binary_from_path=get_binary_path)

    dat_out = file_dataset(bin_path, "OUTPUT", description=inmem_desc)
    dat_out.write([0, 8, 16], 8, byte_array)
    dat_out.close()
    dat_in = file_dataset(bin_path, "INPUT", description=inmem_desc)
    buf = dat_in.read([0, 8, 16], 8)
    assert buf == byte_array

    dat_out.remove()


def test_file_datas():

    ar = np.array([1., 2., 3.])
    byte_array = ar.tobytes()

    inmem_desc = inmem_description(io.BytesIO(), "OUTPUT",
                                   array=ar,
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])

    fl = tempfile.NamedTemporaryFile().name

    inmem_desc.set_binary_path_func(get_binary_path)
    bin_path = file_datasets.get_data_path(
        fl, "OUTPUT", description=inmem_desc,
        binary_from_path=get_binary_path, blk=4)

    dat_out = file_datasets(bin_path, "OUTPUT", description=inmem_desc, blk=4)
    dat_out.write([0, 8, 16], 8, byte_array)
    dat_out.close()
    dat_in = file_datasets(bin_path, "INPUT", description=inmem_desc)
    buf = dat_in.read([0, 8, 16], 8)
    assert buf == byte_array

    dat_out.remove()


def test_file_follow_hdr():

    ar = np.array([1., 2., 3.])
    byte_array = ar.tobytes()

    inmem_desc = inmem_description(io.BytesIO(), "OUTPUT",
                                   array=ar,
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])

    fl = tempfile.NamedTemporaryFile().name

    inmem_desc.set_binary_path_func(get_binary_path)
    bin_path = file_dataset.get_data_path(fl, "OUTPUT", description=inmem_desc,
                                          binary_from_path=get_binary_path)

    dat_out = file_dataset(bin_path, "OUTPUT", description=inmem_desc)
    dat_out.write([0, 8, 16], 8, byte_array)
    dat_out.close()
    dat_in = file_dataset(bin_path, "INPUT", description=inmem_desc)
    buf = dat_in.read([0, 8, 16], 8)
    assert buf == byte_array

    dat_out.remove()
