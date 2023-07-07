import pytest
import numpy as np
import os
from sep_python._sep_io import (InOut)
from sep_python._sep_vector import get_sep_vector


def test_single_in():
    ar = np.array([2, 4, 6], np.float32)
    byt = ar.tobytes()

    fl_bin = open("junk123.H@", "wb")
    fl_bin.write(byt)
    fl_bin.close()

    fl_txt = open("junk123.H", "w")
    fl_txt.write("in='junk123.H@' n1=3 o1=0.1 d1=.1 esize=4"
                 + "data_format=native_float")
    fl_txt.close()

    io = InOut(get_sep_vector)
    stor = io.get_reg_storage("junk123.H")

    vals = get_sep_vector(stor.get_hyper())
    stor.read(vals)

    assert 3 == stor.get_int("n1")
    assert .1 == stor.get_float("o1")
    assert .1 == stor.get_float("d1")

    assert np.all(ar == vals.get_nd_array())

    os.remove("junk123.H")
    os.remove("junk123.H@")


def test_single_out():
    ar = np.array([2, 4, 6], np.float32)

    vec = get_sep_vector(ar, ns=[3], os=[.1], ds=[.1])

    io = InOut(get_sep_vector)

    outF = io.get_reg_storage("junk123.H", vec=vec)

    outF.write_description()
    outF.write(vec)

    vec2 = vec.clone()
    inF = io.get_reg_storage("junk123.H")
    inF.read(vec2)

    assert 3 == inF.get_int("n1")
    assert .1 == inF.get_float("o1")
    assert .1 == inF.get_float("d1")

    assert np.all(vec.get_nd_array() == vec2.get_nd_array())

    outF.remove()


def test_multiple_out():
    ar = np.array([2, 4, 6], np.float32)
    vec = get_sep_vector(ar, ns=[3], os=[.1], ds=[.1])
    io = InOut(get_sep_vector)
    outF = io.get_reg_storage("junk123.H", vec=vec, blk=4)

    outF.write_description()
    outF.write(vec)

    vec2 = vec.clone()
    inF = io.get_reg_storage("junk123.H")
    inF.read(vec2)

    assert 3 == inF.get_int("n1")
    assert .1 == inF.get_float("o1")
    assert .1 == inF.get_float("d1")

    assert np.all(vec.get_nd_array() == vec2.get_nd_array())

    outF.remove()
