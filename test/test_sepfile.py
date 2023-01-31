"""Module to run tests on SEPIO"""
import pytest
import numpy as np
from sep_python.hypercube import Hypercube
import sep_python.sep_io


def test_simple_args_1_line():
    """Simple test to make sure basic parameter handling works"""
    file_pointer = open("/tmp/junk.H", "w", encoding="utf-8")
    file_pointer.write("\n n1=10 o1=1. d1=2. label='test' in='/tmp/junk.H@'\n")
    file_pointer.close()
    sep_file = sep_python.sep_io.SEPFile(path="/tmp/junk.H")
    assert 10 == sep_file.get_int("n1")
    assert 2.0 == sep_file.get_float("d1")
    assert 5.0 == sep_file.get_float("DJFKKDJ", 5.0)
    sep_file.remove()


def test_simple_write_history():
    """Test whether we can do a write"""
    hyper = Hypercube.set_with_ns([10, 12], os=[2.0, 1.0])
    sep_ponter = sep_python.sep_io.SEPFile(
        hyper=hyper, path="/tmp/junk.H", type="float64"
    )
    sep_ponter.write_description()
    sep_ponter_in = sep_python.sep_io.SEPFile(path="/tmp/junk.H")
    assert 10 == sep_ponter_in.get_int("n1")
    assert 12 == sep_ponter_in.get_int("n2")
    assert 0.01 > abs(sep_ponter_in.get_float("o1") - 2.0)
    sep_ponter_in.remove()


def test_simple_write_history_GCS():
    """Test write to GCS"""
    hyper = Hypercube.set_with_ns([10, 12], os=[2.0, 1.0])
    sep_ponter = sep_python.sep_io.SEPGcsObj(
        hyper=hyper, path="gs://scratch-sep/junk.H", type="float64"
    )
    sep_ponter.close()
    sep_ponter_in = sep_python.sep_io.SEPGcsObj(path="gs://scratch-sep/junk.H")
    assert 10 == sep_ponter_in.get_int("n1")
    assert 12 == sep_ponter_in.get_int("n2")
    assert 0.01 > abs(sep_ponter_in.get_float("o1") - 2.0)
    sep_ponter_in.remove()


def test_simple_write_read():
    """Test that read works"""
    simple_array = np.ndarray((20, 10))
    for jstride in range(simple_array.shape[0]):
        for istride in range(simple_array.shape[1]):
            simple_array[jstride, istride] = 100 * jstride + istride
    sep_ptr = sep_python.sep_io.SEPFile(array=simple_array, path="/tmp/junk2.H")
    sep_ptr.write_description()
    sep_ptr.write(simple_array)
    sep_ptr_in = sep_python.sep_io.SEPFile(path="/tmp/junk2.H")
    array2 = np.zeros((20, 10))
    sep_ptr_in.read(sep_ptr_in)
    for jstride in range(simple_array.shape[0]):
        for istride in range(simple_array.shape[1]):
            assert simple_array[jstride, istride] == array2[jstride, istride]
    sep_ptr.remove()


def test_simple_write_read_GCS():
    """Test write to GCS"""
    array = np.ndarray((20, 10))
    for j in range(array.shape[0]):
        for i in range(array.shape[1]):
            array[j, i] = 100 * j + i
    fle = sep_python.sep_io.SEPGcsObj(array=array, path="gs://scratch-sep/junk2.H")
    fle.write_description()
    fle.write(array)
    fle.close()

    fl2 = sep_python.sep_io.SEPGcsObj(path="gs://scratch-sep/junk2.H")
    array2 = np.zeros((20, 10))
    fl2.read(array2)
    for j in range(array.shape[0]):
        for i in range(array.shape[1]):
            assert array[j, i] == array2[j, i]
    fl2.remove()
