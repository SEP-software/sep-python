"""Module for testing modes"""
import numpy as np

from sep_python.hypercube import Hypercube
import sep_python.modes


def test_sep_mode():
    """Test tht sep mode wroks"""
    sep_io = sep_python.modes.default_io
    hyper = Hypercube.set_with_ns([10, 20], os=[1.0, 2.0], ds=[2.0, 3])
    array = np.ndarray((20, 10))
    for j in range(array.shape[0]):
        for i in range(array.shape[1]):
            array[j, i] = 100 * j + i

    vec = sep_io.get_reg_vector(array, hyper=hyper)
    fle = sep_io.get_reg_storage(vec=vec, path="gs://scratch-sep/junk3.H")
    fle.write_description()
    fle.write(array)
    fle.close()

    fl2 = sep_io.get_reg_storage(path="gs://scratch-sep/junk3.H")
    array2 = np.zeros((20, 10))
    fl2.read(array2)
    for j in range(array.shape[0]):
        for i in range(array.shape[1]):
            assert array[j, i] == array2[j, i]

    fl3 = sep_io.get_reg_storage(vec=vec, path="/tmp/junk4.H")
    fl3.close()

    fl4 = sep_io.get_reg_storage(path="/tmp/junk4.H")
    assert fl4.getInt("n1") == 10
    assert fl4.getInt("n2") == 20
    assert fl4.getFloat("o1") == 1.0
    assert fl4.getFloat("o2") == 2.0
    assert fl4.getFloat("d1") == 2.0
    assert fl4.getFloat("d2") == 3.0

    fl3.remove()
    fle.remove()
