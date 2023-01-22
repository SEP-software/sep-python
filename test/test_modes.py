import numpy as np

from sep_python.hypercube import Hypercube,Axis
import sep_python.modes


def test_sepMode():
    io=sep_python.modes.default_io
    hyper=Hypercube(ns=[10,20],os=[1.,2.],ds=[2.,3])
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    
    vec=io.get_reg_vector(ar,hyper=hyper)
    fle=io.get_reg_storage(vec=vec,path="gs://scratch-sep/junk3.H")
    fle.write_description()
    fle.write(ar)
    fle.close()

    fl2=io.get_reg_storage(path="gs://scratch-sep/junk3.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]

    fl3=io.get_reg_storage(vec=vec,path="/tmp/junk4.H")
    fl3.close()

    fl4=io.get_reg_storage(path="/tmp/junk4.H")
    assert fl4.getInt("n1") == 10
    assert fl4.getInt("n2") == 20
    assert fl4.getFloat("o1") == 1.
    assert fl4.getFloat("o2") == 2.
    assert fl4.getFloat("d1") == 2.
    assert fl4.getFloat("d2") == 3.

    fl3.remove()
    fle.remove()
    