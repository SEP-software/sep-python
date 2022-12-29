from sepPython.modes import modes
from sepPython.Hypercube import hypercube,axis
import numpy as np


def test_sepMode():
    io=modes.defaultIO
    hyper=hypercube(ns=[10,20],os=[1.,2.],ds=[2.,3])
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    
    vec=io.getRegVector(ar,hyper=hyper)
    fle=io.getRegStorage(vec=vec,path="gs://scratch-sep/junk3.H")
    fle.writeDescription()
    fle.write(ar)
    fle.close()

    fl2=io.getRegStorage(path="gs://scratch-sep/junk3.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]

    fl3=io.getRegStorage(vec=vec,path="/tmp/junk4.H")
    fl3.close()

    fl4=io.getRegStorage(path="/tmp/junk4.H")
    assert fl4.getInt("n1") == 10
    assert fl4.getInt("n2") == 20
    assert fl4.getFloat("o1") == 1.
    assert fl4.getFloat("o2") == 2.
    assert fl4.getFloat("d1") == 2.
    assert fl4.getFloat("d2") == 3.

    fl3.remove()
    fle.remove()
    