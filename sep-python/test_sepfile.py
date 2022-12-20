import pytest
import sepfile
import os
import Hypercube
import numpy as np


def test_simpleArgs_1Line():
    f=open("/tmp/junk.H","w")
    f.write("n1=10 o1=1. d1=2. label='test' in='/tmp/junk.H@'\n")
    f.close()
    fl=sepfile.sepFile(path="/tmp/junk.H")
    assert 10==fl.getInt("n1")
    assert 2.==fl.getFloat("d1")
    assert 5.==fl.getFloat("DJFKKDJ",5.)
    os.remove("/tmp/junk.H")

def test_simpleWriteHistory():
    hyper=Hypercube.hypercube(ns=[10,12],os=[2.,1.])
    fle=sepfile.sepFile(hyper=hyper,path="/tmp/junk.H",type="float64")
    fle.writeDescription()
    fin=sepfile.sepFile(path="/tmp/junk.H")
    assert 10==fin.getInt("n1")
    assert 12==fin.getInt("n2")
    assert .01 > abs(fin.getFloat("o1")-2.)
    os.remove("/tmp/junk.H")

def test_simpleWriteRead():
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    fle=sepfile.sepFile(array=ar,path="/tmp/junk2.H")
    fle.writeDescription()
    fle.write(ar)
    fl2=sepfile.sepFile(path="/tmp/junk2.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]
    os.remove("/tmp/junk2.H")
    os.remove("/tmp/junk2.H@")



