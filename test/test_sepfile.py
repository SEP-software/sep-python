import pytest
from sepPython import sepIO
import os
from sepPython import hypercube,axis
import numpy as np
import logging



def eest_simpleArgs_1Line():
    f=open("/tmp/junk.H","w")
    f.write("n1=10 o1=1. d1=2. label='test' in='/tmp/junk.H@'\n")
    f.close()
    fl=sepIO.sFile(path="/tmp/junk.H")
    assert 10==fl.getInt("n1")
    assert 2.==fl.getFloat("d1")
    assert 5.==fl.getFloat("DJFKKDJ",5.)
    fl.remove()

def test_simpleWriteHistory():
    hyper=hypercube(ns=[10,12],os=[2.,1.])
    fle=sepIO.sFile(hyper=hyper,path="/tmp/junk.H",type="float64")
    fle.writeDescription()
    fin=sepIO.sFile(path="/tmp/junk.H")
    assert 10==fin.getInt("n1")
    assert 12==fin.getInt("n2")
    assert .01 > abs(fin.getFloat("o1")-2.)
    fin.remove()

def test_simpleWriteHistoryGCS():
    hyper=hypercube(ns=[10,12],os=[2.,1.])
    fle=sepIO.sGcsObj(hyper=hyper,path="gs://scratch-sep/junk.H",type="float64")
    fle.close()
    fin=sepIO.sGcsObj(path="gs://scratch-sep/junk.H")
    assert 10==fin.getInt("n1")
    assert 12==fin.getInt("n2")
    assert .01 > abs(fin.getFloat("o1")-2.)
    fin.remove()

def test_simpleWriteRead():
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    fle=sepIO.sFile(array=ar,path="/tmp/junk2.H")
    fle.writeDescription()
    fle.write(ar)
    fl2=sepIO.sFile(path="/tmp/junk2.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]
    fl2.remove()

def test_simpleWriteReadGCP():
    # create logger with 'spam_application'
    #logger = logging.getLogger(None)
    #logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    #fh = logging.FileHandler('spam.log')
    #fh.setLevel(logging.DEBUG)
    #logger.addHandler(fh)
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    fle=sepIO.sGcsObj(array=ar,path="gs://scratch-sep/junk2.H")
    fle.writeDescription()
    fle.write(ar)
    fle.close()

    fl2=sepIO.sGcsObj(path="gs://scratch-sep/junk2.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]
    fl2.remove()


