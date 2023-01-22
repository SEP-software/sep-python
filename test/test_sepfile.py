import pytest
import numpy as np
from sep_python.hypercube import Hypercube
import sep_python.sep_io 




def test_simple_args_1_Line():
    f=open("/tmp/junk.H","w")
    f.write("\n n1=10 o1=1. d1=2. label='test' in='/tmp/junk.H@'\n")
    f.close()
    fl=sep_python.sep_io.sFile(path="/tmp/junk.H")
    assert 10 == fl.get_int("n1")
    assert 2. == fl.get_float("d1")
    assert 5. == fl.get_float("DJFKKDJ",5.)
    fl.remove()

def test_simple_write_history():
    hyper=Hypercube(ns=[10,12],os=[2.,1.])
    fle=sep_python.sep_io.SEPFile(hyper=hyper,path="/tmp/junk.H",type="float64")
    fle.write_description()
    fin=sep_python.sep_io.SEPFile(path="/tmp/junk.H")
    assert 10 == fin.get_int("n1")
    assert 12 == fin.get_int("n2")
    assert .01 > abs(fin.get_float("o1")-2.)
    fin.remove()

def test_simple_write_history_GCS():
    hyper=Hypercube(ns=[10,12],os=[2.,1.])
    fle=sep_python.sep_io.SEPGcsObj(hyper=hyper,path="gs://scratch-sep/junk.H",type="float64")
    fle.close()
    fin=sep_python.sep_io.SEPGcsObj(path="gs://scratch-sep/junk.H")
    assert 10 == fin.get_int("n1")
    assert 12 == fin.get_int("n2")
    assert .01 > abs(fin.get_float("o1")-2.)
    fin.remove()

def test_simple_write_read():
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    fle=sep_python.sep_io.SEPFile(array=ar,path="/tmp/junk2.H")
    fle.write_description()
    fle.write(ar)
    fl2=sep_python.sep_io.SEPFile(path="/tmp/junk2.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]
    fl2.remove()

def test_simple_write_read_GCP():
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
    fle=sep_python.sep_io.SEPGcsObj(array=ar,path="gs://scratch-sep/junk2.H")
    fle.write_description()
    fle.write(ar)
    fle.close()

    fl2=sep_python.sep_io.SEPGcsObj(path="gs://scratch-sep/junk2.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]
    fl2.remove()


