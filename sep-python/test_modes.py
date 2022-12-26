import modes
import Hypercube


def test_sepMode():
    io=modes.defaultIO
    hyper=Hypercube.hypercube(ns=[10,20],os=[1.,2.],ds=[2.,3])
    ar=np.ndarray((20,10))
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            ar[j,i]=100*j+i
    
    vec=io.getVector(ndarray,hyper=hyper)

    fle=sepIO.sGcsObj(vec=vec,path="gs://scratch-sep/junk3.H")
    fle.writeDescription()
    fle.write(ar)
    fle.close()

    fl2=sepIO.sGcsObj(path="gs://scratch-sep/junk3.H")
    ar2=np.zeros((20,10))
    fl2.read(ar2)
    for j in range(ar.shape[0]):
        for i in range(ar.shape[1]):
            assert ar[j,i] == ar2[j,i]

    fl3=sepIO.file(vec=vec,path="/tmp/junk4.H")
    fl3.close()

    fl4=sepIO.file(path="/tmp/junk4.H")
    assert fl4.getInt("n1") == 20
    assert fl4.getInt("n2") == 20
    assert fl4.getInt("o1") == 1
    assert fl4.getInt("o2") == 2
    assert fl4.getInt("d1") == 2
    assert fl4.getInt("d2") == 3

    fl3.remove()
    fle.remove()
    