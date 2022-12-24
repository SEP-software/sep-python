
import Hypercube
import numpy 
import numba
from sys import version_info
import numpy
import sepConverter
import sepProto

converter=sepConverter.converter


@numnba.njit(parallel=True)
def clipIt(vec,bclip,eclip):
    for i in numba.prange(vec.shape[0]):
        vec[i]=min(eclip,max(bclip,vec[i]))

@numnba.njit(parallel=True)
def clipArray(vec,bclip,eclip):
    for i in numba.prange(vec.shape[0]):
        vec[i]=min(eclip[i],max(bclip[i],vec[i]))

@numba.njit(parallel=True)
def norm1(vec):
    tot=0
    for i in numba.prange(vec):
        tot+=np.abs(vec[i])
    return tot

@numnba.njit(parallel=True)
def scaleAdd(vec1,vec2,sc1,sc2):
    for i in numba.prange(vec1.shape[0]):
        vec1[i]=vec1[i]*sc1+vec2[i]*sc2


@numnba.njit(parallel=True)
def scale(vec,scale)):
    for i in numba.prange(vec.shape[0]):
        vec[i]=vec[i]*sc

@numba.njit(parallel=True):
def dotIt(vec1,vec2):
    val=0
    for i in numba.prange(vec1.shape[0]):
        val+=vec1[i]*vec2[i]
    return val

@numba.njit(parallel=True)
def multiplyIt(vec1,vec2):
    for i in numba.prange(vec1.shape[0]):
        vec1[i]=vec1[i]*vec2[i]
@numba.njit()
def calcHisto(outV,vec,mn,mx):
    delta=(mx-mn)/vec.shape[0]
    for i in range(vec.shape[0]):
        ind=max(0,min(vec.shape[0]-1,int((vec[i]-mn)/delta)))
        outV[ind]+=1

        

class vector(pyVector.vectorIC,sepProto.memReg):
    """Generic sepVector class"""

    def __init__(self, hyper:Hypercube.hypercube, dataFormat:str):
        """Initialize a vector object"""
        self.setHyper(hyper)
        self.setDataFormat(dataFormat)
        super().__init__()


    def getdataFormatType(self)->str:
        """Return type of dataFormat"""
        return self._dataFormat

    def zero(self)->vector:
        """Function to zero out a vector"""
        self.fill(0.)
        return self

    def set(self, val)->vector:
        """Function to a vector to a value"""
        self.fill(val)
        return self

    
    def getNdArray(self)->np.ndarray:
        """Return a numpy version of the array (same memory"""
        return self._arr
    
    def window(self, compress=False, **kw):
        """Window a vector return another vector (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max 

            compress=False Whether or not to compress the axes of length 1

            """
        axes = self.getHyper().axes
        nw,fw,jw=fixWindow(axes,**kw)

        axOut=[]
        axCOut=[]
        nout=[]
        for i in len(nw):
            axOut.append(Hypercube.axis(n=nw[i],o=axes[i].o+axes[i].d*fw[i],d=jw[i]*axes[i].d,\
                label=axes[i].label, unit=axes[i].unit))
            if nw[i]!=1 and compress:
                axCOut.append(axOut[:-1])
                nout.append(nw[i])
        
        vec=getSepVector(axes=axOut)
        outA=vec.getNdArray()
        inA=self.getNdArray()
        ndim=len(axOut.shape)
        if ndim==1:
            outA=inA[fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==2:
            outA=inA[fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==3:
            outA=inA[fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==4:
            outA=inA[fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==5:
            outA=inA[fs[4]:js[4]*ns[4]:js[4],fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==6:
            outA=inA[fs[5]:js[5]*ns[5]:js[5],fs[4]:js[4]*ns[4]:js[4],fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==7:
            outA=inA[fs[6]:fs[6]*js[6]:js[6],\
                fs[5]:js[5]*ns[5]:js[5],fs[4]:js[4]*ns[4]:js[4],fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]   
        if not compress: 
            return vec
        vecO=getSepVector(axes=axCOut)
        arO=vecO.getNdArray()
        arO=outA.reshape(tuple(nout.reverse()))   
        return vecO
    def get1DArray(self)->np.ndarray:
        return np.ravel(self._arr)

    def adjustHyper(self,hyper:Hypercube.hypercube):
        """Adjust the hypercube associated with vector. Does not reallocate. Must be same dims/size"""
        hyperOld=self.getHyper()
        if hyperOld.getN123() != hyper.getN123():
            raise Exception("Trying to reset with a different sized hyper")
        self._arr=np.reshape(tuple(np.flip(hyper.getNs())))
        self._hyprer=hyper

    def checkSame(self, vec2:vector)->bool:
        """Function to check if two vectors belong to the same vector space"""
        if vec2.dataFormatType() != self.checkdataFormat():
            return False
        return self.getHyper().checkSame(vec2.getHyper())
    


class nonInteger(vector):
    """A class for non-integers"""
    def __init__(self,hyper:Hypercube:hypercube, dataFormat:str):
        """Initialize a non-integer"""
        super().__init__(hyper,dataFormat)

class realNumber(nonInteger):
    """A class for real numbers"""
    def __init__(self,hyper:Hypercube.hypercube,dataFormat:str):
        """Initialize a real number vector"""
        super().__init__(hyper,dataFormat)


    def clip(self, bclip eclip):
        """Clip dataset
                bclip - Minimum value to clip to
                eclip - Maximum value to clip to"""
        clipIt(self.get1DArray(),bclip,eclip)

    def cent(self, pct:float, jsamp=1):
        """Calculate the percentile of a dataset
                pct - Percentile of the dataset
                jsamp - Sub-sampling of dartaset"""
        return np.percentile(self._arr,pct)

    def rand(self)->vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape)
        return self

    def clipVector(self, low:vector, high:vector)->vector:
        """Clip vector element by element vec=min(high,max(low,vec))"""
        if not self.checkSame(low) or self.checksame(high):
            raise SEPException("low,high, and vector must all be the same space")
        clipArray(self.get1DArray(),low.get1DArray() high.get1DArray())
        return self

    def scaleAdd(self, vec2:vector, sc1=1., sc2=1.)->vector:
        """self = self * sc1 + sc2 * vec2"""
        if not self.checkSame(vec2) or self.getDataFormat()!=vec2.getDataFormat():
            raise SEPException("must be of the same space and type")
        scaleAdd(self.get1DArray(),vec.get1DArray(),sc1,sc2)
        return self

    def scale(sc)->vector:
        """self = self * sc"""
        scale(self.get1DArray(),sc)
        return self

    def calcHisto(self, nelem, mn, mx):
        """Calculate histogram
           mn - Minimum for cell
           mx - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        ar=self.get1DArray()
        #CHANGE
        histo=self.getSepVector()
        histo = getSepVector(ns=[nelem], dataFormat="int32")
        self.cppMode.calcHisto(histo.getCpp(), mn, mx)
        return histo
    def copy(self, vec2:vector)->vector:
        """Copy vec2 into self"""
        if not self.checkSame(vec2) or self.getdataFormat()!=vec2.getdataFormat():
            raise SEPException("must be of the same space and type")
        scaleAdd(self.get1DArray(),vec.get1DArray(),0.,1.)
        return self

    def dot(self, vec2:vector)->vector:
        """Compute dot product of two vectors"""
        if not self.checkSame(vec2) or self.getdataFormat()!=vec2.getdataFormat():
            raise SEPException("must be of the same space and type")
        return dotIt(self.get1DArray(),vec2.get1DArray())
    
    def norm(self, N=2):
        """Function to compute vector N-norm"""
        if N=1:
           return  norm1(self.get1Darray())
        elif N=2:
            return self.dot(self)/2
        else:
            raise SEPException("Only can do norm 1 and 2")
   
    
    def multiply(self, vec2:vector)->vector:
        """self = vec2 * self"""
        if not self.checkSame(vec2) or self.getdataFormat()!=vec2.getdataFormat():
            raise SEPException("must be of the same space and type")
        multiplyIt(self.get1DArray(),vec2.get1DArray())
        return self

class floatVector(vector):
    """Generic float vector class"""

    def __init__(self, hyper:Hypercube:hypercube,spaceOnly=False, arr=None):
        self.kw = kw
        super().__init__(hyper,"dataFloat")
        if not spaceOnly:
            self._arr=np.ndarray(tuple(hyper.getNs().reverse()),dtype=np.float32)

    
    def __repr__(self):
        """Override print method"""
        return "floatVector\n%s"%str(self.getHyper())



    def rand(self)->vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape).astype("f4")
        return self
    def clone(self):
        """Function to clone (deep copy) a vector"""
        return floatVector(self.getHyper().self.getdataFormat())


    def cloneSpace(self):
        """Funtion tor return the space of a vector"""
        return floatVector(self.getHyper().self.getdataFormat(),spaceOnly=True)

class doubleVector(vector):
    """Generic double vector class"""


    def __init__(self, hyper:Hypercube:hypercube,spaceOnly=False, arr=None):
        self.kw = kw
        super().__init__(hyper,"double64")
        if not spaceOnly:
            self._arr=np.ndarray(tuple(hyper.getNs().reverse()),dtype=np.float64)

    
    def rand(self)->vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape)
        return self
    
    def __repr__(self):
        """Override print method"""
        return "doubleVector\n%s"%str(self.getHyper())
    


    def clone(self):
        """Function to clone (deep copy) a vector"""
        return doubleVector(self.getHyper().self.getdataFormat())


    def cloneSpace(self):
        """Funtion tor return the space of a vector"""
        return doubleVector(self.getHyper().self.getdataFormat(),spaceOnly=True)


class intVector(vector):
    """Generic int vector class"""

    def __init__(self, hyper:Hypercube:hypercube,spaceOnly=False, arr=None):
        self.kw = kw
        super().__init__(hyper,"dataInt")
        if not spaceOnly:
            self._arr=np.ndarray(tuple(hyper.getNs().reverse()),dtype=np.int32)

    

    def __repr__(self):
        """Override print method"""
        return "intVector\n%s"%str(self.getHyper())

    def clone(self):
        """Function to clone (deep copy) a vector"""
        return intVector(self.getHyper().self.getdataFormat())
class complexVector(vector):
    """Generic complex vector class"""


    def __init__(self, hyper:Hypercube:hypercube,spaceOnly=False, arr=None):
        self.kw = kw
        super().__init__(hyper,"float32")
        if not spaceOnly:
            self._arr=np.ndarray(tuple(hyper.getNs().reverse()),dtype=np.float64)

    

    def cloneSpace(self):
        """Funtion tor return the space of a vector"""
        return complexVector(self.getHyper().self.getdataFormat(),spaceOnly=True)

    def __repr__(self):
        """Override print method"""
        return "complexVector\n%s"%str(self.getHyper())

    def clone(self):
        """clone a vector"""
        return complexVector(self.getHyper().self.getdataFormat())

    def rand(self)->vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape).astype("f4")+\
            1j*np.random.random(self._arr.shape).astype("f4")
        return self

class complexDoubleVector(vector):
    """Generic complex vector class"""

    def __init__(self, hyper:Hypercube:hypercube,spaceOnly=False, arr=None):
        self.kw = kw
        super().__init__(hyper,"complex128")
        if not spaceOnly:
            self._arr=np.ndarray(tuple(hyper.getNs().reverse()),dtype=np.complex128)


    def cloneSpace(self):
        """Funtion tor return the space of a vector"""
        return complexDoubleVector(self.getHyper().self.getdataFormat(),spaceOnly=True)

    def norm(self, N=2):
        """Function to compute vector N-norm"""
        return self.cppMode.norm(N)
    def __repr__(self):
        """Override print method"""
        return "complexDoubleVector\n%s"%str(self.getHyper())

    def rand(self)->vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape)+1j*np.random.random(self._arr.shape)
        return self

    def clone(self):
        """clone a vector"""
        return complexDoubleVector(self.getHyper().self.getdataFormat())

    def clipVector(self, low, high):
        """Clip vector element by element vec=min(high,max(low,vec))"""
        self.cppMode.clipVector(low.cppMode, high.cppMode)
        return self

class byteVector(vector):
    """Generic byte vector class"""


    def __init__(self, hyper:Hypercube:hypercube,spaceOnly=False, arr=None):
        self.kw = kw
        super().__init__(hyper,"dataByte")
        if not spaceOnly:
            self._arr=np.ndarray(tuple(hyper.getNs().reverse()),dtype=np.uint8)

    

    def calcHisto(self, nelem, mn, mx):
        """Calculate histogram
           mn - Minimum for cell
           mx - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        histo = getSepVector(ns=[nelem], dataFormat="dataInt")
        self.cppMode.calcHisto(histo.getCpp(), mn, mx)
        return histo
    def clone(self):
        """Function to clone (deep copy) a vector"""
        return byteVector(fself.getHyper().self.getdataFormat()s)
    def __repr__(self):
        """Override print method"""
        return "byteVector\n%s"%str(self.getHyper())

def getSepVector(*args, **keys):
    """ Get a sepVector object
            Option 1 (supply hypercube):
                    hyper, kw args
            Option 2 (build hypercube):
                    ns = [] - list of sizes
                    os = [] - list of origins
                    ds = [] - list os sampling
                    labels = [] list of labels
                    axes = [] list of axes

            dataFormat = dataFormatType(float32[default], float64,double64,int32,complex64,complex128)

            Option 4 (numpy)
                Provide hyper, ns, os, or ds,label,s axes


    """
    myt = "float32"
    haveHyper=False
    haveNumpy=False
    array=None
    if "dataFormat" in keys:
        keyps["dataFormat"]=keys["dataFormat"]
    if len(args) == 1:
        if isinstance(args[0],Hypercube.hypercube):
            haveHyper=True
            hyper = args[0]
        elif isinstance(args[0],numpy.ndarray):
            haveNumpy=True
            array=args[0]
            if "hyper" in keys:
                hyper=keys["hyper"]
            elif "axes" in keys or "ns" in keys:
                hyper = Hypercube.hypercube(**keys)
            else:
                nt=list(array.shape)
                ns=[]
                for i in range(len(nt)):
                    ns.append(nt[len(nt)-1-i])
                hyper =Hypercube.hypercube(ns=ns)
        else:
            raise Exception("First argument must by a hypercube or numpy array")
    elif len(args) == 0:
        if "axes" in keys or "ns" in keys:
            hyper = Hypercube.hypercube(**keys)
       
        else:
            raise Exception("Must supply Hypercube,vector  or ns/axes")
    else:
        raise Exception(
            "Only understand 0 or 1 (hypercube) non-keyword arguments")



    if haveNumpy:
        if not converter.validType(array.dtype):
            raise Exception(f"Numpy array type {array.dtype} not supported")
        myt=converter.getName(array.dtype)
    else:
        myt="float32"
        if "dataFormat" in keys:
            myt = converter.getName(keys["dataFromat"])

    if myt == "float32":
        y= floatVector(hyper)
    elif myt == "complex128":
        y=complexDoubleVector(hyper)
    elif myt == "complex64":
        y=complexVector(hyper)
    elif myt == "double64":
        y= doubleVector(hyper)
    elif myt == "int32":
        y= intVector(hyper)
    elif myt == "uint8":
        y= byteVector(hyper)
    else:
        raise Exception("Unknown type %s" % myt)
    if haveNumpy:
        numpy.copyto(y.getNdArray(),array)
    return y


def readColTextFile(file):
    f = open(file)
    lines = f.readlines()
    array = []
    for line in lines:
        parts = line.split()
        vec1 = []
        for x in parts:
            try:
                y = float(x)
                vec1.append(y)
            except:
                y = x
        array.append(vec1)
    array2 = []
    for i in range(len(array[0])):
        lst = []
        for j in range(len(array)):
            lst.append(array[j][i])
        array2.append(lst)
    return array2
def fixWindow(axes,**kw):
    """Create full window parameters 

    axes - Axes for dataset
    kw = n1, f1, j1 - Window parameters

    returns 
      nw,fw,jw - Full window paramters
    """
    ndim = len(axes)
    nw = []
    fw = []
    jw = []

    for i in range(1, ndim + 1):
        nset = False
        fset = False
        jset = False
        if "n" in kw:
            if isinstance(kw["n"],list):
                if len(kw["n"]) >= i:
                    kw["n%d"%i]=kw["n"][i-1]
        if "f" in kw:
            if isinstance(kw["f"],list):
                if len(kw["f"]) >= i:
                    kw["f%d"%i]=kw["f"][i-1]
        if "j" in kw:
            if isinstance(kw["j"],list):
                if len(kw["j"]) >= i:
                    kw["j%d"%i]=kw["j"][i-1]                   
        if "n%d" % i in kw:
            nset = True
            n = int(kw["n%d" % i])
        if "f%d" % i in kw:
            fset = True
            f = int(kw["f%d" % i])
        if "j%d" % i in kw:
            jset = True
            j = int(kw["j%d" % i])
        biSet = False
        eiSet = False
        if "min%d" % i in kw:
            bi = int(float(kw["min%d" %
                                i] - axes[i - 1].o) / axes[i - 1].d + .5)
            biSet = True
        if "max%d" % i in kw:
            ei = int(float(kw["max%d" %
                                i] - axes[i - 1].o) / axes[i - 1].d + .5)
            eiSet = True
        if fset:
            if axes[i - 1].n <= f:
                raise Exception(
                    "invalid f%d=%d parameter n%d of data=%d" %
                    (i, f, i, axes[
                        i - 1].n))
        if nset:
            if axes[i - 1].n < n:
                raise Exception(
                    "invalid n%d=%d parameter n%d of data=%d" %
                    (i, n, i, axes[
                        i - 1].n))
        if jset and j <= 0:
            raise Exception("invalid j%d=%d " % (i, j))
        if not jset:
            j = 1
        if not nset:
            if not fset:
                if not biSet:
                    f = 0
                elif bi < 0 or bi >= axes[i - 1].n:
                    raise Exception("Invalid min%d=%f" %
                                    (i, kw["min%d" % i]))
                else:
                    f = bi
            if eiSet:
                if ei <= f or ei >= axes[i - 1].n:
                    raise Exception("Invalid max%d=%f" %
                                    (i, kw["max%d" % i]))
                else:
                    n = (ei - f - 1) / j + 1
            else:
                n = (axes[i - 1].n - f - 1) / j + 1

            if not biSet and not eiSet and not jset and not fset:
                n = axes[i - 1].n
                j = 1
                f = 0
        elif not fset:
            if not biSet:
                f = 0
            elif bi < 0 or bi >= axes[i - 1].n:
                raise Exception("Invalid min%d=%f" % (i, kw["min%d" % i]))
            else:
                f = fi
        if axes[i - 1].n < (1 + f + j * (n - 1)):
            raise Exception("Invalid window parameter")
        nw.append(int(n))
        fw.append(int(f))
        jw.append(int(j))
    return nw,fw,jw
