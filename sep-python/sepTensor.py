
import pyHypercube
import pySepVector
import Hypercube
import numpy 
import pyVector
from sys import version_info


dtypeToSepVecType={
 "float32":"dataFloat",
 "int32":"dataInt",
 "int64":"dataLong",
 "float64":"dataDouble",
 "uint8":"dataByte",
 "complex64":"dataComplex",
 "complex128":"dataComplexDouble"

 }
SepVecTypeToDtype= {v: k for k, v in dtypeToSepVecType.items()}


class tensor(pyVector.vectorIC):
    """Generic septensor class"""

    def __init__(self):
        """Initialize a tensor object"""
        if "fromCpp" in self.kw:
            self.cppMode = self.kw["fromCpp"]
        elif "fromHyper" in self.kw:
            self.cppMode = getCppSepTensor(self.kw["fromHyper"], self.kw)
        super().__init__(self.getNdArray())

    def getCpp(self):
        """Return the associated cpp object"""
        return self.cppMode

    def clip(self, bclip, eclip):
        """Clip dataset
                bclip - Minimum value to clip to
                eclip - Maximum value to clip to"""
        self.cppMode.clip(bclip, eclip)

    def cent(self, pct, jsamp=1):
        """Calculate the percentile of a dataset
                pct - Percentile of the dataset
                jsamp - Sub-sampling of dartaset"""
        return self.cppMode.cent(pct, jsamp)

    def getStorageType(self):
        """Return type of storage"""
        return self.storage

    def zero(self):
        """Function to zero out a tensor"""
        self.cppMode.set(0.)
        return self

    def set(self, val):
        """Function to a tensor to a value"""
        self.cppMode.set(val)
        return self

    def getHyper(self):
        """Return the hypercube associated with the tensor"""
        return Hypercube.hypercube(hypercube=self.cppMode.getHyper())

    def getNdArray(self):
        """Return a numpy version of the array (same memory"""
        return numpy.array(self.cppMode, copy=False)

    def adjustHyper(self,hyper):
        """Adjust the hypercube associated with tensor. Does not reallocate. Must be same dims/size"""
        hyperOld=self.getHyper()
        if hyperOld.getN123() != hyper.getN123():
            raise Exception("Trying to reset with a different sized hyper")
        if len(hyperOld.axes) != len(hyper.axes):
            raise Exception("Trying to reset with a different sized hyper")
        self.cppMode.setHyper(hyper.cppMode)

    def reshape(self,**kw):
        """Reshape a vector (preserving the number
            of elements.

            Option 1; Specify hypercube
                hyper=hyper
            Option 2: Specify axes
                axes=axes
            Option 3: Specify ns
                ns=ns
            Option 4: reshape
                reshape=[] 
                    Example 1,2,4 will combine lenght
                    of axes and 4
            Option 5:
                    squish=True,
        """
        if "hyper" in kw:
            hyper=kw["hyper"]
            if not isinstance(hyper,Hypercube.hypercube):
                raise Exception("hyper must be of type hypercube")
        elif "axes" in kw:
            if not isinstance(kw["axes"],lst):
                raise Exception("axes must be a list")
            if not  isinstance(kw[axes][0],Hypercube.axis):
                raise Exception("Expecting axes to be a list of axes")
            hyper=Hypercube.hypercube(axes=kw["axes"])
        elif "ns" in kw:
            if not isinstance(ke["ns"],lst):
                raise Exception("Expecting lst for ns")
            hyper=Hypercube.hypercube(ns=kw["ms"])
        elif "reshape" in kw:
            if not  isinstance(kw["reshape"],lst):
                raise Exception("Expecting reshape to be a list")
            if kw["reshape"][0]<1:
                raise Exception("First axis must be >=1")
            last=0
            axes=self.getHyper().axes
            aout=[]
            for i,ax in enumerate(kw["reshape"]):
                if ax < last:
                    raise Exception("reshape can not decrease")
                elif ax >= len(axes):
                    raise Exception("Can not specify axis larger than im array")
                elif ax == last+1:
                    aout.append(axes[i-1])
                elif ax==last:
                    aout.append(Hypercube.axis(n=1))
                else:
                    n123=1
                    for i in range(last,ax):
                        n123*=axes[i].n
                    aout.append(Hypercube.axis(n=n123))
                hyper=Hypercube.hypercube(axes=aout)
        elif "squish" in kw:
            axes=self.getHyper().axes
            aout=[]
            for ax in axes:
                if ax.n !=1:
                    aout.append(ax)
            hyper=Hypercube.hypercube(axes=aout)
        else:
            raise Exception("Unknown way to reshape")
        if self.getHyper().getN123() != hyper.getN123():
            raise Exception("Input and output not the same size")
        vec=getSepTensor(hyper,storage=self.storage)
        vec.cppMode.copyElements(self.cppMode)
        return vec
    def windowInternal(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        axes = self.getHyper().axes
        nw,fw,jw=fixWindow(axes,**kw)
        return self.cppMode.window(nw, fw, jw)

    def checkSame(self, vec2):
        """Function to check if two tensors belong to the same tensor space"""
        return self.getCpp().checkSame(vec2.getCpp())


class floatTensor(tensor):
    """Generic float tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataFloat"

    def norm(self, N=2):
        """Function to compute tensor N-norm"""
        self.cppMode.norm(N)
        return self
    def scale(self, sc):
        """Function to scale a tensor"""
        self.cppMode.scale(sc)
        return self

    def __repr__(self):
        """Override print method"""
        return "FloatTensor\n%s"%str(self.getHyper())

    def calcHisto(self, nelem, mn, mx):
        """Calculate histogram
           mn - Minimum for cell
           mx - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        histo = getSepTensor(ns=[nelem], storage="dataInt")
        self.cppMode.calcHisto(histo.getCpp(), mn, mx)
        return histo

    def rand(self):
        """Function to fill with random numbers"""
        self.cppMode.rand()
        return self

    def clone(self):
        """Function to clone (deep copy) a tensor"""
        return floatTensor(fromCpp=self.cppMode.clone())

    def cliptensor(self, low, high):
        """Clip tensor element by element vec=min(high,max(low,vec))"""
        self.cppMode.cliptensor(low.cppMode, high.cppMode)
        return self

    def cloneSpace(self):
        """Funtion tor return the space of a tensor"""
        return floatTensor(fromCpp=self.cppMode.cloneSpace())

    def scaleAdd(self, vec2, sc1=1., sc2=1.):
        """self = self * sc1 + sc2 * vec2"""
        self.cppMode.scaleAdd(vec2.cppMode, sc1, sc2)
        return self

    def copy(self, vec2):
        """Copy vec2 into self"""
        self.scaleAdd(vec2, 0., 1.)
        return self

    def dot(self, vec2):
        """Compute dot product of two tensors"""
        return self.cppMode.dot(vec2.cppMode)

    def multiply(self, vec2):
        """self = vec2 * self"""
        self.cppMode.mult(vec2.cppMode)
        return self

    def norm(self, nrm=2):
        """Return the norm of a tensor"""
        return self.cppMode.norm(nrm)

    def window(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        return floatTensor(fromCpp=self.windowInternal(**kw))


class doubleTensor(tensor):
    """Generic double tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataDouble"

    def norm(self, N=2):
        """Function to compute tensor N - norm"""
        self.cppMode.norm(N)
        return self

    def scale(self, sc):
        """Function to scale a tensor"""
        self.cppMode.scale(sc)
        return self
    
    def __repr__(self):
        """Override print method"""
        return "DoubleTensor\n%s"%str(self.getHyper())
    
    def rand(self):
        """Function to fill with random numbers"""
        self.cppMode.rand()
        return self

    def cliptensor(self, low, high):
        """Clip tensor element by element vec=min(high,max(low,vec))"""
        self.cppMode.cliptensor(low.cppMode, high.cppMode)
        return self

    def calcHisto(self, nelem, mn, mx):
        """Calculate histogram
           mn - Minimum for cell
           mx - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        histo = getSepTensor(ns=[nelem], storage="dataInt")
        self.cppMode.calcHisto(histo.getCpp(), mn, mx)
        return histo

    def clone(self):
        """Function to clone (deep copy) a tensor"""
        return doubleTensor(fromCpp=self.cppMode.clone())

    def copy(self, vec2):
        """Copy vec2 into self"""
        self.scaleAdd(vec2, 0., 1.)
        return self

    def cloneSpace(self):
        """Funtion tor return the space of a tensor"""
        return doubleTensor(fromCpp=self.cppMode.cloneSpace())

    def scaleAdd(self, vec2, sc1=1., sc2=1.):
        """self = self * sc1 + sc2 * vec2"""
        self.cppMode.scaleAdd(vec2.cppMode, sc1, sc2)
        return self

    def dot(self, vec2):
        """Compute dot product of two tensors"""
        return self.cppMode.dot(vec2.cppMode)

    def multiply(self, vec2):
        """self = vec2 * self"""
        self.cppMode.mult(vec2.cppMode)
        return self

    def norm(self, nrm=2):
        """Return the norm of a tensor"""
        return self.cppMode.norm(nrm)

    def window(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        return doubleTensor(fromCpp=self.windowInternal(**kw))


class intTensor(tensor):
    """Generic int tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataInt"

    def __repr__(self):
        """Override print method"""
        return "IntTensor\n%s"%str(self.getHyper())

    def clone(self):
        """Function to clone (deep copy) a tensor"""
        return intTensor(fromCpp=self.cppMode.clone())
    def window(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        return intTensor(fromCpp=self.windowInternal(**kw))
    def windowInternal(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        axes = self.getHyper().axes
        nw,fw,jw=fixWindow(axes,**kw)
        return self.cppMode.window(nw, fw, jw)

class longTensor(tensor):
    """Generic int tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataInt"

    def __repr__(self):
        """Override print method"""
        return "LongTensor\n%s"%str(self.getHyper())

    def clone(self):
        """Function to clone (deep copy) a tensor"""
        return longTensor(fromCpp=self.cppMode.clone())
    def window(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        return longTensor(fromCpp=self.windowInternal(**kw))
    def windowInternal(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        axes = self.getHyper().axes
        nw,fw,jw=fixWindow(axes,**kw)
        return self.cppMode.window(nw, fw, jw)

class complexTensor(tensor):
    """Generic complex tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataComplex"

    def cloneSpace(self):
        """Funtion tor return the space of a tensor"""
        return complexTensor(fromCpp=self.cppMode.cloneSpace())

    def norm(self, N=2):
        """Function to compute tensor N-norm"""
        return self.cppMode.norm(N)

    def zero(self):
        """Function to zero out a tensor"""
        self.cppMode.set(0.)
        return self

    def multiply(self, vec2):
        """self = vec2 * self"""
        self.cppMode.mult(vec2.cppMode)
        return self

    def rand(self):
        """Fill with random numbers"""
        self.cppMode.rand()
        return self

    def copy(self, vec2):
        """Copy vec2 into self"""
        self.scaleAdd(vec2, 0., 1.)
        return self
    def __repr__(self):
        """Override print method"""
        return "ComplexTensor\n%s"%str(self.getHyper())

    def clone(self):
        """clone a tensor"""
        return complexTensor(fromCpp=self.cppMode.clone())

    def cliptensor(self, low, high):
        """Clip tensor element by element vec=min(high,max(low,vec))"""
        self.cppMode.cliptensor(low.cppMode, high.cppMode)
        return self

    def dot(self, vec2):
        """Compute dot product of two tensors"""
        return self.cppMode.dot(vec2.cppMode)

    def scale(self, sc):
        """Function to scale a tensor"""
        self.cppMode.scale(sc)
        return self

    def scaleAdd(self, vec2, sc1=1., sc2=1.):
        """self = self * sc1 + sc2 * vec2"""
        self.cppMode.scaleAdd(vec2.cppMode, sc1, sc2)
        return self

    def window(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        return complexTensor(fromCpp=self.windowInternal(**kw))

class complexDoubleTensor(tensor):
    """Generic complex tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataComplexDouble"

    def cloneSpace(self):
        """Funtion tor return the space of a tensor"""
        return complexDoubleTensor(fromCpp=self.cppMode.cloneSpace())

    def norm(self, N=2):
        """Function to compute tensor N-norm"""
        return self.cppMode.norm(N)
    def __repr__(self):
        """Override print method"""
        return "ComplexDoubleTensor\n%s"%str(self.getHyper())
    def zero(self):
        """Function to zero out a tensor"""
        self.cppMode.set(0.)
        return self

    def multiply(self, vec2):
        """self = vec2 * self"""
        self.cppMode.mult(vec2.cppMode)
        return self

    def rand(self):
        """Fill with random numbers"""
        self.cppMode.rand()
        return self

    def copy(self, vec2):
        """Copy vec2 into self"""
        self.scaleAdd(vec2, 0., 1.)
        return self

    def clone(self):
        """clone a tensor"""
        return complexDoubleTensor(fromCpp=self.cppMode.clone())

    def cliptensor(self, low, high):
        """Clip tensor element by element vec=min(high,max(low,vec))"""
        self.cppMode.cliptensor(low.cppMode, high.cppMode)
        return self

    def dot(self, vec2):
        """Compute dot product of two tensors"""
        return self.cppMode.dot(vec2.cppMode)

    def scale(self, sc):
        """Function to scale a tensor"""
        self.cppMode.scale(sc)
        return self

    def scaleAdd(self, vec2, sc1=1., sc2=1.):
        """self = self * sc1 + sc2 * vec2"""
        self.cppMode.scaleAdd(vec2.cppMode, sc1, sc2)
        return self

    def window(self, **kw):
        """Window a tensor return another tensor (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max """
        return complexDoubleTensor(fromCpp=self.windowInternal(**kw))


class byteTensor(tensor):
    """Generic byte tensor class"""

    def __init__(self, **kw):
        self.kw = kw
        super().__init__()
        self.storage = "dataByte"

    def calcHisto(self, nelem, mn, mx):
        """Calculate histogram
           mn - Minimum for cell
           mx - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        histo = getSepTensor(ns=[nelem], storage="dataInt")
        self.cppMode.calcHisto(histo.getCpp(), mn, mx)
        return histo
    def clone(self):
        """Function to clone (deep copy) a tensor"""
        return byteTensor(fromCpp=self.cppMode.clone())
    def __repr__(self):
        """Override print method"""
        return "ByteTensor\n%s"%str(self.getHyper())


def getSepTensor(*args, **keys):
    """ Get a septensor object
            Option 1 (supply hypercube):
                    hyper, kw args
            Option 2 (build hypercube):
                    ns = [] - list of sizes
                    os = [] - list of origins
                    ds = [] - list os sampling
                    labels = [] list of labels
                    axes = [] list of axes
            Option 3 (get 2 - D slice)
                    tensor - tensor to grab from
                    iax1, iax2 - axes to grab
                    rev1, rev1 - whether or not to reverse axes
                    beg, end - beg and end position for all axes(lists)
            storage = StorageType(dataFloat[default], dataComplex,dataComplexDouble,
                        dataDouble,dataInt,dataByte)

            Option 4 (numpy)
                Provide hyper, ns, os, or ds,label,s axes


    """
    myt = "dataFloat"
    haveHyper=False
    haveNumpy=False
    array=None
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
        elif "tensor" in keys:
            if "iax1" in keys and "iax2" in keys and "rev1" in keys and "rev2" in keys and "ipos" in keys and "beg" in keys and "end" in keys:
                if isinstance(keys["tensor"], FloatTensor):
                    return floatTensor(fromCpp=pySepVector.float2DReg(keys["tensor"].getCpp(), keys["iax1"], keys["rev1"],
                                                                      keys["iax2"], keys["rev2"], keys["ipos"], keys["beg"], keys["end"]))
                elif isinstance(keys["tensor"], DoubleTensor):
                    return doubleTensor(fromCpp=pySepVector.double2DReg(keys["tensor"].cppMode, keys["iax1"], keys["rev1"],
                                                                        keys["iax2"], keys["rev2"], keys["ipos"], keys["beg"], keys["end"]))
                elif isinstance(keys["tensor"], IntTensor):
                    return intTensor(fromCpp=pySepVector.int2DReg(keys["tensor"].cppMode, keys["iax1"], keys["rev1"],
                                                                  keys["iax2"], keys["rev2"], keys["ipos"], keys["beg"], keys["end"]))
                elif isinstance(keys["tensor"], ByteTensor):
                    return byteTensor(fromCpp=pySepVector.byte2DReg(keys["tensor"].cppMode, keys["iax1"], keys["rev1"],
                                                                    keys["iax2"], keys["rev2"], keys["ipos"], keys["beg"], keys["end"]))
            else:
                raise Exception("Must supply iax1,iax2,rev1,rev2,ipos,beg,end")
        else:
            raise Exception("Must supply Hypercube,tensor  or ns/axes")
    else:
        raise Exception(
            "Only understand 0 or 1 (hypercube) non-keyword arguments")



    if haveNumpy:
        if not str(array.dtype) in dtypeToSepVecType:
            raise Exception("Numpy array not supported",str(array.dtype))
        myt=dtypeToSepVecType[str(array.dtype)]
    else:
        myt="dataFloat"
        if "storage" in keys:
            myt = keys["storage"]

    if myt == "dataFloat":
        x = getFloatTensor(hyper)
        y= floatTensor(fromCpp=x)
    elif myt == "dataComplexDouble":
        x = getComplexDoubleTensor(hyper)
        y=complexDoubleTensor(fromCpp=x)
    elif myt == "dataComplex":
        x = getComplexTensor(hyper)
        y=complexTensor(fromCpp=x)
    elif myt == "dataDouble":
        x = getDoubleTensor(hyper)
        y= doubleTensor(fromCpp=x)
    elif myt == "dataInt":
        x = getIntTensor(hyper)
        y= intTensor(fromCpp=x)
    elif myt == "dataByte":
        x = getByteTensor(hyper)
        y= byteTensor(fromCpp=x)
    else:
        raise Exception("Unknown type %s" % myt)
    if haveNumpy:
        numpy.copyto(y.getNdArray(),array)
    return y

def getCppSepTensor(hyper, **keys):
    h = hyper.getCpp()
    myt = "dataFloat"
    if "storage" in keys:
        myt = keys["storage"]
    if myt == "datFloat":
        x = getFloatTensor(hyper)
    elif myt == "dataComplex":
        x = getComplexTensor(hyper)
    elif myt == "dataComplexDouble":
        x = getComplexDoubleTensor(hyper)
    elif myt == "dataDouble":
        x = getDoubleTensor(hyper)
    elif myt == "dataInt":
        x = getIntTensor(hyper)
    elif myt == "dataByte":
        x = getByteTensor(hyper)
    else:
        raise Exception("Unknown type" % myt)


def getFloatTensor(hyper):
    h = hyper.getCpp()
    if len(hyper.axes) == 1:
        return pySepVector.floatTensor1D(h)
    elif len(hyper.axes) == 2:
        return pySepVector.floatTensor2D(h)
    elif len(hyper.axes) == 3:
        return pySepVector.floatTensor3D(h)
    elif len(hyper.axes) == 4:
        return pySepVector.floatTensor4D(h)
    elif len(hyper.axes) == 5:
        return pySepVector.floatTensor5D(h)
    elif len(hyper.axes) == 6:
        return pySepVector.floatTensor6D(h)
    elif len(hyper.axes) == 7:
        return pySepVector.floatTensor7D(h)

def getComplexTensor(hyper):
    h = hyper.getCpp()

    if len(hyper.axes) == 1:
        return pySepVector.complexTensor1D(h)
    elif len(hyper.axes) == 2:
        return pySepVector.complexTensor2D(h)
    elif len(hyper.axes) == 3:
        return pySepVector.complexTensor3D(h)
    elif len(hyper.axes) == 4:
        return pySepVector.complexTensor4D(h)
    elif len(hyper.axes) == 5:
        return pySepVector.complexTensor5D(h)
    elif len(hyper.axes) == 6:
        return pySepVector.complexTensor6D(h)
    elif len(hyper.axes) == 7:
        return pySepVector.complexTensor7D(h)
def getComplexDoubleTensor(hyper):
    h = hyper.getCpp()

    if len(hyper.axes) == 1:
        return pySepVector.complexDoubleTensor1D(h)
    elif len(hyper.axes) == 2:
        return pySepVector.complexDoubleTensor2D(h)
    elif len(hyper.axes) == 3:
        return pySepVector.complexDoubleTensor3D(h)
    elif len(hyper.axes) == 4:
        return pySepVector.complexDoubleTensor4D(h)
    elif len(hyper.axes) == 5:
        return pySepVector.complexDoubleTensor5D(h)
    elif len(hyper.axes) == 6:
        return pySepVector.complexDoubleTensor6D(h)
    elif len(hyper.axes) == 7:
        return pySepVector.complexDoubleTensor7D(h)


def getByteTensor(hyper):
    h = hyper.getCpp()

    if len(hyper.axes) == 1:
        return pySepVector.byteTensor1D(h)
    elif len(hyper.axes) == 2:
        return pySepVector.byteTensor2D(h)
    elif len(hyper.axes) == 3:
        return pySepVector.byteTensor3D(h)
    elif len(hyper.axes) == 4:
        return pySepVector.byteTensor4D(h)
    elif len(hyper.axes) == 5:
        return pySepVector.byteTensor5D(h)
    elif len(hyper.axes) == 6:
        return pySepVector.byteTensor6D(h)


def getDoubleTensor(hyper):
    h = hyper.getCpp()
    if len(hyper.axes) == 1:
        return pySepVector.doubleTensor1D(h)
    elif len(hyper.axes) == 2:
        return pySepVector.doubleTensor2D(h)
    elif len(hyper.axes) == 3:
        return pySepVector.doubleTensor3D(h)
    elif len(hyper.axes) == 4:
        return pySepVector.doubleTensor4D(h)
    elif len(hyper.axes) == 5:
        return pySepVector.doubleTensor5D(h)
    elif len(hyper.axes) == 6:
        return pySepVector.doubleTensor6D(h)
    elif len(hyper.axes) == 7:
        return pySepVector.doubleTensor7D(h)

def getIntTensor(hyper):
    h = hyper.getCpp()
    if len(hyper.axes) == 1:
        return pySepVector.intTensor1D(h)
    elif len(hyper.axes) == 2:
        return pySepVector.intTensor2D(h)
    elif len(hyper.axes) == 3:
        return pySepVector.intTensor3D(h)
    elif len(hyper.axes) == 4:
        return pySepVector.intTensor4D(h)
    elif len(hyper.axes) == 5:
        return pySepVector.intTensor5D(h)
    elif len(hyper.axes) == 6:
        return pySepVector.intTensor6D(h)


class rectFilter1D(floatTensor):

    def __init__(self, n, f, pef=False):
        """Initialize a rectFilter1D
                n - size of box(list 1 - D)
                f - location of the 1 (list 1 - D) (set to - 1)
                pef - Whether or not this is a PEF"""
        self.cppMode = pySepVector.rectFilter1D(n, f, pef)


class rectFilter2D(floatTensor):

    def __init__(self, *arg, **kw):
        """Initialize a rectFilter2D
            Option 1:
                n - size of box(list 2 - D)
                f - location of the 1 (list 2 - D) (set to - 1)
                pef- Whether or not the filter is a PEF
            Option 2:
                clone=True/False True-with memory, False without"""
        if len(arg) == 3:
            self.cppMode = pySepVector.rectFilter2D(arg[0], arg[1], arg[2])
        elif len(arg) == 2:
            self.cppMode = pySepVector.rectFilter2D(arg[0], arg[1], False)
        elif "clone" in kw:
            if "space" in kw:
                if kw["space"]:
                    self.cppMode = kw["clone"].cloneSpace()
                else:
                    self.cppMode = kw["clone"].clone()
            else:
                self.cppMode = kw["clone"].clone()
        else:
            raise Exception("Unknown way to create rectFilter2D")

    def clone(self):
        """Function to clone (deep copy) a tensor"""
        return rectFilter2D(clone=self.cppMode)

    def cloneSpace(self):
        """Funtion tor return the space of a tensor"""
        return rectFilter2D(clone=self.cppMode, space=False)


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
            bi = int(float(kw["min" %
                                i] - axes[i - 1].o) / axes[i - 1].d + .5)
            biSet = True
        if "max%d" % i in kw:
            ei = int(float(kw["max" %
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
