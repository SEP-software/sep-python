""" Class for storing regular data  """
import logging
import numba
import numpy as np
from sys import version_info
from genericSolver.pyVector import vector as pyvec
from sep_python.hypercube import Hypercube,Axis
import sep_python.sep_converter
import sep_python.sep_proto


converter=sep_python.sep_converter.converter

@numba.njit(parallel=True)
def clip_it(vec,bclip,eclip):
    """Numba clip function"""
    for i in numba.prange(vec.shape[0]):
        vec[i]=min(eclip,max(bclip,vec[i]))

@numba.njit(parallel=True)
def clip_array(vec,bclip,eclip):
    """Clip based on vectors

    Args:
        vec ((1d-array): in/out vectors
        bclip (1d-array): min
        eclip ((1d-array): mac
    """
    for i in numba.prange(vec.shape[0]):
        vec[i]=min(eclip[i],max(bclip[i],vec[i]))

@numba.njit(parallel=True)
def norm1(vec):
    """Norm 1"""
    tot=0
    for i in numba.prange(vec.shape[0]):
        tot+=np.abs(vec[i])
    return tot

@numba.njit(parallel=True)
def scale_add(vec1,vec2,sc1,sc2):
    """Scale and add two vectors"""
    for i in numba.prange(vec1.shape[0]):
        vec1[i]=vec1[i]*sc1+vec2[i]*sc2

@numba.njit(parallel=True)
def scale(vec,sc):
    """Scale a vector"""
    for i in numba.prange(vec.shape[0]):
        vec[i]=vec[i]*sc

@numba.njit(parallel=True)
def dot_it(vec1,vec2):
    """Dot product of two vectors"""
    val=0
    for i in numba.prange(vec1.shape[0]):
        val+=vec1[i]*vec2[i]
    return val

@numba.njit(parallel=True)
def multiply_it(vec1,vec2):
    """Multiply two vectors element by element"""
    for i in numba.prange(vec1.shape[0]):
        vec1[i]=vec1[i]*vec2[i]

@numba.njit()
def calc_histo(out_vec,vec,min_val,max_val):
    """Calc the histogram of a vector"""
    delta=(max_val-min_val)/vec.shape[0]
    for i in range(vec.shape[0]):
        ind=max(0,min(vec.shape[0]-1,int((vec[i]-min_val)/delta)))
        out_vec[ind]+=1

class Vector(sep_python.sep_proto.MemReg,pyvec):
    """Generic sepVector class"""

    def __init__(self, hyper:Hypercube, data_format:str):
        """Initialize a vector object"""

        self._logger=logging.getLogger(None)
        sep_python.sep_proto.MemReg.__init__(self)
        pyvec.__init__(self)
        self.set_hyper(hyper)
        self._data_format=data_format
    def set_logger(self,logger):
        """Set the logger for the vector

        logger - Set the vector for logger
        """
        self._logger=logger

    def get_data_format(self)->str:
        """Return type of data_format"""
        return self._data_format

    def zero(self):
        """Function to zero out a vector"""
        self.get_nd_array().fill(0.)
        return self

    def set(self, val):
        """Function to a vector to a value"""
        self.get_nd_array().fill(val)
        return self

    def get_nd_array(self)->np.ndarray:
        """Return a numpy version of the array (same memory"""
        return self._arr

    def min(self):
        """Find the minimum of array"""
        return self._arr.min()

    def max(self):
        """Find the maximum of an array"""
        return self._arr.max()
        
    def window(self, compress=False, **kw):
        """Window a vector return another vector (of the same dimension
            specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
            any of these by lists.
            Can not specify n and min or max
            compress=False Whether or not to compress the axes of length 1

            """
        axes =  self.get_hyper().axes
        n_wind,f_wind,j_wind=fix_window(axes,**kw)

        ns=n_wind
        fs=f_wind
        js=j_wind

        axis_out=[]
        axis_c_out=[]
        nout=[]
        for i in range(len(n_wind)):
            axis_out.append(Axis(n=n_wind[i],o=axes[i].o+axes[i].d*f_wind[i],d=j_wind[i]*axes[i].d,\
                label=axes[i].label, unit=axes[i].unit))
            if n_wind[i]!=1 and compress:
                axis_c_out.append(axis_out[:-1])
                nout.append(n_wind[i])

        vec=get_sep_vector(axes=axis_out)
        #out_array=vec.get_nd_array()
        in_array=self.get_nd_array()
        ndim=len(axis_out)
        if ndim==1:
            out_array=in_array[fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==2:
            out_array=in_array[fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==3:
            out_array=in_array[fs[2]:js[2]*ns[2]:js[2],
              fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==4:
            out_array=in_array[fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==5:
            out_array=in_array[fs[4]:js[4]*ns[4]:js[4],fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==6:
            out_array=in_array[fs[5]:js[5]*ns[5]:js[5],fs[4]:js[4]*ns[4]:js[4],\
                fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]
        elif ndim==7:
            out_array=in_array[fs[6]:fs[6]*js[6]:js[6],\
                fs[5]:js[5]*ns[5]:js[5],fs[4]:js[4]*ns[4]:js[4],fs[3]:js[3]*ns[3]:js[3],\
                fs[2]:js[2]*ns[2]:js[2],fs[1]:js[1]*ns[1]:js[1],fs[0]:js[0]*ns[0]:js[0]]   
        if not compress:
            return vec
        vec_out=get_sep_vector(axes=axis_c_out)
        vec_out.get_nd_array=out_array
        #arO=vec_out.get_nd_array()
        #arO=out_array.reshape(tuple(nout[::-1]))
        #         return vec_out

    def get_1d_array(self)->np.ndarray:
        """Get 1-D representation of array"""
        return np.ravel(self._arr)

    def adjust_hyper(self,hyper:Hypercube):
        """
            Adjust the Hypercube associated with vector. Does not reallocate. 
            Must be same dims/size
        """
        hyper_old=self.get_hyper()
        if hyper_old.getN123() != hyper.getN123():
            self._logger.fatal("Trying to reset with a different sized hyper")
            raise Exception("")
        new_shape=tuple(np.flip(tuple(hyper.get_ns())))
        self._arr=np.reshape(self._ar,new_shape)
        self._hyper=hyper

    def check_same(self, vec2)->bool:
        """Function to check if two vectors belong to the same vector space"""
        if vec2.get_data_format() != self.get_data_format():
            return False
        return self.get_hyper().check_same(vec2.get_hyper())
    
class NonInteger(Vector):
    """A class for non-integers"""
    def __init__(self,hyper:Hypercube, data_format:str):
        """Initialize a non-integer"""
        super().__init__(hyper,data_format)

class RealNumber(NonInteger):
    """A class for real numbers"""

    def __init__(self,hyper:Hypercube,data_format:str):
        """Initialize a real number vector"""
        super().__init__(hyper,data_format)

    def clip(self, bclip, eclip):
        """Clip dataset
                bclip - Minimum value to clip to
                eclip - Maximum value to clip to"""
        clip_it(self.get1DArray(),bclip,eclip)

    def cent(self, pct:float, jsamp=1):
        """Calculate the percentile of a dataset
                pct - Percentile of the dataset
                jsamp - Sub-sampling of dartaset"""
        return np.percentile(self._arr,pct)

    def rand(self)->Vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape)
        return self

    def clip_vector(self, low:Vector, high:Vector)->Vector:
        """Clip vector element by element vec=min(high,max(low,vec))"""
        if not self.check_same(low) or self.check_same(high):
            self._logger.fatal("low,high, and vector must all be the same space")
            raise Exception("")
        clip_array(self.get1DArray(),low.get1DArray(), high.get1DArray())
        return self

    def scale_add(self, vec2:Vector, sc1=1., sc2=1.)->Vector:
        """self = self * sc1 + sc2 * vec2"""
        if not self.check_same(vec2) or self.get_data_format()!=vec2.get_data_format():
            self._logger.fatal("must be of the same space and type")
            raise Exception("")
        scale_add(self.get1DArray(),vec2.get1DArray(),sc1,sc2)
        return self

    def scale(self,sc)->Vector:
        """self = self * sc"""
        scale(self.get_1d_array(),sc)
        return self

    def calc_histo(self, nelem, min_val, max_val):
        """Calculate histogram
           min_val - Minimum for cell
           max_val - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        #ar=self.get1DArray()
        #CHANGE
        histo=self.get_sep_vector()
        histo = get_sep_vector(ns=[nelem], data_format="int32")
        self.cppMode.calc_histo(histo.getCpp(), min_val, max_val)
        return histo

    def copy(self, vec2:Vector)->Vector:
        """Copy vec2 into self"""
        if not self.check_same(vec2) or self.get_data_format()!=vec2.get_data_format():
            self._logger.fatal("must be of the same space and type")
            raise Exception("")
        scale_add(self.get1DArray(),vec2.get_1d_array(),0.,1.)
        return self

    def dot(self, vec2:Vector)->Vector:
        """Compute dot product of two vectors"""
        if not self.check_same(vec2) or self.get_data_format()!=vec2.get_data_format():
            self._logger.fatal("must be of the same space and type")
            raise Exception("")
        return dot_it(self.get1DArray(),vec2.get1DArray())
    
    def norm(self, N=2):
        """Function to compute vector N-norm"""
        if N==1:
            return  norm1(self.get1Darray())
        elif N==2:
            return self.dot(self)/2
        else:
            self._logger.fatal("Only can do norm 1 and 2")
            raise Exception("")
   
    def multiply(self, vec2:Vector)->Vector:
        """self = vec2 * self"""
        if not self.check_same(vec2) or self.get_data_format()!=vec2.get_data_format():
            self._logger.fatal("must be of the same space and type")
            raise Exception("")
        multiply_it(self.get_1d_array(),vec2.get_1d_array())
        return self


class FloatVector(Vector):
    """Generic float vector class"""

    def __init__(self, hyper:Hypercube,space_only:bool=False):
        super().__init__(hyper,"dataFloat")
        if not space_only:
            self._arr=np.ndarray(tuple(hyper.get_ns()[::-1]),dtype=np.float32)
 
    def __repr__(self):
        """Override print method"""
        return "FloatVector\n%s"%str(self.get_hyper())

    def rand(self)->Vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape).astype("f4")
        return self

    def clone(self):
        """Function to clone (deep copy) a vector"""
        return FloatVector(self.get_hyper())

    def clone_space(self):
        """Funtion tor return the space of a vector"""
        return FloatVector(self.get_hyper(),space_only=True)

class Double_Vector(Vector):
    """Generic double vector class"""

    def __init__(self, hyper:Hypercube,space_only=False):
        super().__init__(hyper,"double64")
        if not space_only:
            self._arr=np.ndarray(tuple(hyper.get_ns()[::-1]),dtype=np.float64)

    def rand(self)->Vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape)
        return self
    
    def __repr__(self):
        """Override print method"""
        return "Double_Vector\n%s"%str(self.get_hyper())
    
    def clone(self):
        """Function to clone (deep copy) a vector"""
        return Double_Vector(self.get_hyper())

    def clone_space(self):
        """Funtion tor return the space of a vector"""
        return Double_Vector(self.get_hyper(),space_only=True)


class IntVector(Vector):
    """Generic int vector class"""

    def __init__(self, hyper:Hypercube,space_only=False):
        super().__init__(hyper,"dataInt")
        if not space_only:
            self._arr=np.ndarray(tuple(hyper.get_ns()[::-1]),dtype=np.int32)

    def __repr__(self):
        """Override print method"""
        return "IntVector\n%s"%str(self.get_hyper())

    def clone(self):
        """Function to clone (deep copy) a vector"""
        return IntVector(self.get_hyper())


class ComplexVector(Vector):
    """Generic complex vector class"""

    def __init__(self, hyper:Hypercube,space_only=False):
        super().__init__(hyper,"float32")
        if not space_only:
            self._arr=np.ndarray(tuple(hyper.get_ns()[::-1]),dtype=np.complex64)

    def clone_space(self):
        """Funtion tor return the space of a vector"""
        return ComplexVector(self.get_hyper(),space_only=True)

    def __repr__(self):
        """Override print method"""
        return "ComplexVector\n%s"%str(self.get_hyper())

    def clone(self):
        """clone a vector"""
        return ComplexVector(self.get_hyper())

    def rand(self)->Vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape).astype("f4")+\
            1j*np.random.random(self._arr.shape).astype("f4")
        return self

class ComplexDoubleVector(Vector):
    """Generic complex vector class"""

    def __init__(self, hyper:Hypercube,space_only=False):
        super().__init__(hyper,"complex128")
        if not space_only:
            self._arr=np.ndarray(tuple(hyper.get_ns()[::-1]),dtype=np.complex128)

    def clone_space(self):
        """Funtion tor return the space of a vector"""
        return ComplexDoubleVector(self.get_hyper(),space_only=True)

    def norm(self, N=2):
        """Function to compute vector N-norm"""
        return self.cppMode.norm(N)

    def __repr__(self):
        """Override print method"""
        return "ComplexDoubleVector\n%s"%str(self.get_hyper())

    def rand(self)->Vector:
        """Function to fill with random numbers"""
        self._arr=np.random.random(self._arr.shape)+1j*np.random.random(self._arr.shape)
        return self

    def clone(self):
        """clone a vector"""
        return ComplexDoubleVector(self.get_hyper())

    def clip_vector(self, low, high):
        """Clip vector element by element vec=min(high,max(low,vec))"""
        clip_array(self.get_nd_arraty(),low.get_nd_array(), high.get_nd_array())
        return self

class ByteVector(Vector):
    """Generic byte vector class"""

    def __init__(self, hyper:Hypercube,space_only=False):
        super().__init__(hyper,"dataByte")
        if not space_only:
            self._arr=np.ndarray(tuple(hyper.get_ns()[::-1]),dtype=np.uint8)

    def calc_histo(self, nelem, min_val, max_val):
        """Calculate histogram
           min_val - Minimum for cell
           max_val - Maximum for cell
           nelem - Return number of elements in histogram

           @return Histogram """
        histo = get_sep_vector(ns=[nelem], data_format="dataInt")
        calc_histo(histo,self.get_nd_array(),min_val,max_val)
        return histo
    def clone(self):
        """Function to clone (deep copy) a vector"""
        return ByteVector(self.get_hyper())
    def __repr__(self):
        """Override print method"""
        return "ByteVector\n%s"%str(self.get_hyper())

def get_sep_vector(*args, **keys,):
    """ Get a sepVector object
            Option 1 (supply Hypercube):
                    hyper, kw args
            Option 2 (build Hypercube):
                    ns = [] - list of sizes
                    os = [] - list of origins
                    ds = [] - list os sampling
                    labels = [] list of labels
                    axes = [] list of axes

            data_format = data_formatType(float32[default], float64,double64,
                            int32,complex64,complex128)

            Option 4 (numpy)
                Provide hyper, ns, os, or ds,label,s axes

            Optional:
                logger - Provide a logger for errors
    """
    myt = "float32"
    #have_hyper=False
    have_numpy=False
    array=None

    if "logger" in keys:
        logger=keys["loggger"]
    else:
        logger=logging.getLogger(None)


    if "data_format" in keys:
        keys["data_format"]=keys["data_format"]
    if len(args) == 1:
        if isinstance(args[0],Hypercube):
            have_hyper=True
            hyper = args[0]
        elif isinstance(args[0],np.ndarray):
            have_numpy=True
            array=args[0]
            if "hyper" in keys:
                hyper=keys["hyper"]
            elif "axes" in keys or "ns" in keys:
                hyper = Hypercube(**keys)
            else:
                nt=list(array.shape)
                ns=[]
                for i in range(len(nt)):
                    ns.append(nt[len(nt)-1-i])
                hyper =Hypercube(ns)
        else:
            logger.fatal("First argument must by a Hypercube or numpy array")
            raise Exception("")
    elif len(args) == 0:
        if "axes" in keys or "ns" in keys:
            hyper = Hypercube(**keys)
       
        else:
            logger.fatal("Must supply Hypercube,vector  or ns/axes")
            raise Exception("")
    else:
        logger.fatal("Only understand 0 or 1 (Hypercube) non-keyword arguments")
        raise Exception("")


    if have_numpy:
        if not converter.valid_type(str(array.dtype)):
            logger.fatal(f"Numpy array type {array.dtype} not supported")
            raise Exception("")
        myt=converter.get_name(str(array.dtype))
    else:
        myt="float32"
        if "data_format" in keys:
            myt = converter.get_name(keys["data_format"])

    if myt == "float32":
        out_type= FloatVector(hyper)
    elif myt == "complex128":
        out_type=ComplexDoubleVector(hyper)
    elif myt == "complex64":
        out_type=ComplexVector(hyper)
    elif myt == "float64":
        out_type= Double_Vector(hyper)
    elif myt == "int32":
        out_type= IntVector(hyper)
    elif myt == "uint8":
        out_type= ByteVector(hyper)
    else:
        logger.fatal(f"Unknown type {myt}")
        raise Exception("")
    if have_numpy:
        np.copyto(out_type.get_nd_array(),array)
    return out_type

def rea_col_textfile(file):
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

def fix_window(axes,**kw):
    """Create full window parameters

    axes - Axes for dataset
    kw = n1, f1, j1 - Window parameters

    returns
      n_wind,f_wind,j_wind - Full window paramters
    """
    ndim = len(axes)
    n_wind = []
    f_wind = []
    j_wind = []

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
        bi_set = False
        ei_set = False
        if "min%d" % i in kw:
            bi = int(float(kw["min%d" %
                                i] - axes[i - 1].o) / axes[i - 1].d + .5)
            bi_set = True
        if "max%d" % i in kw:
            ei = int(float(kw["max%d" %
                                i] - axes[i - 1].o) / axes[i - 1].d + .5)
            ei_set = True
        if fset:
            if axes[i - 1].n <= f:
                logging.getLogger().fatal("invalid f{i}={f} parameter n{i} of data={axes[i-1].n")
                raise Exception("")
        if nset:
            if axes[i - 1].n < n:
                logging.getLogger().fatal(f"invalid n{i}={n} parameter n{i} of data={axes[i-1].n}")
                raise Exception("")
        if jset and j <= 0:
            logging.getLogger().fatal(f"invalid j{i}={j}")
            raise Exception("")
        if not jset:
            j = 1
        if not nset:
            if not fset:
                if not bi_set:
                    f = 0
                elif bi < 0 or bi >= axes[i - 1].n:
                    logging.getLogger().fatal(f"Invalid min{i}={kw['min%d'%i]}")
                    raise Exception("")
                else:
                    f = bi
            if ei_set:
                if ei <= f or ei >= axes[i - 1].n:
                    logging.getLogger().fatal(f"Invalid max{i}={kw['min%d'%i]}")

                else:
                    n = (ei - f - 1) / j + 1
            else:
                n = (axes[i - 1].n - f - 1) / j + 1

            if not bi_set and not ei_set and not jset and not fset:
                n = axes[i - 1].n
                j = 1
                f = 0
        elif not fset:
            if not bi_set:
                f = 0
            elif bi < 0 or bi >= axes[i - 1].n:
                logging.getLogger().fatal(f"Invalid min{i}={kw['min%d'%i]}")
                raise Exception("")
            else:
                f = int(bi)
        if axes[i - 1].n < (1 + f + j * (n - 1)):
            logging.getLogger().fatal("Invalid window parameter")
            raise Exception("")
        n_wind.append(int(n))
        f_wind.append(int(f))
        j_wind.append(int(j))
    return n_wind,f_wind,j_wind
