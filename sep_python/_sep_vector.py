""" Class for storing regular data  """
import logging
import numba
import numpy as np
import copy
from generic_solver import vectorIC as pyvec
from sep_python import Hypercube, Axis
import sep_python._sep_converter
import sep_python._sep_proto

converter = sep_python._sep_converter.converter


@numba.njit()
def calc_histo(out_vec, vec, min_val, max_val):
    """Calc the histogram of a vector"""
    delta = (max_val - min_val) / vec.shape[0]
    for i in range(vec.shape[0]):
        ind = max(0, min(vec.shape[0] - 1, int((vec[i] - min_val) / delta)))
        out_vec[ind] += 1


class Vector(sep_python._sep_proto.MemReg, pyvec):
    """Generic sepVector class"""

    def __init__(self, hyper: Hypercube, data_type: str, vals=None, space_only=False):
        """Initialize a vector object"""

        self.arr = None
        self._logger = logging.getLogger(None)
        sep_python._sep_proto.MemReg.__init__(self)
        self.set_hyper(hyper)
        self.set_data_type(data_type)
        if vals is not None:
            if not isinstance(vals, np.ndarray):
                raise Exception("Vals must be ndarray")
            if vals.size == 0:  # empty array
                arr = np.zeros(tuple(reversed(hyper.get_ns())), dtype=data_type)
                pyvec.__init__(self, arr)
            else:
                if converter.get_numpy(data_type) != vals.dtype:
                    raise Exception(
                        "Data types do not match %s %s",
                        converter.get_numpy(data_type),
                        vals.dtype,
                    )
                pyvec.__init__(self, copy.deepcopy(vals))
        elif not space_only:
            arr = np.zeros(tuple(reversed(hyper.get_ns())), dtype=data_type)
            pyvec.__init__(self, arr)
        else:
            arr = np.zeros(tuple(reversed(hyper.get_ns())), dtype=data_type)
            pyvec.__init__(self, arr)
            self.arr = np.empty(0, dtype=self.getNdArray().dtype)

    def set_logger(self, logger):
        """Set the logger for the vector

        logger - Set the vector for logger
        """
        self._logger = logger

    def shape(self):
        """Return shape of vector"""
        return self.get_hyper().get_ns().reverse()


    def __getitem__(self,key):
        """Set a value"""
        return self.val[key]
    
    def  __setitem__(self, key, newvalue):
        """Set a value in the array"""
        self.val[key]=newvalue
        
    def get_numpy_hyper(self):
        """Return a reversed hypercube, numpy order"""
        return self.get_hyper().reverse_hypercube()
    
    def set(self, val):
        """Function to a vector to a value"""
        self.get_nd_array().fill(val)
        return self

    def __getitem__(self, key):
        """Easy access of underlyind nd-array"""
        return self.arr[key]

    def __setitem__(self, key, val):
        """Easy access of nd-array"""
        self.arr[key] = val

    def get_nd_array(self) -> np.ndarray:
        """Return a numpy version of the array (same memory"""
        return self.arr

    def scale_add(self, vec2, sc1=1.0, sc2=1.0):
        """self = self * sc1 + sc2 * vec2"""
        self.scaleAdd(vec2, sc1, sc2)
        return self

    def window(self, compress=False, **kw):
        """Window a vector return another vector (of the same dimension
        specify min1..min6, max1...max6, f1...f6, j1...j6, n1...n6, or
        any of these by lists.
        Can not specify n and min or max
        compress=False Whether or not to compress the axes of length 1

        """
        axes = self.get_hyper().axes
        n_wind, f_wind, j_wind = fix_window(axes, **kw)

        n_s = n_wind
        f_s = f_wind
        j_s = j_wind

        axis_out = []
        axis_c_out = []
        nout = []
        for i in range(len(n_wind)):
            axis_out.append(
                Axis(
                    n=n_wind[i],
                    o=axes[i].o + axes[i].d * f_wind[i],
                    d=j_wind[i] * axes[i].d,
                    label=axes[i].label,
                    unit=axes[i].unit,
                )
            )
            if n_wind[i] != 1 and compress:
                axis_c_out.append(axis_out[:-1])
                nout.append(n_wind[i])

        vec = get_sep_vector(axes=axis_out)
        # out_array=vec.get_nd_array()
        in_array = self.get_nd_array()
        ndim = len(axis_out)
        if ndim == 1:
            out_array = in_array[f_s[0] : j_s[0] * n_s[0] : j_s[0]]
        elif ndim == 2:
            out_array = in_array[
                f_s[1] : j_s[1] * n_s[1] : j_s[1], f_s[0] : j_s[0] * n_s[0] : j_s[0]
            ]
        elif ndim == 3:
            out_array = in_array[
                f_s[2] : j_s[2] * n_s[2] : j_s[2],
                f_s[1] : j_s[1] * n_s[1] : j_s[1],
                f_s[0] : j_s[0] * n_s[0] : j_s[0],
            ]
        elif ndim == 4:
            out_array = in_array[
                f_s[3] : j_s[3] * n_s[3] : j_s[3],
                f_s[2] : j_s[2] * n_s[2] : j_s[2],
                f_s[1] : j_s[1] * n_s[1] : j_s[1],
                f_s[0] : j_s[0] * n_s[0] : j_s[0],
            ]
        elif ndim == 5:
            out_array = in_array[
                f_s[4] : j_s[4] * n_s[4] : j_s[4],
                f_s[3] : j_s[3] * n_s[3] : j_s[3],
                f_s[2] : j_s[2] * n_s[2] : j_s[2],
                f_s[1] : j_s[1] * n_s[1] : j_s[1],
                f_s[0] : j_s[0] * n_s[0] : j_s[0],
            ]
        elif ndim == 6:
            out_array = in_array[
                f_s[5] : j_s[5] * n_s[5] : j_s[5],
                f_s[4] : j_s[4] * n_s[4] : j_s[4],
                f_s[3] : j_s[3] * n_s[3] : j_s[3],
                f_s[2] : j_s[2] * n_s[2] : j_s[2],
                f_s[1] : j_s[1] * n_s[1] : j_s[1],
                f_s[0] : j_s[0] * n_s[0] : j_s[0],
            ]
        elif ndim == 7:
            out_array = in_array[
                f_s[6] : f_s[6] * n_s[6] : j_s[6],
                f_s[5] : j_s[5] * n_s[5] : j_s[5],
                f_s[4] : j_s[4] * n_s[4] : j_s[4],
                f_s[3] : j_s[3] * n_s[3] : j_s[3],
                f_s[2] : j_s[2] * n_s[2] : j_s[2],
                f_s[1] : j_s[1] * n_s[1] : j_s[1],
                f_s[0] : j_s[0] * n_s[0] : j_s[0],
            ]
        if not compress:
            return vec
        vec_out = get_sep_vector(axes=axis_c_out)
        vec_out.get_nd_array = out_array

    def get_1d_array(self) -> np.ndarray:
        """Get 1-D representation of array"""
        return np.ravel(self.arr)

    def adjust_hyper(self, hyper: Hypercube):
        """
        Adjust the Hypercube associated with vector. Does not reallocate.
        Must be same dims/size
        """
        hyper_old = self.get_hyper()
        if hyper_old.getN123() != hyper.getN123():
            self._logger.fatal("Trying to reset with a different sized hyper")
            raise Exception("")
        new_shape = tuple(np.flip(tuple(hyper.get_ns())))
        self.arr = np.reshape(self.get_nd_array(), new_shape)
        self._hyper = hyper

    def check_same(self, vec2) -> bool:
        """Function to check if two vectors belong to the same vector space"""
        return self.checkSame(vec2)


class NonInteger(Vector):
    """A class for non-integers"""

    def __init__(self, hyper: Hypercube, data_type: str, vals=None, space_only=False):
        """Initialize a non-integer"""
        super().__init__(hyper, data_type, vals=vals, space_only=space_only)


class RealNumber(NonInteger):
    """A class for real numbers"""

    def __init__(self, hyper: Hypercube, data_type: str, vals=None, space_only=False):
        """Initialize a real number vector"""
        super().__init__(hyper, data_type, vals=vals, space_only=space_only)

    def clip(self, bclip, eclip):
        """Clip dataset
        bclip - Minimum value to clip to
        eclip - Maximum value to clip to"""
        np.clip(self.get_nd_array(), bclip, eclip)

    def cent(self, pct: float, jsamp=1):
        """Calculate the percentile of a dataset
        pct - Percentile of the dataset
        jsamp - Sub-sampling of dartaset"""
        if jsamp:
            pass
        return np.percentile(self.arr, pct)

    def calc_histo(self, nelem, min_val, max_val):
        """Calculate histogram
        min_val - Minimum for cell
        max_val - Maximum for cell
        nelem - Return number of elements in histogram

        @return Histogram"""
        # ar=self.get_1d_array()
        # CHANGE
        histo = get_sep_vector(ns=[nelem], data_type="int32")
        calc_histo(histo.get_nd_array(), self.get_nd_array(), min_val, max_val)
        return histo


class FloatVector(Vector):
    """Generic float vector class"""

    def __init__(self, hyper: Hypercube, vals=None, space_only=False):
        super().__init__(hyper, "float32", vals=vals, space_only=space_only)

    def __repr__(self):
        """Override print method"""
        return f"FloatVector\n{str(self.get_hyper())}"

    def clone(self):
        """Function to clone (deep copy) a vector"""
        vec= FloatVector(self.get_hyper(), vals=self.get_nd_array())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def cloneSpace(self):
        vec= self.clone_space()
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def clone_space(self):
        """Funtion tor return the space of a vector"""
        vec= FloatVector(self.get_hyper(), space_only=True)
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def checkSame(self, other):
        """Function to check that two vectors are from same space"""
        if not isinstance(other, FloatVector):
            return False
        return self.get_hyper().check_same(other.get_hyper())


class DoubleVector(Vector):
    """Generic double vector class"""

    def __init__(self, hyper: Hypercube, vals=None, space_only=False):
        super().__init__(hyper, "float64", vals=vals, space_only=space_only)

    def __repr__(self):
        """Override print method"""
        return f"Double_Vector\n{str(self.get_hyper())}"

    def clone(self):
        """Function to clone (deep copy) a vector"""
        vec= DoubleVector(self.get_hyper(), vals=self.get_nd_array())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def clone_space(self):
        """Funtion tor return the space of a vector"""
        vec=DoubleVector(self.get_hyper(), space_only=True)
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def checkSame(self, other):
        """Function to check that two vectors are from same space"""
        if not isinstance(other, DoubleVector):
            return False
        vec= self.get_hyper().check_same(other.get_hyper())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def cloneSpace(self):
        vec= self.clone_space()
        vec._transition=self._transition
        vec._norm=self._norm
        return vec

class IntVector(Vector):
    """Generic int vector class"""

    def __init__(self, hyper: Hypercube, vals=None, space_only=False):
        super().__init__(hyper, "int32", vals=vals, space_only=space_only)

    def __repr__(self):
        """Override print method"""
        return f"IntVector\n{str(self.get_hyper())}"

    def clone(self):
        """Function to clone (deep copy) a vector"""
        vec= IntVector(self.get_hyper(), vals=self.get_nd_array())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def checkSame(self, other):
        """Function to check that two vectors are from same space"""
        if not isinstance(other, IntVector):
            return False
        vec= self.get_hyper().check_same(other.get_hyper())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec

class ComplexVector(Vector):
    """Generic complex vector class"""

    def __init__(self, hyper: Hypercube, vals=None, space_only=False):
        super().__init__(hyper, "complex64", vals=vals, space_only=space_only)

    def clone_space(self):
        """Funtion tor return the space of a vector"""
        vec= ComplexVector(self.get_hyper(), space_only=True)
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def cloneSpace(self):
        vec= self.clone_space()
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def __repr__(self):
        """Override print method"""
        return f"ComplexVector\n{str(self.get_hyper())}"

    def clone(self):
        """clone a vector"""
        vec= ComplexVector(self.get_hyper(), vals=self.get_nd_array())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def checkSame(self, other):
        """Function to check that two vectors are from same space"""
        if not isinstance(other, ComplexVector):
            return False
        return self.get_hyper().check_same(other.get_hyper())


class ComplexDoubleVector(Vector):
    """Generic complex vector class"""

    def __init__(self, hyper: Hypercube, vals=None, space_only=False):
        super().__init__(hyper, "complex128", vals=vals, space_only=space_only)

    def clone_space(self):
        """Funtion tor return the space of a vector"""
        vec= ComplexDoubleVector(self.get_hyper(), space_only=True)
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def cloneSpace(self):
        vec= self.clone_space()
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def __repr__(self):
        """Override print method"""
        return f"ComplexDoubleVector\n{str(self.get_hyper())}"

    def clone(self):
        """clone a vector"""
        vec= ComplexDoubleVector(self.get_hyper(), vals=self.get_nd_array())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def checkSame(self, other):
        """Function to check that two vectors are from same space"""
        if not isinstance(other, ComplexDoubleVector):
            return False
        return self.get_hyper().check_same(other.get_hyper())


class ByteVector(Vector):
    """Generic byte vector class"""

    def __init__(self, hyper: Hypercube, vals=None, space_only=False):
        super().__init__(hyper, "uint8", vals=vals, space_only=space_only)

    def calc_histo(self, nelem, min_val, max_val):
        """Calculate histogram
        min_val - Minimum for cell
        max_val - Maximum for cell
        nelem - Return number of elements in histogram

        @return Histogram"""
        histo = get_sep_vector(ns=[nelem], data_type="int32")
        calc_histo(histo, self.get_nd_array(), min_val, max_val)
        return histo

    def clone(self):
        """Function to clone (deep copy) a vector"""
        vec= ByteVector(self.get_hyper(), vals=self.get_nd_array())
        vec._transition=self._transition
        vec._norm=self._norm
        return vec
    
    def __repr__(self):
        """Override print method"""
        return f"ByteVector\n{str(self.get_hyper())}"

    def checkSame(self, other):
        """Function to check that two vectors are from same space"""
        if not isinstance(other, ByteVector):
            return False
        return self.get_hyper().check_same(other.get_hyper())


def get_sep_vector(
    *args,
    **keys,
):
    """Get a sepVector object
    Option 1 (supply Hypercube):
            hyper, kw args
    Option 2 (build Hypercube):
            ns = [] - list of sizes
            os = [] - list of origins
            ds = [] - list os sampling
            labels = [] list of labels
            axes = [] list of axes

    data_type = float32[default], float64,double64,
                    int32,complex64,complex128)

    Option 4 (numpy)
        Provide hyper, ns, os, or ds,label,s axes

    Optional:
        logger - Provide a logger for errors
    """
    myt = "float32"
    # have_hyper=False
    have_numpy = False
    array = None

    if "logger" in keys:
        logger = keys["loggger"]
    else:
        logger = logging.getLogger(None)
    if len(args) == 1:
        if isinstance(args[0], Hypercube):
            hyper = args[0]
        elif isinstance(args[0], np.ndarray):
            have_numpy = True
            array = args[0]
            if "hyper" in keys:
                hyper = keys["hyper"]
            elif "axes" in keys or "ns" in keys:
                if "axes" in keys:
                    hyper = Hypercube(keys["axes"])
                else:
                    ns = keys["ns"]
                    del keys["ns"]
                    hyper = Hypercube.set_with_ns(ns, **keys)
            else:
                nt = list(array.shape)
                ns = []
                for i in range(len(nt)):
                    ns.append(nt[len(nt) - 1 - i])
                hyper = Hypercube.set_with_ns(ns)
            ns = hyper.get_ns()
            ns.reverse()
            if ns != list(array.shape):
                raise Exception(
                    f"hypercube shape (reversed) {ns}"
                    + f"and array shape {array.shape} don't agree"
                )
        else:
            logger.fatal("First argument must by a Hypercube or numpy array")
            raise Exception("")
    elif len(args) == 0:
        if "axes" in keys or "ns" in keys:
            if "axes" in keys:
                hyper = Hypercube(keys["axes"])
            else:
                ns = keys["ns"]
                del keys["ns"]
                hyper = Hypercube.set_with_ns(ns, **keys)

        else:
            logger.fatal("Must supply Hypercube,vector  or ns/axes")
            raise Exception("")
    else:
        logger.fatal("Only understand 0 or 1" + " (Hypercube) non-keyword arguments")
        raise Exception("")

    if have_numpy:
        if not converter.valid_type(str(array.dtype)):
            logger.fatal("Numpy array type not supported %s", array.dtype)
            raise Exception("")
        myt = converter.get_name(str(array.dtype))
    else:
        myt = "float32"
        if "data_type" in keys:
            myt = converter.get_name(keys["data_type"])

    if myt == "float32":
        out_type = FloatVector(hyper)
    elif myt == "complex128":
        out_type = ComplexDoubleVector(hyper)
    elif myt == "complex64":
        out_type = ComplexVector(hyper)
    elif myt == "float64":
        out_type = DoubleVector(hyper)
    elif myt == "int32":
        out_type = IntVector(hyper)
    elif myt == "uint8":
        out_type = ByteVector(hyper)
    else:
        logger.fatal("Unknown type %s", myt)
        raise Exception("")
    if have_numpy:
        np.copyto(out_type.get_nd_array(), array)
    return out_type


def rea_col_textfile(file):
    """Create an array from a texf file

    file- file name
    """
    f = open(file, "r", encoding="utf-8")
    lines = f.readlines()
    array = []
    for line in lines:
        parts = line.split()
        vec1 = []
        for x in parts:
            try:
                y = float(x)
                vec1.append(y)
            except ValueError:
                y = x
        array.append(vec1)
    array2 = []
    for i in range(len(array[0])):
        lst = []
        for j in range(len(array)):
            lst.append(array[j][i])
        array2.append(lst)
    return array2


def fix_window(axes, **kw):
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
            if isinstance(kw["n"], list):
                if len(kw["n"]) >= i:
                    kw[f"n{i}"] = kw["n"][i - 1]
        if "f" in kw:
            if isinstance(kw["f"], list):
                if len(kw["f"]) >= i:
                    kw[f"f{i}"] = kw["f"][i - 1]
        if "j" in kw:
            if isinstance(kw["j"], list):
                if len(kw["j"]) >= i:
                    kw[f"j{i}"] = kw["j"][i - 1]
        if f"n{i}" in kw:
            nset = True
            n = int(kw[f"n{i}"])
        if f"f{i}" in kw:
            fset = True
            f = int(kw[f"f{i}"])
        if f"j{i}" in kw:
            jset = True
            j = int(kw[f"j{i}"])
        bi_set = False
        ei_set = False
        if f"min{i}" in kw:
            bi = int(float(kw[f"min{i}"] - axes[i - 1].o) / axes[i - 1].d + 0.5)
            bi_set = True
        if "max{i}" in kw:
            ei = int(float(kw[f"max{i}"] - axes[i - 1].o) / axes[i - 1].d + 0.5)
            ei_set = True
        if fset:
            if axes[i - 1].n <= f:
                logging.getLogger().fatal(
                    f"invalid f{i}={f} parameter n%d of data={axes[i-1].n}"
                )
                raise Exception("")
        if nset:
            if axes[i - 1].n < n:
                logging.getLogger().fatal(
                    f"invalid n{i}={n} parameter n%d of data={axes[i-1].n}"
                )
                raise Exception("")
        if jset and j <= 0:
            logging.getLogger().fatal("invalid j%d,%d", i, j)
            raise Exception("")
        if not jset:
            j = 1
        if not nset:
            if not fset:
                if not bi_set:
                    f = 0
                elif bi < 0 or bi >= axes[i - 1].n:
                    logging.getLogger().fatal("Invalid min%d=%d", i, kw[f"min{i}"])
                    raise Exception("")
                else:
                    f = bi
            if ei_set:
                if ei <= f or ei >= axes[i - 1].n:
                    logging.getLogger().fatal("Invalid max%d=%d", i, kw[f"min{i}"])

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
                logging.getLogger().fatal("Invalid min%d=%d", i, kw[f"min{i}"])
                raise Exception("")
            else:
                f = int(bi)
        if axes[i - 1].n < (1 + f + j * (n - 1)):
            logging.getLogger().fatal("Invalid window parameter")
            raise Exception("")
        n_wind.append(int(n))
        f_wind.append(int(f))
        j_wind.append(int(j))
    return n_wind, f_wind, j_wind
