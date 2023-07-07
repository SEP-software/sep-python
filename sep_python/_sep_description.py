import os
import copy
import io
import sys
import re
import numpy as np
import sep_python._sep_converter
from sep_python._base_helper import base_class_doc_string
from sep_python._hypercube import Hypercube, Axis
from sep_python._sep_helpers import database_from_str, get_datafile
from sep_python._description import description_base

converter = sep_python._sep_converter.converter
hyper_des = re.compile(r"(n|o|d|label|unit)\d")


class sep_description_base(description_base):
    """Base class for writing descriptions SEP style"""

    def __init__(self, path, intent, **kw):
        """Initialie a description using metadata

        path - Path to object (could be a file descriptor)
        intent - Intent of object

        Option 1 (Input):
            database - Provide database

        Option 2 (Output):
             array (np.ndarray) - Array or memReg storage
                or
                vec ()
        Option 3 (Output):
            hyper - Hypercube describing data
            os,ds,labels,units - Lists describing array

        Optional:
                ndims - Minimum dimensions for the data
                ignore_1 [True] Ignore axes that are 1 in length

        """
        super().__init__()
        self._path = path
        self._data_type = "float32"
        self._esize = 4
        self._xdr = False
        self._binary_path = None
        self._binary_path_func = get_datafile

        if intent == "OUTPUT":
            if "array" in kw or "vec" in kw:
                if "array" in kw:
                    array = kw["array"]
                    if "hyper" in kw:
                        self.set_hyper(copy.deepcopy(kw["hyper"]))
                    else:
                        n_s = list(array.shape)[::-1]
                        o_s = [0.0] * len(n_s)
                        d_s = [1.0] * len(n_s)
                        labels = [""] * len(n_s)
                        units = [""] * len(n_s)
                        if "os" in kw:
                            o_s = kw["os"]
                        if "ds" in kw:
                            d_s = kw["ds"]
                        if "labels" in kw:
                            labels = kw["labels"]
                        if "units" in kw:
                            units = kw["units"]
                        if "hyper" in kw:
                            if str(n_s) != str(kw["hyper"].getNs()):
                                raise Exception("")
                        else:
                            self.set_hyper(
                                Hypercube.set_with_ns(
                                    n_s, os=o_s, ds=d_s, labels=labels, units=units
                                )
                            )
                elif "vec" in kw:
                    array = kw["vec"].get_nd_array()
                    self.set_hyper(kw["vec"].get_hyper())
                self.set_data_type(str(array.dtype))
                self._esize = converter.get_esize(str(array.dtype))
                self._params = self.build_params_from_hyper(self._hyper)

            elif "hyper" in kw:
                self._params = self.build_params_from_hyper(kw["hyper"])
                if "data_type" not in kw:
                    raise Exception(
                        "Musty specify type" + " when creating from hypercube"
                    )
                self.set_data_type(converter.get_numpy(kw["data_type"]))
        else:
            if "dictionary" not in kw:
                raise Exception("Muat provide dictionary when from input")
            self.set_dictionary(self.build_params_from_dict(kw["dictionary"], **kw))

    def build_params_from_hyper(self, hyper: Hypercube):
        """Build parameters from hypercube"""
        pars = {}
        self._history = ""

        for i, axis in enumerate(hyper.axes):
            pars[f"n{i+1}"] = axis.n
            pars[f"o{i+1}"] = axis.o
            pars[f"d{i+1}"] = axis.d
            pars[f"label{i+1}"] = axis.label
            pars[f"unit{i+1}"] = axis.unit
        self._hyper = copy.deepcopy(hyper)

        return pars

    def get_history_dict(self, path):
        """Build parameters from Path"""
        try:
            file_pointer = open(path, "rb")
        except IOError:
            self._logger.fatal("Trouble opening %s", path)
            raise Exception("")

        mystr = file_pointer.read(1024 * 1024)
        file_pointer.close()
        end_of_file_marker = mystr.find(4)
        file_pointer = open(path, "r")
        if end_of_file_marker == -1:
            self._head = 0
            # self._history=str(mystr)
            self._history = file_pointer.read()
        else:
            self._head = end_of_file_marker + 1
            input_buffer = io.BytesIO(mystr[: self._head])
            if input_buffer:
                pass
            wrapper = io.TextIOWrapper(input_buffer, encoding="utf-8")

            self._history = wrapper.read()
        file_pointer.close()
        # self._history=self._history.replace("\\n","\n").replace("\\t","\t")

        pars = {}
        pars = database_from_str(self._history, pars)
        return pars

    def build_params_from_dict(self, pars: dict, **kw):
        """Build parameters from Path"""

        ndim = 1
        found = False
        axes = []
        skip_1 = True
        if "ignore_1" in kw:
            skip_1 = kw["ignore_1"]
        while not found:
            if f"n{ndim}" not in pars:
                found = True
            else:
                nsamp = int(pars[f"n{ndim}"])
            if f"o{ndim}" in pars:
                origin = pars[f"o{ndim}"]
            else:
                origin = 0
            if f"d{ndim}" in pars:
                dsamp = pars[f"d{ndim}"]
            else:
                dsamp = 1
            if f"label{ndim}" in pars:
                label = pars[f"label{ndim}"]
            else:
                label = ""
            if f"unit{ndim}" in pars:
                unit = pars[f"unit{ndim}"]
            else:
                unit = ""
            ndim += 1
            if not found:
                axes.append(Axis(n=nsamp, o=origin, d=dsamp, label=label, unit=unit))
            if "ndims" in kw:
                if kw["ndims"] > len(axes):
                    axes.append(n=1)
        if skip_1:
            ngreater1 = len(axes)
            for i in range(len(axes) - 1, 1, -1):
                if axes[i].n == 1:
                    ngreater1 = i
            axes = axes[:ngreater1]
        self._hyper = Hypercube(axes=axes)

        if "in" in pars:
            self.set_binary_path(pars["in"])

        if "data_format" in pars:
            if pars["data_format"][:3] == "xdr":
                self._xdr = True
                pars["data_format"] = "native" + pars["data_format"][3:]
            self.set_data_type(converter.sep_name_to_numpy(pars["data_format"]))
            self._esize = converter.get_esize(
                converter.from_SEP_name(pars["data_format"])
            )
            if "esize" in pars:
                if int(pars["esize"]) == 8:
                    if (
                        pars["data_format"] == "native_float"
                        or pars["data_format"] == "xdr_float"
                    ):
                        self._esize = 8
                        self.set_data_type("complex64")
        elif "esize" in pars:
            self._esize = int(pars["esize"])
            if self._esize == 1:
                self.set_data_type(np.uint8)
            elif self._esize == 8:
                self.set_data_type(np.complex64)
            elif self._esize == 4:
                self.set_data_type(np.float32)
        else:
            self._esize = 4
            self.set_data_type(np.float32)

        return pars

    def read_from_file_descriptor(file_pointer):
        """Read from file_pointer"""
        mystr = file_pointer.read(1024 * 1024 * 1024)

        end_of_file_marker = mystr.find(4)

        if end_of_file_marker != -1:
            if len(mystr) >= 1024 * 1024 * 1024 - 1:
                raise Exception("Can not deal with description file >= 1GB")

        if end_of_file_marker == -1:
            input_buffer = io.BytesIO(mystr)
            wrapper = io.TextIOWrapper(input_buffer, encoding="utf-8")
            data = None
        else:
            head = end_of_file_marker + 1
            data = mystr[end_of_file_marker + 1 :]

            input_buffer = io.BytesIO(mystr[:head])
            wrapper = io.TextIOWrapper(input_buffer, encoding="utf-8")

        description = wrapper.read()

        pars = {}
        pars = database_from_str(description, pars)

        return (True, pars, data, file_pointer, end_of_file_marker != -1)

    def get_binary_path(self):
        """Get binary path"""
        if self._binary_path is None:
            raise Exception("Binary path has not been set")
        return self._binary_path

    def set_binary_path(self, path):
        """
        Set the binary path
        """
        if isinstance(path, list):
            self._binary_path = ";".join(path)
        else:
            self._binary_path = path

    def set_data_path(self, pth):
        if isinstance(pth, list):
            self._params["in"] = ":".join(pth)
        else:
            self._params["in"] = pth

    def get_xdr(self):
        """Return whether the data is xdr"""
        return self._xdr

    def from_string(self, str):
        """Create the dictionary from a stringk

        str - String to read dictionary from

        """
        pars = {}
        pars = database_from_str(str, pars)

    def to_string(self):
        """Write description to_string"""
        mystr = ""
        for par in self._params:
            if not hyper_des.search(par):
                mystr += f"{par}={self._params[par]}\n"
        mystr += self.hyper_to_str() + "\n"
        mystr += f"in={self.get_binary_path()}\n"
        mystr += f"esize={self._esize}\n"
        mystr += f"data_format={converter.get_SEP_name(self.get_data_type())}"
        mystr += "\n"
        return mystr

    def to_dictionary(self):
        """Write description to_dictionary

        Mainly used for unit tests

        """

        mydict = {}
        for par, val in self._params.items():
            mydict[par] = val
        mydict = self.hyper_to_dict(mydict)
        mydict["in"] = self.get_binary_path()
        mydict["esize"] = self._esize
        mydict["data_format"] = converter.get_SEP_name(self.get_data_type())
        return mydict

    def close(self):
        if not self._written:
            self.write_description()
            self._written = True


class sep_description_file(sep_description_base):
    """SEP description file stored in a file"""

    @base_class_doc_string(sep_description_base.__init__)
    def __init__(self, path, intent, **kw):
        """
        Descriptor in file

        path - Path to file
        intent - Intent for file

        Optional args (kw)
        """
        if intent == "INPUT":
            if "dictionary" not in kw:
                raise Exception("Expecing dictionary in initialization")
            if "in" not in kw["dictionary"]:
                raise Exception("in must be set in dictionary")
        super().__init__(path, intent, **kw)

    def check_valid(path):
        """
        Check to see if valid for gcs
        """

        if len(path) >= 5:
            if path == "gs://":
                raise Exception("Can not specify gcs path")

    def read_description(path):
        if path == "<":
            file_pointer = sys.stdin.buffer
        else:
            try:
                file_pointer = open(path, "rb")
            except IOError:
                return False, None, None, None, None
        return sep_description_base.read_from_file_descriptor(file_pointer)

    def write_description(self):
        if self._path == ">":
            fl = sys.stdout.buffer
        else:
            try:
                fl = open(self._path, "w")
            except IOError:
                raise Exception("Trouble opening path %s", self._path)
        fl.write(self.to_string().encode("utf-8"))
        if self.get_binary_path() == "stdin":
            fl.write(bytes([4]))

    def remove(self):
        """Remove the given file"""
        if os.path.isfile(self._path):
            os.remove(self._path)


class inmem_description(sep_description_base):
    @base_class_doc_string(sep_description_base.__init__)
    def __init__(self, path, intent, **kw):
        """Initialize from memory"""
        if not isinstance(path, io.BytesIO):
            raise Exception("Expecting path to be a file")
        super().__init__(path, intent, **kw)

    def read_description(path):
        """Read description"""
        if not isinstance(path, io.BytesIO):
            raise Exception("Expecting path to be a file")
        return sep_description_base.read_from_file_descriptor(path)

    def check_valid(path):
        """Read description"""
        if not isinstance(path, io.BytesIO):
            raise Exception("Expecting path to be a file")
        return True

    def write_description(self):
        self._path.write(self.to_string().encode("utf-8"))
        self._path.write(bytes([4]))

    def close(self):
        """Close inmem (nothing to do)"""
        pass

    def remove(self):
        """Remove file"""
        pass
