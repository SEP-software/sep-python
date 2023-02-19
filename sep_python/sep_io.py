"""Module for reading/writing SEP formated files"""
import re
import os
import copy
import time
import logging
import io
from abc import abstractmethod
from concurrent import futures
import numpy as np
from google.cloud import storage
from sep_python.hypercube import Hypercube, Axis
import sep_python.io_base
import sep_python.sep_converter
import sep_python.sep_proto
import sep_python.gcp_helper


__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version__ = "2022.12.13"


def datapath(host=None, all_paths=None):
    """Return the datapath

    If host is not specified  defaults to the local machine
    If all_paths is specifed returns the list of a ; list of directories

    """

    if host is None:
        hst = os.uname()[1]
    else:
        hst = host

    path = os.environ.get("DATAPATH")
    if not path:
        try:
            file = open(".datapath", "r")
        except:
            try:
                file = open(os.path.join(os.environ.get("HOME"), ".datapath"), "r")
            except:
                file = None
    if file:
        for line in file.readlines():
            check = re.match(r"(?:%s\s+)?datapath=(\S+)" % hst, line)
            if check:
                path = check.group(1)
            else:
                check = re.match(r"datapath=(\S+)", line)
                if check:
                    path = check.group(1)
        file.close()
    if not path:
        path = "/tmp/"
    if all_paths:
        return path.split(":")
    return path


def get_datafile(name, host=None, all_files=None, nfiles=1):
    """Returns the datafile name(s) using SEP datafile conventions

    if host is not specified defaults to local machine
    if all_files is specified and datapath is a ; seperated
        list returns list of paths
    if nfiles is specified returns multi-file names

    """

    filepaths = datapath(host, all_files)
    if all_files:
        files_out = []
        for i in range(nfiles):
            for directory in filepaths:
                if i == 0:
                    end = "@"
                else:
                    end = "@" + str(i)
                files_out.append(directory + os.path.basename(name) + end)
        return files_out
    else:
        return filepaths + os.path.basename(name) + "@"
    return filepaths


class InOut(sep_python.io_base.InOut):
    """Class for doing IO of SEPlib regular cubes"""

    def __init__(self, create_mem, **kw):
        """
        SEPlib IO

        create_mem - Function to create memory

        Optional:

          logger - Logger to use for IO

        """
        super().__init__(create_mem)

        if "logger" in kw:
            self.set_logging(kw["logger"])

    def get_reg_storage(self, **kw):
        """
        Return a regular sampled object pointer
        """
        if "path" not in kw:
            self._logger.fatal("path must be specified")
            raise Exception("")

        path = kw["path"]
        if "//" not in path:
            stor = SEPFile(**kw)
        elif path[:5] == "gs://":
            stor = SEPGcsObj(**kw)
        self.add_storage(path, stor)
        return stor


converter = sep_python.sep_converter.converter


def database_from_str(string_in: str, data_base: dict):
    """Create a database from string

    string_in - The string we want to parse
    data_base - The output databse (this is a recursive funciton)

    return

    data_base - Output dtabase
    """
    lines = string_in.split("\n")
    parq1 = re.compile(r'([^\s]+)="(.+)"')
    parq2 = re.compile(r"(\S+)='(.+)'")
    par_string = re.compile(r"(\S+)=(\S+)")
    comma_string = re.compile(",")
    for line in lines:
        args = line.split()
        comment = 0
        for arg in args:
            if arg[0] == "#":
                comment = 1
            res = None
            if comment != 1:
                res = parq1.search(arg)
                if res:
                    pass
                else:
                    res = parq2.search(arg)
                    if res:
                        pass
                    else:
                        res = par_string.search(arg)
            if res:
                if res.group(1) == "par":
                    try:
                        file2_open = open(res.group(2), encoding="utf-8")
                    except:
                        logging.getLogger(None).fatal(
                            "Trouble opening %s", res.group(2)
                        )
                        raise Exception("")
                    database_from_str(file2_open.read(), data_base)
                    file2_open.close()
                else:
                    val = res.group(2)
                    if isinstance(val, str):
                        if comma_string.search(val):
                            val = val.split(",")
                    data_base[f"{str(res.group(1))}"] = val
    return data_base


def check_valid(param_dict: dict, args: dict):
    """Check to make sure keyword is of the correct type

    param_dict - dictionary of kw
    args - Dictionary of argument names and required types
    """
    for arg, typ in args.items():
        if arg in param_dict:
            if not isinstance(param_dict[arg], typ):
                logging.getLogger().fatal(
                    "Expecting  %sto be of type %s but is type %s", arg, typ, type(arg)
                )
                raise Exception("")


class RegFile(sep_python.io_base.RegFile):
    """A class to"""

    def __init__(self, **kw):

        check_valid(
            kw,
            {
                "hyper": Hypercube,
                "path": str,
                "vec": sep_python.sep_proto.MemReg,
                "array": np.ndarray,
                "os": list,
                "ds": list,
                "labels": list,
                "units": list,
                "logger": logging.Logger,
            },
        )

        super().__init__()

        self._xdr = False
        self._par_put = []
        self._first_write = True
        self._wrote_history = False
        self._head = 0
        self._esize = None
        self._data_out = False
        self._io_type = "SEP"
        self._intent = "OUTPUT"
        self._closed = False
        if "logger" in kw:
            self.set_logger(kw["logger"])
        else:
            self.set_logger(logging.getLogger(None))

        if "array" in kw or "vec" in kw:

            if "array" in kw:
                array = kw["array"]
                if "hyper" in kw:
                    self._hyper = copy.deepcopy(kw["hyper"])
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
                            self._logger.fatal(
                                "Shape of hypercube and array are different"
                            )
                            raise Exception("")
                    else:
                        self._hyper = Hypercube.set_with_ns(
                            n_s, os=o_s, ds=d_s, labels=labels, units=units
                        )
            elif "vec" in kw:
                array = kw["vec"].get_nd_array()
                self._hyper = kw["vec"].get_hyper()
            self.set_data_type(str(array.dtype))
            self._esize = converter.get_esize(str(array.dtype))
            self._params = self.build_params_from_hyper(self._hyper)

            if "path" not in kw:
                self._logger.fatal("Must specify path")
                raise Exception("")
            self._path = kw["path"]
            self.set_binary_path(get_datafile(self._path))

        elif "hyper" in kw:
            self._params = self.build_params_from_hyper(kw["hyper"])
            if "path" not in kw:
                self._logger.fatal("Must specify path in creation")
                raise Exception("")
            self._path = kw["path"]
            self.set_binary_path(get_datafile(self._path))
            if "type" not in kw:
                self._logger.fatal("Musty specify type when creating from hypercube")
                raise Exception("")
            self.set_data_type(converter.get_numpy(kw["type"]))
        elif "path" in kw:
            if not isinstance(kw["path"], str):
                self._logger.fatal("path must be a string")
                raise Exception("")
            self._params = self.build_params_from_path(kw["path"], **kw)
            self._path = kw["path"]
            self._intent = "INPUT"
        else:
            self._logger.fatal("Did not provide a valid way to create a dataset")
            raise Exception("")

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

    @abstractmethod
    def get_history_dict(self, path):
        """Given a path return the history file associated with it

        path to history description

        return dictionary

        """

    def write_description(self):
        """Write description file"""
        raise Exception("Must override")

    def build_params_from_path(self, fle: str, **kw):
        """Build parameters from Path"""

        pars = self.get_history_dict(fle)

        ndim = 1
        found = False
        axes = []
        skip_1 = True
        if "ignore_1" in kw:
            skip_1 = kw["ignore_1"]
        while not found:
            if not f"n{ndim}" in pars:
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

    def get_par(self, param: str, default=None):
        """Return parameter of any type
        param - Parameter to retrieve
        default - Default value
        """
        if param in self._params:
            return self._params[param]
        if default is not None:
            return default
        self._logger.fatal("Can't find %s", param)

    def get_int(self, param: str, default=None) -> int:
        """Return parameter of int
        param - Parameter to retrieve
        default - Default value
        """
        val = self.get_par(param, default)
        try:
            return int(val)
        except ValueError as v_error:
            self._logger.fatal("Can convert %s=%s to int", param, val)
            raise Exception("") from v_error

    def get_float(self, param: str, default=None) -> float:
        """Return parameter of type float
        param - Parameter to retrieve
        default - Default value
        """
        val = self.get_par(param, default)
        try:
            return float(val)
        except ValueError as v_error:
            self._logger(f"Can convert {param}={val} to float")
            raise Exception("") from v_error

    def get_string(self, param: str, default=None) -> str:
        """Return parameter of type string
        param - Parameter to retrieve
        default - Default value
        """
        return self.get_par(param, default)

    def get_ints(self, param: str, default=None) -> list:
        """Return parameter of type int arrau
        param - Parameter to retrieve
        default - Default value
        """
        val = self.get_par(param, default)
        vals = val.split(",")
        vout = []
        for value in vals:
            try:
                vout.append(int(value))
            except ValueError as v_error:
                self._logger.fatal("Can not convert %s=%s to ints", param, value)
                raise Exception("") from v_error

    def get_floats(self, param: str, default=None) -> float:
        """Return parameter of float arry
        param - Parameter to retrieve
        default - Default value
        """
        val = self.get_par(param, default)
        vals = val.split(",")
        vout = []
        for value in vals:
            try:
                vout.append(float(value))
            except ValueError as v_error:
                self._logger.fatal("Can not convert %s=%s to floats", param, value)
                raise Exception("") from v_error

    def put_par(self, param: str, val):
        """Store a parameter

        param - Parameter to store
        val  - Value"""

        if not isinstance(val, list):
            try:
                pout = str(val[0])
            except ValueError as v_error:
                self._logger.fatal("trouble converting %s to a string", val)
                raise Exception("") from v_error
            for value in val[1:]:
                try:
                    pout += "," + str(value)
                except ValueError as v_error:
                    self._logger.fatal("Trouble converting %s to a string", value)
                    raise Exception("") from v_error
        else:
            try:
                pout = str(val)
            except ValueError as v_error:
                self._logger.fatal("Trouble converting %s to a string", val)
                raise Exception("") from v_error
        self._par_put.append(param)
        self._params[param] = pout

    def get_prog_name(self) -> str:
        """
        Return get program name
        """
        return self._prog_name

    def close(self):
        """
        Delete function. Write descrption if data has been written
        """
        if not self._wrote_history and self._intent == "OUTPUT":
            self.write_description()


class SEPFile(RegFile):
    """Class when SEP data is stored in a file"""

    def __init__(self, **kw):

        """ "Initialize a sepFile object

        Optional:

          logger-logger object

         Method 1:
          hyper - Hypercube
          path  (str) - path for Path
          type (str) - SEP Data type


         Method 2:
          Path  - Path to file

          Optional:
           ndims - Minimum dimensions for the data
           ignore_1 [True] Ignore axes that are 1 in length

         Method 3:
           path (str) - path for history file
           array (np.ndarray) - Array or memReg storage
              or
           vec ()

           Optional:
              hyper - Hypercube describing data
              os,ds,labels,units - Lists describing array

        """
        if "logger" in kw:
            self.set_logger(kw["logger"])
        else:
            self.set_logger(logging.getLogger(None))
        if "path" not in kw:
            self._logger.fatal("Must specify path")
            raise Exception("")
        if "//" in kw["path"]:
            self._logger.fatal(
                "When creating a file object path must not have a web address %s",
                kw["path"],
            )
            raise Exception("")
        super().__init__(**kw)

        if self._intent == "INPUT":
            if "in" in self._params:
                self.set_binary_path = self._params["in"]

        if self.get_binary_path() is None:
            self._logger.fatal("Binary path is not")

        self._closed = False  # Whether the close funciton has been called

    def get_history_dict(self, path):
        """Build parameters from Path"""
        try:
            file_pointer = open(path, "rb")
        except:
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

    def read(self, mem, **kw):
        """
        read a block of data

        mem - Array to be read into

        Optional
        nw,fw,jw - Standard window parameters

        """
        if isinstance(mem, sep_python.sep_proto.MemReg):
            array = mem.get_nd_array()
        elif isinstance(mem, np.ndarray):
            array = mem
        else:
            self._logger.fatal("Do not how to read into type %s ", type(mem))
            raise Exception("")

        seeks, blk, many = self.loop_it(
            *self.condense(*self.get_hyper().get_window_params(**kw))
        )
        ar_use = array.ravel()
        if self.get_binary_path() == "stdin" or self.get_binary_path() == "follow_hdr":
            file_pointer = open(self._path, "rb")
        else:
            file_pointer = open(self.get_binary_path(), "rb")
        old = 0
        new = old + many
        for seek in seeks:
            file_pointer.seek(seek)
            bytes_array = file_pointer.read(blk)
            if len(bytes) != blk:
                self._logger.fatal(
                    "Only read  %d of %d starting at %d", len(bytes_array), blk, seek
                )
                raise Exception(
                    f"Only read  {len(bytes_array)} of {blk} starting at {seek}"
                )
            if self._xdr:
                if self._esize == 8:
                    tmp = np.frombuffer(bytes_array, dtype=np.float32)
                    tmo = tmp.byteswap()
                    tmo = np.frombuffer(tmo.tobytes(), ar_use.dtype)
                else:
                    tmp = np.frombuffer(bytes, dtype=ar_use.dtype)
                    tmo = tmp.byteswap()
                ar_use[old:new] = tmo.copy()
            else:
                ar_use[old:new] = np.frombuffer(bytes, dtype=ar_use.dtype).copy()
            old = new
            new = new + many
        file_pointer.close()

    def write(self, mem, **kw):
        """
        write a block of data

        mem - Array to be read into

        Optional
        nw,fw,jw - Standard window parameters

        """
        if isinstance(mem, sep_python.sep_proto.MemReg):
            array = mem.get_nd_array()
        elif isinstance(mem, np.ndarray):
            array = mem
        else:
            self._logger.fatal("Do not how to read into type %s", type(mem))
            raise Exception("")
        seeks, blk, many = self.loop_it(
            *self.condense(*self.get_hyper().get_window_params(**kw))
        )
        ar_use = array.ravel()
        self._data_out = True
        file_pointer = open(self.get_binary_path(), "wb+")
        old = 0
        new = old + many
        for seek in seeks:
            file_pointer.seek(seek)
            file_pointer.write(ar_use[old:new].tobytes())
            old = new
            new = new + many
        file_pointer.close()

    def write_description(self):
        """Write description to path"""

        file_pointer = open(self._path, "w")
        file_pointer.write(f"{self._history}\n{self.get_prog_name()}\n")
        for par in self._par_put:
            file_pointer.write(f"{par}={self._params[par]}")
        file_pointer.write("\n" + self.hyper_to_str())
        self.set_binary_path(datafile(self._path))
        file_pointer.write(f"in={self.get_binary_path()}\n")
        file_pointer.write(
            f"esize={self._esize} data_format={converter.get_SEP_name(self.get_data_type())}\n\n"
        )
        self._wrote_history = True

        file_pointer.close()

    def remove(self, error_if_not_exists=True):
        """Remove data

        errorIfNotExist- Return an error if file does not exist

        """
        if os.path.isfile(self._path):
            os.remove(self._path)
            if self._binary_path != "stdin":
                if os.path.isfile(self._binary_path):
                    os.remove(self._binary_path)
        elif error_if_not_exists:
            self._logger.fatal("Tried to remove file %s", self._path)
            raise Exception("")


class SEPGcsObj(RegFile):
    """Class when SEP data is stored in an object"""

    def __init__(self, **kw):
        """ "Initialize a sepFile object


        Optional

          logger - Logger for object

        Method 1:
        hyper - Hypercube
        path  (str) - path for Path
        type (str) - SEP Data type


        Method 2:
        Path  - Path to file

        Optional:
        ndims - Minimum dimensions for the data
        ignore_1 [True] Ignore axes that are 1 in length

        Method 3:
        path (str) - path for history file
        array (np.ndarray) - Array or memReg storage
            or
        vec ()

        Optional:
            hyper - Hypercube describing data
            os,ds,labels,units - Lists describing array

        """

        if "logger" in kw:
            self.set_logger(kw["logger"])
        else:
            self.set_logger(logging.getLogger(None))

        if "path" not in kw:
            self._logger.fatal("path must be specified when creating object")
            raise Exception("")

        gs_re = re.compile(r"gs://(\S+)\/(.+)")
        gs_exists = gs_re.search(kw["path"])

        self._blobs = []
        if gs_exists:
            self._bucket = gs_exists.group(1)
            self._object = gs_exists.group(2)
        else:
            self._logger.fatal("Invalid path for google storage object %s", kw["path"])
            raise Exception("")
        super().__init__(**kw)

    def get_history_dict(self, path):
        client = storage.Client()
        bucket = client.bucket(self._bucket)
        if not bucket.exists():
            self._logger.fatal("bucket %s does not exist", self._bucket)
            raise Exception("")

        blob = bucket.get_blob(self._object)
        if not blob.exists():
            self._logger.fatal(
                "blob %s does not exist in bucket %s", self._object, self._bucket
            )
            raise Exception("")

        pars = blob.metadata
        new_str = ""
        for key, val in pars.items():
            new_str += f"{key}={val}"
        if "history" in pars:
            pars = database_from_str(pars["history"], pars)
            self._history = pars["history"]
            if "progName" in pars:
                self._history += f"\n{pars['progName']}\n"
                del pars["history"]
        self._history += f"\n{new_str}"
        return pars

    def write_description(self):
        """Write description to path"""
        pass

    def write_description_final(self):
        """
        Write description when closing file

        blob - Blob to set metadata (history)

        """
        tmp = copy.deepcopy(self._params)
        tmp["history"] = self._history
        tmp["progName"] = self.get_prog_name()
        tmp["esize"] = self._esize
        tmp["data_format"] = converter.get_SEP_name(self.get_data_type())

        self.hyper_to_dict(tmp)
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.blob(self._object)
        blob.metadata = tmp
        blob.patch()

    def close(self):
        """Close (and) pottentially combine GCS objects"""

        if self._closed:
            self._logger.info("Closed called multiple times %s", self._object)

        elif self._intent == "OUTPUT":
            self._closed = True
            found = False
            sleep = 0.2
            itry = 0
            while not found and itry < 5:
                try:
                    storage_client = storage.Client()
                    bucket = storage_client.bucket(self._bucket)
                    if len(self._blobs) == 0:
                        blob = bucket.blob(self._object)
                        blob.upload_from_string(
                            "",
                            content_type="application/x-www-form-urlencoded;charset=UTF-8",
                        )
                    elif len(self._blobs) == 1:
                        self._logger.info(
                            "Renaming %s to %s", self._blobs[0].name, self._object
                        )
                        bucket.rename_blob(self._blobs[0], self._object)
                    else:
                        with futures.ThreadPoolExecutor(max_workers=60) as executor:
                            sep_python.gcp_helper.compose(
                                f"gs://{self._bucket}/{self._object}",
                                self._blobs,
                                storage_client,
                                executor,
                                self._logger,
                            )
                    self.write_description_final()
                    found = True
                    # for a in self._blobs:
                    #   a.delete()
                except:
                    itry += 1
                    time.sleep(sleep)
                    sleep = sleep * 2
                    if itry == 5:
                        self._logger.fatal("Trouble obtaining client")
                        raise Exception("trouble obtaining client")

    def __del__(self):
        """Delete object"""
        if not self._closed and self._intent == "OUTPUT":
            self._logger.fatal("Must close gcs object before the delete is called")
            raise Exception("Must close gcs object before the delete is called")

    def remove(self, error_if_not_exists: bool = True):
        """Remove data

        errorIfNotExists - Return an error if blob does not exist

        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.get_blob(self._object)
        if blob.exists():
            blob.delete()
        elif error_if_not_exists:
            self._logger.fatal(
                "Attempted to remove blob=%s which does not exist", self._object
            )
            raise Exception("")

    def read(self, mem, **kw):
        """
        read a block of data

        mem - Array to be read into

        Optional
        nw,fw,jw - Standard window parameters

        """

        if isinstance(mem, sep_python.sep_proto.MemReg):
            array = mem.getNdArray()
        elif isinstance(mem, np.ndarray):
            array = mem
        else:
            self._logger.fatal("Do not how to read into type %s", type(mem))
            raise Exception("")
        seeks, blk, many = self.loop_it(
            *self.condense(*self.get_hyper().get_window_params(**kw))
        )
        ar_use = array.ravel()

        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.get_blob(self._object)

        with blob.open("rb") as file_pointer:
            old = 0
            new = old + many
            for seek in seeks:
                file_pointer.seek(seek + self._head)
                byte_array = file_pointer.read(blk)
                if self._xdr:
                    byte_array = bytearray(bytes).reverse()
                ar_use[old:new] = np.frombuffer(byte_array, dtype=ar_use.dtype).copy()
                old = new
                new = new + many

    def write(self, mem, **kw):
        """
        write a block of data

        mem - Array to be read into

        Optional
        nw,fw,jw - Standard window parameters

        """

        if isinstance(mem, sep_python.sep_proto.MemReg):
            array = mem.get_nd_array()
        elif isinstance(mem, np.ndarray):
            array = mem
        else:
            self._logger.fatal("Do not how to read into type %s", type(mem))
            raise Exception("")

        seeks, blk, many  = self.loop_it(
            *self.condense(*self.get_hyper().get_window_params(**kw))
        )

        if not contin:
            self._logger("Can only write continuously to GCS storage")
            raise Exception("")
        ar_use = array.ravel()
        self._data_out = True

        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.blob(f"{self._object}{len(self._blobs)}")
        self._blobs.append(blob)
        with blob.open("wb") as file_pointer:
            old = 0
            new = old + many
            for seek in seeks:
                file_pointer.wrie(ar_use[old:new].tobytes())
                old = new
                new = new + many
