"""Module for reading/writing SEP formated files"""

import sys
import numpy as np
import sep_python._io_base
import sep_python._sep_converter
import sep_python._sep_proto
import sep_python._gcp_helper
from sep_python._sep_helpers import get_datapath
from sep_python._data import data_base
from sep_python._file_data import file_dataset
from sep_python._file_data import file_datasets, file_pipe, file_follow_hdr
from sep_python._gcs_io import gcs_dataset, gcs_datasets
from sep_python._gcs_io import gcs_text_data
from sep_python._gcs_io import gcs_sep_description_object
from sep_python._gcs_io import gcs_sep_description_metadata
from sep_python._sep_description import sep_description_base, inmem_description
from sep_python._sep_description import sep_description_file
from sep_python._data import text_data, inmem_data
__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version__ = "2022.12.13"


converter = sep_python._sep_converter.converter


class InOut(sep_python._io_base.InOut):
    """Class for doing IO of SEPlib regular cubes"""

    #
    # The function storage_options returns the type of files.
    #
    # If the class is sub-classed you can redefine this function.
    #
    # Must be declared before __init__
    #

    def __init__(self, create_mem, **kw):
        """
        SEPlib IO

        create_mem - Function to create memory

        Optional:

          logger - Logger to use for IO
          default_description [file] - Description options

          datapath - [Calculated the following
            SEP rules] The default datapath for output
          override_path - [None] The path to override
            all binary file pointers



        """
        super().__init__(create_mem)

        if "logger" in kw:
            self.set_logging(kw["logger"])

        if "datapath" in kw:
            self._default_datapath = kw["datapath"]
        else:
            self._datapth = get_datapath()

        self._des_types = {"file": sep_description_file,
                           "gcs_object": gcs_sep_description_object,
                           "inmem": inmem_description,
                           "gcs_metadata": gcs_sep_description_metadata}
        self._binary_types = {"follow_hdr": file_follow_hdr,
                              "fle_pipe": file_pipe,
                              "other_file": file_dataset,
                              "files": file_datasets,
                              "text_file": text_data,
                              "in_mem": inmem_data,
                              "gcs_object": gcs_dataset,
                              "gcs_text": gcs_text_data,
                              "gcs_objects": gcs_datasets}

    def get_reg_storage(self, path, **kw):
        """
            Method 1: Input
                Based on name, file type

            Method 2: Output

                description [default_description] - Description options
                  file, gcs_object, gcs_metadata, inmem

                binary  [default_binary] - See the list options below
                  follow_hdr, other_file, files,
                   text_file,gcs_object, gcs_objects
        """

        if "array" not in kw and "vec" not in kw and "hyper" not in kw:
            intent = "INPUT"
            (guess, database, binary,
             file_ptr, offset, eot) = self.guess_input_description(path, kw)
            if "description" in kw:
                if guess != kw["description"]:
                    raise Exception(
                        "Guess of input %s and specified input %s don't match",
                        guess, kw["description"])
            elif not guess and "description" not in kw:
                raise Exception("Need to specify description")
            else:
                guess = kw["description"]
            if guess not in self._des_types:
                raise Exception("Don't know how to deal with %s", guess)
            self._des_types[guess].check_valid(path, kw)
            des = self._des_types(path, database=database,
                                  binary=binary, file_ptr=file_ptr, **kw)

            guess = self.guess_input_binary(guess, database, eot)
            if "binary" in kw:
                if guess != kw["binary"]:
                    raise Exception("guessed type %s not same as %s",
                                    guess, kw["binary"])
            elif not guess and "binary" not in kw:
                raise Exception("Need to specify binary")
            else:
                guess = kw["binary"]
            if guess not in self._binary_types:
                raise Exception("Don't know how to deal with %s", guess)
            self._binary_types[guess].check_valid(path, kw)
            binary = self._binary_types[guess](path, "INPUT",
                                               offset=offset,
                                               hyper=des.get_hyper(),
                                               description=des,
                                               file_ptr=file_ptr, **kw)

        else:
            intent = "OUTPUT"
            (guess, data_type,
             file_pt) = self.guess_output_description(path, kw)
            if "description" in kw:
                if guess != kw["description"]:
                    raise Exception(
                        "Guess of input %s and specified input %s don't match",
                        guess, kw["description"])
            elif not guess and "description" not in kw:
                raise Exception("Need to specify description")
            else:
                guess = kw["description"]
            if guess not in self._des_types:
                raise Exception("Don't know how to deal with %s", guess)
            self._des_types[guess].check_valid(path, kw)
            des = self._des_types(path, file_ptr=file_ptr, **kw)
            des = self._des_types(path, "OUTPUT",
                                  data_type=data_type, file_ptr=file_ptr, **kw)

            guess = self.guess_output_binary(guess, path, **kw)
            if "binary" in kw:
                if guess != kw["binary"]:
                    raise Exception("guessed type %s not same as %s",
                                    guess, kw["binary"])
            elif not guess and "binary" not in kw:
                raise Exception("Need to specify binary")
            else:
                guess = kw["binary"]
            if guess not in self._binary_types:
                raise Exception("Don't know how to deal with %s", guess)
            self._binary_types[guess].check_valid(path, kw)
            binary = self._binary_types[guess](path, "OUTPUT",
                                               data_type=data_type,
                                               description=des,
                                               file_ptr=file_ptr, **kw)
        return RegFile(intent, des, binary, kw)

    def guess_input_description(self, path: str, **kw):
        """Guess the input description to use

            path - Path to description
            kw   - Optional argumeemts

            Override this class to expand capabailities

        """

        if path == "<":
            (valid, description, binary,
             file_ptr, eot) = sep_description_file.read_description(path)
            if not valid:
                raise Exception("Trouble reading from stdin")
            return "file", description, binary, file_ptr, 0, True
        elif path[0:5] == "gs://":
            (valid,
             description) = gcs_sep_description_metadata.read_description(path)
            if valid:
                return "gcs_metadata", False, False, False, False
            (valid, description,
             binary, offset,
             eot) = gcs_sep_description_object.read_description(path)
            if valid:
                return "gcs_object", description, False, False, offset, eot
            raise Exception("Specified gcs path can't read description")
        else:
            (valid, description, binary, offset,
             eot) = file_dataset.read_description(path)
            if valid:
                return "file", description, None, None, offset, eot
        return None

    def guess_input_binary(self, description: str, my_dict: dict,  eot: bool):
        """
            description - Description associated with input
            my_dict  - Dictionary read from description
            eot - Whether or not an EOT existed in description

            Override this class to expand capabailities

        """
        if description == "<":
            return "file_pipe"
        elif description == "file":
            if eot:
                return "follow_hdr"
            if "in" in my_dict:
                paths = my_dict["in"].split(":")
                if len(paths) > 1:
                    if len(paths[0]) < 5:
                        return "files"
                    if paths[0][0:5] == "gs://":
                        return "gcs_objects"
                    else:
                        return "files"
                else:
                    if len(my_dict["in"]) < 5:
                        return "file_object"
                    elif my_dict[0:5] == "gs://":
                        return "gcs_object"
                    else:
                        return "file_object"
            else:
                raise Exception("No in innput dataset")
        elif description == "gcs_metadata":
            if "esize" not in my_dict:
                if my_dict["esize"] == 0:
                    return "gcs_text_data"
            if "in" not in my_dict:
                raise Exception("in must be specified")
            paths = my_dict["in"].split(":")
            if len(paths) > 1:
                raise Exception("Expecting a single file with gcs_metadata")
            if len(paths[0]) < 5:
                raise Exception("Expecting a single file with gcs_metadata")
            if paths[0][0:5] != "gs://":
                raise Exception("Expecting gs:// in path")
        elif description == "gcs_object":
            if "esize" not in my_dict:
                if my_dict["esize"] == 0:
                    return "gcs_text_data"
            if "in" not in my_dict:
                raise Exception("in must be specified")
            if my_dict["in"] == "follow_hdr":
                return "gcs_pipe_data"
            paths = my_dict["in"].split(":")
            if len(paths) > 1:
                if paths[0][0:5] == "gs://":
                    return "gcs_objects"
                else:
                    raise Exception("Expecting gcs path")
            else:
                if paths[0][0:5] == "gs://":
                    return "gcs_object"
                else:
                    raise Exception("Expecting gcs path")
        elif description == "inmen":
            return "inmem_data"
        else:
            return None

    def guesss_output_description(self, path: str, **kw):
        """
            path - Path to description
            kw -  Optional arguments

            Override this class to expand capabailities

        """
        if "array" in kw or "vec" in kw:
            if "vec" in kw:
                array = kw["vec"].get_nd_array()
                data_type = str(array.dtype)
            else:
                if "data_type" not in kw:
                    raise Exception("Must specify output data_type")
                data_type = kw["data_type"]

        if path == ">":
            return "file", data_type, sys.stdout.buffer
        if path[0:5] == "gs://":
            if "binary" in kw:
                if kw["binary"] == "gcs_objets":
                    return "gcs_object", data_type, None
                elif kw["binary"] == "gcs_object":
                    return "gcs_object", data_type,  None
                else:
                    return None
            else:
                return "gcso_object", data_type, None
        if "description" in kw:
            if kw["description"] == "inmem":
                return "inmen", data_type, None
        try:
            fl = open(path, "rb")
        except IOError:
            raise Exception("Trouble opening file %s for output", path)
            return "file", data_type, fl
        return None

    def guess_output_binary(self, path: str, description: str, **kw):
        """
            description - Description associated with output
            kw - Optional arguemnts


            Override this class to expand capabailities
        """

        if path == ">":
            return "follow_hdr"
        if description == "inmem":
            return "inmem_data"
        if description == "gcs_metadata":
            return "gcs_object"
        if description == "gcs_object":
            return None
        return None


class RegFile(sep_python._io_base.RegFile):
    """A class to do IO on a regular file"""

    def __init__(self, intent, description_obj: sep_description_base,
                 binary_obj: data_base):
        """
            intent - Intent (INPUT/OUTPUT)
            description_obj - Object to deal with description
            binary_obj - Binary obj

        """

        self._description_obj = description_obj
        self._binary_obj = binary_obj
        self._intent = intent
        self._wrote_history = False
        self._xdr = False

        self._params = description_obj.get_history_description()

    def read(self, mem, **kw):
        """
        read a block of data

        mem - Array to be read into

        Optional
        nw,fw,jw - Standard window parameters

        """
        if isinstance(mem, sep_python._sep_proto.MemReg):
            array = mem.get_nd_array()
        elif isinstance(mem, np.ndarray):
            array = mem
        else:
            raise Exception("Do not how to read into type %s ", type(mem))

        seeks, blk = self.loop_it(
            *self.condense(*self.get_hyper().get_window_params(**kw))[:2]
        )

        bytes_array = self._binary_obj.read(seeks, blk)

        if self._xdr:
            if self._esize == 8:
                tmp = np.frombuffer(bytes_array, dtype=np.float32)
                tmo = tmp.byteswap()
                tmo = np.frombuffer(tmo.tobytes(), array.dtype)
            else:
                tmp = np.frombuffer(bytes_array, dtype=array.dtype)
                tmo = tmp.byteswap()
            ar_use = tmo.copy()
        else:
            ar_use = np.frombuffer(bytes_array, dtype=ar_use.dtype).copy()

        array = ar_use

    def write(self, mem, **kw):
        """
        write a block of data

        mem - Array to be read into

        Optional
        nw,fw,jw - Standard window parameters

        """
        if isinstance(mem, sep_python._sep_proto.MemReg):
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
        self._binary_obj.write(seeks, blk, ar_use.tobytes())

    def write_description(self):
        """Write description file"""
        self._description_obj.write_description()
        self._wrote_history = True

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
                self._logger.fatal(
                    "Can not convert %s=%s to ints", param, value)
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
                self._logger.fatal(
                    "Can not convert %s=%s to floats", param, value)
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
                    self._logger.fatal(
                        "Trouble converting %s to a string", value)
                    raise Exception("") from v_error
        else:
            try:
                pout = str(val)
            except ValueError as v_error:
                self._logger.fatal("Trouble converting %s to a string", val)
                raise Exception("") from v_error
        self._par_put.append(param)
        self._params[param] = pout
