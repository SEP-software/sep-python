"""Module to convert between different definitions for different datatype"""
import logging
import numpy as np


class DtypeConvert:
    """Class to convert between types"""

    def __init__(self):
        self._numpy_to_name = {}
        self._name_to_numpy = {}
        self._name_to_esize = {}
        self._name_to_SEP = {}
        self._SEP_to_name = {}
        self._old_style = {}
        self._logger = logging.getLogger("None")

    def set_logger(self, logger: logging.Logger):
        """
        Set the logger for the converter module
        """
        self._logger = logger

    def add_data_type(
        self,
        local_name: str,
        sep_name: str,
        old_style_name: str,
        numpy_type: type,
        esize: int,
    ):
        """
        Add a datatype

        local_name - Local name used for the type
        sep_name   - Data type specified in SEP file
        old_style_name - Name used with previous version of sepVector
        numpy_type    - Numpy type
        esize      - Data byte size
        """
        self._numpy_to_name[numpy_type] = local_name
        self._name_to_numpy[local_name] = numpy_type
        self._name_to_SEP[local_name] = sep_name
        self._name_to_esize[local_name] = esize
        self._SEP_to_name[sep_name] = local_name
        self._old_style[old_style_name] = local_name

    def get_name(self, nm) -> str:
        """Return internal name, handles the case if type or str is provided
        nm (either str or type)
        """
        if isinstance(nm, type):
            if nm not in self._numpy_to_name:
                self._logger.fatal("Unkown type %s", nm)
                raise Exception("")
            return self._numpy_to_name[nm]
        elif isinstance(nm, str):
            if nm[:4] == "data":
                if nm not in self._old_style:
                    self._logger.fatal("Unkwown type %s", nm)
                    raise Exception("")
                nm = self._old_style[nm]
            if nm not in self._name_to_esize:
                self._logger.fatal("Unknown name %s", nm)
                raise Exception("")
            return nm
        else:
            try:
                conv = str(nm)
                if conv not in self._numpy_to_name:
                    self._logger.fatal("Unkown type %s", nm)
                    raise Exception("")
                return self._numpy_to_name[nm]
            except ValueError:
                self._logger.fatal(
                    "Do not know how to deal with type=%s val=%s", type(nm), nm
                )
                raise Exception("")

    def sep_name_to_numpy(self, SEP_name: str) -> type:
        """
        Return Numpy type given sepName

        SEP_name -SEP Name
        """
        if SEP_name not in self._SEP_to_name:
            self._logger.fatal(
                f"Invalid sep type {SEP_name} {self._SEP_to_name}", SEP_name
            )
            raise Exception("")
        return self._name_to_numpy[self._SEP_to_name[SEP_name]]

    def from_SEP_name(self, SEP_name: str) -> str:
        """
        Return Numpy type given sepName

        SEP_name -SEP Name
        """
        if SEP_name not in self._SEP_to_name:
            self._logger.fatal(f"Invalid type {SEP_name}")
            raise Exception("")
        return self._SEP_to_name[SEP_name]

    def get_numpy(self, name) -> type:
        """
        Return Numpy type

        name -Local name
        """
        return self._name_to_numpy[self.get_name(name)]

    def get_esize(self, local_name) -> int:
        """
        Return esize given local name
        """
        return self._name_to_esize[self.get_name(local_name)]

    def get_SEP_name(self, local_name) -> str:
        """
        Return SEP name

        local_name - Local name
        """
        return self._name_to_SEP[self.get_name(local_name)]

    def valid_type(self, typ: str):
        """
        Check to see if specified type is valid"""
        if typ not in self._name_to_numpy.keys():
            return False
        return True


converter = DtypeConvert()
converter.add_data_type("float32", "native_float",
                        "dataFloat", np.float32, 4)
converter.add_data_type("float64", "native_double",
                        "dataDouble", np.float64, 8)
converter.add_data_type("int32", "native_int",
                        "dataInt", np.int32, 4)
converter.add_data_type("uint8", "native_byte",
                        "dataByte", np.uint8, 1)
converter.add_data_type("complex64", "dataComplex",
                        "native_complex", np.complex64, 8)
converter.add_data_type(
    "complex128", "dataComplexDouble",
    "native_complex_double", np.complex128, 16
)
