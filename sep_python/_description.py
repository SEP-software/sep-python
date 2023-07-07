from abc import ABC, abstractmethod


class description_base(ABC):
    """Description of data base class"""

    def __init__(self):
        self._hyper = None
        self._data_type = None
        self._params = None
        self._meta = False
        self._binary_path_func = False
        self._written = False

    def get_binary_path_func(self):
        """Get the function that returns the data
        path given the description given the description
        file name"""
        if self._binary_path_func is None:
            raise Exception("binary_path_func has not been set")
        return self._binary_path_func

    def set_hyper(self, hyper):
        """Set the hypercube associated with dataset

        hyper - Hypercube that describes regular data

        """
        self._hyper = hyper

    def get_data_type(self):
        """Return data_type"""
        return self._data_type

    def set_data_type(self, typ):
        """Set the data type"

        typ - Set data_type
        """

        self._data_type = typ

    def hyper_to_dict(self, myd: dict):
        """Try to create a hypercube from a dictionary of parameters

        myd - Dictionary of params (e.g. {"n1":5})

        """
        idim = 1
        for axis in self._hyper.axes:
            myd[f"n{idim}"] = axis.n
            myd[f"o{idim}"] = axis.o
            myd[f"d{idim}"] = axis.d
            myd[f"label{idim}"] = axis.label
            myd[f"unit{idim}"] = axis.unit
            idim += 1
        return myd

    def hyper_to_str(self):
        """Return a string descrption of the current hypercube"""
        idim = 1
        out = ""
        for axis in self._hyper.axes:
            out += (
                f"n{idim}={axis.n} o{idim}={axis.o} "
                + f'd{idim}={axis.d} label{idim}="{axis.label}" '
                + f'unit{idim}="{axis.unit}"\n'
            )
            idim += 1
        return out

    def get_hyper(self):
        """Get the hypercube associated with description"""
        return self._hyper

    def set_dictionary(self, dict):
        """Set the dictionary associated with description"""
        self._params = dict

    def get_dictionary(self):
        """Get dictionary associated with description"""
        if self._params is None:
            raise Exception("Requesting a dictionary that hasn't been set")
        return self._params

    @abstractmethod
    def set_binary_path(self):
        """Set the binary path for the dataset"""

    def set_binary_path_func(self, func):
        """Set the function to get the data file"""
        self._binary_path_func = func

    @abstractmethod
    def read_description(path):
        """Read description"""

    @abstractmethod
    def check_valid(path):
        """Check to see if the specified path make sense"""

    @abstractmethod
    def write_description(self):
        """Write description"""

    @abstractmethod
    def remove(self):
        """Remove the description file"""

    def close(self):
        """CLose file"""
        pass
