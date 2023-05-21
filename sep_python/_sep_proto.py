"""generic class for storing regular data in memory"""
import logging
import numpy as np

from sep_python._hypercube import Hypercube
import sep_python._sep_converter

converter = sep_python._sep_converter.converter


class MemReg:
    """Basic class for in memory storage"""

    def __init__(self):
        self._hyper = None
        self._storage = None
        self._logger = logging.getLogger(None)

    def set_logger(self, logger):
        """Set the logger

        logger - Logger to use

        """
        self._logger = logger

    def get_nd_array(self) -> np.ndarray:
        """
        Return a numpy representation of storage"""
        self._logger.fatal("Must override getNdArray")
        raise Exception("")

    def get_hyper(self) -> Hypercube:
        """
        Return the hypercube describing the regular array"""
        if not isinstance(self._hyper, Hypercube):
            self._logger.fatal("Hypercube not set correctly")
            raise Exception("")
        return self._hyper

    def set_hyper(self, hyper: Hypercube):
        """
        Set the hypercube for the regular storage

        """
        self._hyper = hyper

    def set_data_type(self, storage):
        """
        storage - Type of storage for mem
        """
        self._storage = converter.get_name(storage)

    def get_data_type(self):
        """
        Return storage type
        """
        if self._storage is None:
            self._logger.fatal("Storage is not set")
            raise Exception("")
        return self._storage
