
import Hypercube
import numpy as np
import sepConverter
import logging

converter=sepConverter.converter

class memReg:
    """Basic class for in memory storage 

    """
    def __init__(self):
        self._hyper=None
        self._storage=None
        self._logger=logging.getLogger(None)

    def setLogger(self,logger):
        """Set the logger

           logger - Logger to use

        """
        self._loggger=loggeer

    def getNdArray(self)->np.ndarray:
        """
        Return a numpy representation of storage"""
        self._logger.fatal("Must override getNdArray")
        raise Exception("")

    def getHyper(self)->Hypercube.hypercube:
        """
        Return the hypercube describing the regular array"""
        if not isinstance(self._hyper,Hypercube.hypercube):
            self._logger.fatal("Hypercube not set correctly")
            raise Exception("")
        return self._hyper
    
    def setHyper(self,hyper:Hypercube.hypercube):
        """
        Set the hypercube for the regular storage

        """
        self._hyper=hyper

    def setDataType(self,storage):
        """
        storage - Type of storage for mem
        """
        self._storage=converter.getName(storage)

    def getDataType(self):
        """
        Return storage type
        """
        if self._storage==None:
            self._logger.fatal("Storage is not set")
            raise Exception("") 
        return self._storage
