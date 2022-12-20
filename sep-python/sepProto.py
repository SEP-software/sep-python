
import Hypercube
import numpy as np
import sepConverter

converter=sepConverter.converter

class memReg:
    """Basic class for in memory storage 

    """
    def __init__(self):
        self._hyper=None
        self._storage=None

    def getNdArray(self)->np.ndarray:
        """
        Return a numpy representation of storage"""
        raise Exception("Must override getNdArray")
    
    def getHyper(self)->Hypercube.hypercube:
        """
        Return the hypercube describing the regular array"""
        if not isinstance(self._hyper,Hypercube.hypercube):
            raise Exception("Hypercube not set correctly")
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
            raise Exception("Storage is not set")
        return self._storage

    def getStorage(self,hyper:Hypercube.hypercube,storage)->memReg:
        """
        Return memory to store data
        """
        raise Exception("Must override get Storage")