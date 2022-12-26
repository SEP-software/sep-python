import numpy as np 
import logging

class dtypeConvert:
  def __init__(self):
    self._numpyToName={}
    self._nameToNumpy={}
    self._nameToEsize={}
    self._nameToSEP={}
    self._sepToName={}
    self._oldStyle={}
    self._logger=logging.getLogger("None")

  
  def setLogger(self,logger:logging.Logger):
    """
    Set the logger for the converter module

    """
    self._logger=logger



  def addDataType(self,localName:str,sepName:str,oldStyleName:str,numpyT:type,esize:int):
    """
      Add a datatype

      localName - Local name used for the type
      sepName   - Data type specified in SEP file
      oldStyleName - Name used with previous version of sepVector
      numpyT    - Numpy type
      eize      - Data byte size
    """
    self._numpyToName[numpyT]=localName
    self._nameToNumpy[localName]=numpyT
    self._nameToSEP[localName]=sepName
    self._nameToEsize[localName]=esize
    self._sepToName[sepName]=localName
    self._oldStyle[oldStyleName]=localName

  def getName(self,nm)->str:
    """ Return internal name, handles the case if type or str is provided
        nm (either str or type)
    """
    if isinstance(nm,type):
      if nm  not in self._numpyToName:
        self._logger.fatal(f"Unkown type {nm}")
        raise Exception("")
      return self._numpyToName[nm]
    elif isinstance(nm,str):
      if nm[:4]=="data":
        if nm not in self._oldStyle:
            self._logger.fatal(f"Unkwown type {nm}")
            raise Exception("")
        nm=self._oldStyle[nm]
      if nm not in self._nameToEsize:
        self._logger.fatal(f"Unknown name {nm}")
        raise Exception("")
      return nm
    else:
      try:
        conv=str(nm)
        if nm  not in self._numpyToName:
          self._logger.fatal(f"Unkown type {nm}")
          raise Exception("")
        return self._numpyToName[nm] 
      except:
        self._logger.fatal(f"Do not know how to deal with type={type(nm)} val={nm}")
        raise Exception("")
  def sepNameToNumpy(self,sepName:str)->type:
    """
    Return Numpy type given sepName

    sepName -SEP Name
    """
    if sepName not in self._sepToName:
      self._logger.fatal(f"Invalid type {sepName}")
      raise Exception("")
    return self._nameToNumpy[self._sepToName[sepName]]

  def fromSepName(self,sepName:str)->str:
    """
    Return Numpy type given sepName

    sepName -SEP Name
    """
    if sepName not in self._sepToName:
      self._logger.fatal(f"Invalid type {sepName}")
      raise Exception("")
    return self._sepToName[sepName]
   
  def getNumpy(self,name)->type:
    """
    Return Numpy type 

    name -Local name
    """
    return self._nameToNumpy[self.getName(name)]

  def getEsize(self,localN)->int:
    """
    Return esize given local name
    """
    return self._nameToEsize[self.getName(localN)]

  def getSEPName(self,localN)->str:
    """
    Return SEP name

    localN - Local name
    """
    return self._nameToSEP[self.getName(localN)]



converter=dtypeConvert()
converter.addDataType("float32","native_float","dataFloat",np.float32,4)
converter.addDataType("float64","native_double","dataDouble",np.float64,8)
converter.addDataType("int32","native_int","dataInt",np.int32,4)
converter.addDataType("uint8","xdr_byte","dataByte",np.uint8,1)
converter.addDataType("complex64","dataComplex","native_complex",np.complex64,8)
converter.addDataType("complex128","dataComplexDouble","native_complex_double",np.complex128,16)
