import sepPython.ioBase
import re
import string
import pwd
import sys
import os
import socket
import copy
import time
import types
from sepPython.Hypercube import  hypercube,axis
import numpy as np 
import sepPython.sepConverter
import sepPython.sepProto
from google.cloud import storage 
import re
import sepPython.gcpHelper
from  concurrent import futures 
from typing import List
import logging
import binascii

from math import *
__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version = "2022.12.13"

class io(sepPython.ioBase.io):

    def __init__(self,createMem,**kw):
        """
          SEPlib IO 

          createMem - Function to create memory

          Optional:

            logger - Logger to use for IO

        """
        super().__init__(createMem)
    
        if "logger" in kw:
          self.setLogging(kw["logger"])

    def getRegStorage(self,**kw):
        """

            Return a regular sampled file pointer 
        """
        if "path" not in kw:
            self._logger.fatal("path must be specified")
            raise Exception("")
        
        path=kw["path"]
        if "//" not in path:
          stor=sFile(**kw)
        elif path[:5]=="gs://":
          stor=sGcsObj(**kw)
        self.addStorage(path,stor)
        return stor
        
converter=sepPython.sepConverter.converter


def databaseFromStr(strIn:str,dataB:dict):
  lines=strIn.split("\n")
  parq1=re.compile(r'([^\s]+)="(.+)"')
  parq2=re.compile(r"(\S+)='(.+)'")
  parS=re.compile(r'(\S+)=(\S+)')
  commaS=re.compile(',')
  for line in lines:
    args=line.split()
    comment=0
    for arg in args:
      if arg[0]=="#": comment=1
      res=None
      if comment!=1:
        q=0
        res=parq1.search(arg)
        if res: 
          q=1
          meth=0
        else:
          res=parq2.search(arg)
          if res: 
            q=1
            meth=1
          else:
            res=parS.search(arg)
            meth=2
      if res:
        if res.group(1)=="par":
          try:
            f2=open(res.group(2))
          except:
            self._logger.fatal(f"Trouble opening {res.group(2)}")
            raise Exception("") 
          databaseFromStr(f2.read(),dataB)
          f2.close()
        else:
          val=res.group(2)
          if isinstance(val,str): 
            if commaS.search(val): val=val.split(",")
          print(f" {res.group(1)} {meth}")
          dataB[f"{str(res.group(1))}"]=val
  return dataB

def checkValid(kw:dict,args:dict):
  """Check to make sure keyword is of the correct type
   
   kw - dictionary of kw
   args - Dictionary of argument names and required types
   """
  for arg,typ in args.items():
    if arg in kw:
      if not isinstance(kw[arg],typ):
        logging.getLogger().fatal(f"Expecting {arg} to be of type {typ} but is type {type(arg)}")
        raise Exception("")

class reg(sepPython.ioBase.regFile):
  """A class to """
  def __init__(self,**kw):

    checkValid(kw,{"hyper":hypercube,"path":str,"vec":sepPython.sepProto.memReg,
      "array":np.ndarray,"os":list,"ds":list,"labels":list,
      "units":list,"logger":logging.Logger})

    super().__init__()

    self._xdr=False
    self._parPut=[]
    self._firstWrite=True
    self._wroteHistory=False
    self._head=0 
    self._esize=None
    self._dataOut=False
    self._ioType="SEP"
    self._intent="OUTPUT"
    self._closed=False
    if "logger" in kw:
      self.setLogger(kw["logger"])
    else:
      self.setLogger(logging.getLogger(None))
    
    if "array" in kw or "vec" in kw:
      
      if "array" in kw:
        array=kw["array"]
        if "hyper" in kw:
          self._hyper=copy.deepcopy(kw["hyper"])
        else:
          ns=list(array.shape)[::-1]
          os=[0.] * len(ns)
          ds=[1.] * len(ns)
          labels=[""] * len(ns)
          units=[""] * len(ns)
          if "os" in kw: os=kw["os"]
          if "ds" in kw: ds=kw["ds"]
          if "labels" in kw: labels=kw["labels"]
          if "units" in kw: units=kw["units"]
          if "hyper" in kw:
            if str(ns)!=str(kw["hyper"].getNs()):
              self._logger.fatal("Shape of hypercube and array are different")
              raise Exception("")
          else:
            self._hyper=hypercube(ns=ns,os=os,ds=ds,labels=labels,units=units)
      elif "vec" in kw:
        array=kw["vec"].getNdArray()
        self._hyper=kw["vec"].getHyper()
      self.setDataType(str(array.dtype))
      self._esize=converter.getEsize(str(array.dtype))
      self._params=self.buildParamsFromHyper(self._hyper)

      if "path" not in kw: 
        self._logger.fatal("Must specify path")
        raise Exception("")
      self._path=kw["path"]
      self.setBinaryPath(datafile(self._path))

    elif "hyper" in kw:
      self._params=self.buildParamsFromHyper(kw["hyper"])
      if "path" not in kw: 
        self._logger.fatal("Must specify path in creation")
        raise Exception("")
      self._path=kw["path"]
      self.setBinaryPath(datafile(self._path))
      if "type" not in kw: 
        self._logger.fatal("Musty specify type when creating from hypercube")
        raise Exception("")
      self.setDataType(converter.getNumpy(kw["type"]))
    elif "path" in kw:
      if not isinstance(kw["path"],str):
        self._logger.fatal("path must be a string")
        raise Exception("")
      self._params=self.buildParamsFromPath(kw["path"],**kw)
      self._path=kw["path"]
      self._intent="INPUT"
    else:
      self._logger.fatal("Did not provide a valid way to create a dataset")
      raise Exception("")
    
  def buildParamsFromHyper(self,hyper:hypercube):
    """Build parameters from hypercube"""
    pars={}
    self._history="";

    for i,ax in enumerate(hyper.axes):
      pars[f"n{i+1}"]=ax.n
      pars[f"o{i+1}"]=ax.o
      pars[f"d{i+1}"]=ax.d
      pars[f"label{i+1}"]=ax.label
      pars[f"unit{i+1}"]=ax.unit
    self._hyper=copy.deepcopy(hyper)
    
    return pars

  def buildParamsFromPath(self,fle:str,**kw):
    """Build parameters from Path"""

    pars=self.getHistoryDict(fle)

    ndim=1
    found=False
    axes=[]
    while not found: 
      if not f"n{ndim}" in pars:
        found=True
      else:
        n=pars[f"n{ndim}"]
      if f"o{ndim}" in pars:
        o=pars[f"o{ndim}"]
      else:
        o=0
      if f"d{ndim}" in pars:
        d=pars[f"d{ndim}"]
      else:
        d=1
      if f"label{ndim}" in pars:
        label=pars[f"label{ndim}"]
      else:
        label=""
      if f"unit{ndim}" in pars:
        unit=pars[f"unit{ndim}"]
      else:
        unit=""
      ndim+=1
      if not found:
        axes.append(axis(n=n,o=o,d=d,label=label,unit=unit))
      if "ndims" in kw:
        if kw["ndims"] > len(axes):
          axes.append(n=1)
    self._hyper=hypercube(axes=axes)
    
    if "in" in pars:
        self.setBinaryPath(pars["in"])

    if "data_format" in pars:
      if pars["data_format"][:3]=="xdr":
        self._xdr=True
        pars["data_format"]="native"+pars["data_format"][3:]
      self.setDataType(converter.sepNameToNumpy(pars["data_format"]))
      self._esize=converter.getEsize(converter.fromSepName(pars["data_format"]))
    elif "esize" in pars:
      self._esize=int(pars["esize"])
      if self._esize==1:
        self.setDataType(np.uint8)
      elif self._esize==8:
        self.setDataType(np.complex64)
      elif self._esize==4:
        self.setDataType(np.float32)
    else:
      self._esize=4
      self.setDataType(np.float32)


    return pars

  def getPar(self,param:str,default=None):
    """Return parameter of any type
       param - Parameter to retrieve
       default - Default value 
    """
    if param in self._params:
      return self._params[param]
    if default != None:
      return default
    self._logger.fatal(f"Can't find {param}")
  
  def getInt(self,param:str,default=None)->int:
    """Return parameter of int
       param - Parameter to retrieve
       default - Default value 
    """
    v=self.getPar(param,default)
    try:
      return int(v) 
    except ValueError:
      self._logger.fatal(f"Can convert {param}={v} to int")
      raise Exception("")

  def getFloat(self,param:str,default=None)->float:
    """Return parameter of type float
       param - Parameter to retrieve
       default - Default value 
    """
    v=self.getPar(param,default)
    try:
      return float(v) 
    except ValueError:
      self._loger(f"Can convert {param}={v} to float")
      raise Exception("")

  def getString(self,param:str,default=None)->str:
    """Return parameter of type string
       param - Parameter to retrieve
       default - Default value 
    """
    return self.getPar(param,default)

  def getInts(self,param:str,default=None)->list:
    """Return parameter of type int arrau
       param - Parameter to retrieve
       default - Default value 
    """
    v=self.getPar(param,default)
    vs=v.split(",")
    vout=[]
    for v in vs:
      try:
        vout.append(int(v))
      except:
        self._logger.fatal(f"Can not convert {param}={v} to ints")   
        raise Exception("")
  
  def getFloats(self,param:str,default=None)->float:
    """Return parameter of float arry
       param - Parameter to retrieve
       default - Default value 
    """
    v=self.getPar(param,default)
    vs=v.split(",")
    vout=[]
    for v in vs:
      try:
        vout.append(float(v))
      except:
        self._logger.fatal(f"Can not convert {param}={v} to floats")
        raise Exception("")

  def putPar(self,param:str,val):
    """Store a parameter

      param - Parameter to store
      val  - Value"""
    
    if not isinstance(val,list):
      try: 
        pout=str(val[0])
      except:
        self._logger.fatal(f"trouble converting {val} to a string")
        raise Exception("")
      for v in val[1:]:
        try:
          pout+=","+str(v)
        except:
          self._logger.fatal(f"Trouble converting {v} to a string")
          raise Exception("")
    else:
      try:
        pout=str(val[0])
      except:
        self._logger.fatal(f"Trouble converting {v} to a string")
        raise Exception("")
    self._parPut.append(param)
    self._params[param]=pout

  def getProgName(self)->str:
    """
    Return get program name
    """
    return self._progName

  def close(self):
    """
    Delete function. Write descrption if data has been written
    """
    if not self._wroteHistory and self._intent=="OUTPUT":
      self.writeDescription()
     
class sFile(reg):
  """Class when SEP data is stored in a file"""

  def __init__(self,**kw):

    """"Initialize a sepFile object
    
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
      self.setLogger(kw["logger"])
    else:
      self.setLogger(logging.getLogger(None))
    if "path" not in kw:
      self._logger.fatal("Must specify path")
      raise Exception("")
    if "//"  in kw["path"]:
      self._logger.fatal(f"When creating a file object path must not have a web address {kw['path']}")
      raise Exception("")
    super().__init__(**kw)
    
    print("PARS",self._params)
    if self._intent=="INPUT":
      if "in" in self._params:
        self.setBinaryPath=self._params["in"]
    
    if self.getBinaryPath() ==None:
       self._logger.fatal("Binary path is not")

  def getHistoryDict(self,path):
    """Build parameters from Path"""
    try:
      fl=open(path,"rb")
    except:
      self._logger.fatal(f"Trouble opening {path}")
      raise Exception("")


    mystr=fl.read(1024*1024)
    fl.close()
    ic=mystr.find(4)
    fl=open(path,"r")
    if ic==-1:
      self._head=0
      #self._history=str(mystr)
      self._history=fl.read()
    else:
      self._head=ic
      self._history=fl.read(self._head)
    fl.close()
    print("XXXX",self._history.split("\n"))
    
    #self._history=self._history.replace("\\n","\n").replace("\\t","\t")

    pars={}
    pars=databaseFromStr(self._history,pars)
    return pars
    
  def read(self,mem,**kw):
    """
      read a block of data

      mem - Array to be read into

      Optional
      nw,fw,jw - Standard window parameters

    """
    if isinstance(mem,sepPython.sepProto.memReg):
      array=mem.getNdArray()
    elif isinstance(mem,np.ndarray):
      array=mem
    else:
      self._logger.fatal(f"Do not how to read into type {type(mem)}")
      raise Exception("")

    seeks,blk,many,contin=self.loopIt(*self.condense(*self.getHyper().getWindowParams(**kw)))
    arUse=array.ravel()

    if self.getBinaryPath()=="stdin" or self.getBinaryPath()=="follow_hdr":
      fl=open(self._path,"rb")
    else:
      fl=open(self.getBinaryPath(),"rb")
    old=0
    new=old+many
    for sk in seeks:
      fl.seek(sk+self._head)
      bytes=fl.read(blk)
      if self._xdr:
        bytes=bytearray(bytes)
        bytes.reverse()
      arUse[old:new]=np.frombuffer(bytes, dtype=arUse.dtype).copy()
      old=new
      new=new+many
    fl.close()
  
  def write(self,mem,**kw):
    """
      write a block of data

      mem - Array to be read into

      Optional
      nw,fw,jw - Standard window parameters

    """
    if isinstance(mem,sepPython.sepProto.memReg):
      array=mem.getNdArray()
    elif isinstance(mem,np.ndarray):
      array=mem
    else:
      self._logger.fatal(f"Do not how to read into type {type(mem)}")    
      raise Exception("")
    seeks,blk,many,contin=self.loopIt(*self.condense(*self.getHyper().getWindowParams(**kw)))
    arUse=array.ravel()
    self._dataOut=True
    fl=open(self.getBinaryPath(),"wb+")
    old=0
    new=old+many
    for sk in seeks:
      fl.seek(sk)
      fl.write(arUse[old:new].tobytes())
      old=new
      new=new+many
    fl.close()

  def writeDescription(self):
    """Write description to path"""


    fl=open(self._path,"w")
    fl.write(f"{self._history}\n{self.getProgName()}\n")
    for par in self._parPut:
      fl.write(f"{par}={self._params[par]}")
    fl.write("\n"+self.hyperToStr())
    self.setBinaryPath(datafile(self._path))
    fl.write(f"in={self.getBinaryPath()}\n")
    fl.write(f"esize={self._esize} data_format={converter.getSEPName(self.getDataType())}\n\n")
    self._wroteHistory=True
  
    fl.close()

  def remove(self,errorIfNotExists=True):
    """Remove data 
    
      errorIfNotExist- Return an error if file does not exist
    
    """
    if os.path.isfile(self._path):
      os.remove(self._path)
      if self._binaryPath!="stdin":
        if os.path.isfile(self._binaryPath):
          os.remove(self._binaryPath)
    elif errorIfNotExists:
      self._logger.fatal(f"Tried to remove file {self._path}")
      raise Exception("")

class sGcsObj(reg):
  """Class when SEP data is stored in an object"""

  def __init__(self,**kw):
      """"Initialize a sepFile object

      
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
        self.setLogger(kw["logger"])
      else:
        self.setLogger(logging.getLogger(None))

      if "path" not in kw:
          self._logger.fatal("path must be specified when creating object")
          raise Exception("")

      reS=re.compile(r"gs://(\S+)\/(.+)")
      x=reS.search(kw["path"])

      self._blobs=[]
      if x:
          self._bucket=x.group(1)
          self._object=x.group(2)
      else:
          self._logger.fatal(f"Invalid path for google storage object {kw['path']}")
          raise Exception("")
      super().__init__(**kw)

  def getHistoryDict(self,path):
      client = storage.Client()
      bucket = client.bucket(self._bucket)
      if not bucket.exists():
          self._logger.fatal(f"bucket {self._bucket} does not exist")
          raise Exception("")

      blob = bucket.get_blob(self._object)
      if not blob.exists():
          self._logger.fatal(f"blob {self._blob} does not exist in bucket {self._bucket}")
          raise Exception("")

      pars = blob.metadata
      newS=""
      for k,v in pars.items():
          newS+=f"{k}={v}"
      if "history" in pars:
          pars=databaseFromStr(pars["history"],pars)
          self._history=pars["history"]
          if "progName" in pars:
              self._history+=f"\n{pars['progName']}\n"
              del pars["history"]
      self._history+=f"\n{newS}"
      return pars

  def writeDescription(self):
      """Write description to path"""
      pass;

  def writeDescriptionFinal(self):
      """
        Write description when closing file

        blob - Blob to set metadata (history)

      """
      tmp=copy.deepcopy(self._params)
      tmp["history"]=self._history
      tmp["progName"]=self.getProgName()
      tmp["esize"]=self._esize 
      tmp["data_format"]=converter.getSEPName(self.getDataType())

      self.hyperToDict(tmp)
      storage_client = storage.Client()
      bucket = storage_client.bucket(self._bucket)
      blob = bucket.blob(self._object)
      blob.metadata = tmp
      blob.patch()
  
  def close(self):
    """Close (and) pottentially combine GCS objects"""

    if self._closed==True:
      self._logger.info(f"Closed called multiple times {self._object}")

    elif self._intent=="OUTPUT":
      self._closed=True
      found=False
      sleep=.2
      itry=0
      while not found and itry <5:
        try:
          storage_client = storage.Client()
          bucket = storage_client.bucket(self._bucket)
          if len(self._blobs)==0:
            blob = bucket.blob(self._object)
            blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
          elif len(self._blobs)==1:
              self._logger.info(f"Renaming {self._blobs[0].name} to {self._object}")
              new_blob = bucket.rename_blob(self._blobs[0], self._object)
          else:
            with futures.ThreadPoolExecutor(max_workers=60) as executor:
              destination=sepPython.gcpHelper.compose(f"gs://{self._bucket}/{self._object}",self._blobs,storage_client,executor,
              self._logger)
          self.writeDescriptionFinal()
          found=True
          #for a in self._blobs:
          #   a.delete()
        except:
          itry+=1
          time.sleep(sleep)
          sleep=sleep*2
          if itry==5:
            self._logger.fatal("Trouble obtaining client")
            raise Exception("trouble obtaining client")
     
  def __del__(self):
    """Delete object"""
    if not self._closed:
      self._logger.fatal("Must close gcs object before the delete is called")
      raise Exception("")
  def remove(self,errorIfNotExists:bool=True):
      """Remove data 
       
         errorIfNotExists - Return an error if blob does not exist
      
      """
      storage_client = storage.Client()
      bucket = storage_client.bucket(self._bucket)
      blob = bucket.get_blob(self._object)
      if blob.exists():
        blob.delete()
      elif errorIfNotExists:
        self._logger.fatal(f"Attempted to remove blob={self._object} which does not exist")
        raise Exception("")
  def read(self,mem,**kw):
      """
          read a block of data

          mem - Array to be read into

          Optional
          nw,fw,jw - Standard window parameters

      """

      if isinstance(mem,sepPython.sepProto.memReg):
        array=mem.getNdArray()
      elif isinstance(mem,np.ndarray):
        array=mem
      else:
        self._logger.fatal(f"Do not how to read into type {type(mem)}")
        raise Exception("")
      seeks,blk,many,contin=self.loopIt(*self.condense(*self.getHyper().getWindowParams(**kw)))
      arUse=array.ravel()

      storage_client = storage.Client()
      bucket = storage_client.bucket(self._bucket)
      blob = bucket.get_blob(self._object)

      with blob.open("rb") as fl:
          old=0
          new=old+many
          for sk in seeks:
              fl.seek(sk+self._head)
              bytes=fl.read(blk)
              if self._xdr:
                  bytes=bytearray(bytes).reverse()
              arUse[old:new]=np.frombuffer(bytes, dtype=arUse.dtype).copy()
              old=new
              new=new+many

  def write(self,mem,**kw):
      """
          write a block of data

          mem - Array to be read into

          Optional
          nw,fw,jw - Standard window parameters

      """


      if isinstance(mem,sepPython.sepProto.memReg):
        array=mem.getNdArray()
      elif isinstance(mem,np.ndarray):
        array=mem
      else:
        self._logger.fatal(f"Do not how to read into type {type(mem)}")
        raise Exception("")
      
      seeks,blk,many,contin=self.loopIt(*self.condense(*self.getHyper().getWindowParams(**kw)))

      if not contin:
        self._logger("Can only write continuously to GCS storage")
        raise Exception("")
      arUse=array.ravel()
      self._dataOut=True

      storage_client = storage.Client()
      bucket = storage_client.bucket(self._bucket)
      blob = bucket.blob(f"{self._object}{len(self._blobs)}")
      self._blobs.append(blob)
      with blob.open("wb") as fl:
        old=0
        new=old+many
        for sk in seeks:
            fl.write(arUse[old:new].tobytes())
            old=new
            new=new+many

def datapath(host=None,all=None):
    """Return the datapath

        If host is not specified  defaults to the local machine
        If all is specifed returns the list of a ; list of directories

    """    

    if host == None: hst=os.uname()[1]
    else: hst=host

    path = os.environ.get('DATAPATH')
    if not path:
        try:
            file = open('.datapath','r')
        except:
            try:
                file = open(os.path.join(os.environ.get('HOME'),'.datapath'),'r')
            except:
                file = None
    if file:
        for line in file.readlines():
            check = re.match(r"(?:%s\s+)?datapath=(\S+)" % hst,line)
            if check:
                path = check.group(1)
            else:
                check = re.match(r"datapath=(\S+)",line)
                if check:
                    path = check.group(1)
        file.close()
    if not path:
        path = "/tmp/"
    if  all:  return path.split(":")
    return path

def datafile(name,host=None,all=None,nfiles=1):
  """ Returns the datafile name(s) using SEP datafile conventions

      if host is not specified defaults to local machine
      if all is specified and datapath is a ; seperated 
          list returns list of paths
      if nfiles is specified returns multi-file names

   """

  f=datapath(host,all)
  if all:
    list=[]
    for i in range(nfiles):
      for dir in f:
         if i ==0: end="@"
         else: end="@"+str(i)
         list.append(dir+os.path.basename(name)+end)
    return list
  else:
    return f+ os.path.basename(name)+"@"
  return f
  
  
  def remove(self):
    """Remove data """
    os.remove(self._path)
    if self._binaryPath!="stdin":
        os.remove(self._binaryPath)

