import re
import string
import pwd
import sys
import os
import socket
import time
import hHypercube
from math import *
__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version = "2004.4.6"

def databaseFromStr(strIn,dataB):
  lines=strIn.split("\n")
  parq1=re.compile('(\S+)="(\S+)"')
  parq2=re.compile("(\S+)='(\S+)'")
  parS=re.compile('(\S+)=(\S+)')
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
        if  res: q=1
        else:
          res=parq2.search(arg)
          if res: q=1
          else:
            res=parS.search(arg)
      if res:
        if res.group(1)=="par":
          try:
            f2=open(res.group(2))
          except:
            raise Exception(f"Trouble opening {res.group(2)}")
          databaseFromStr(f2.read(),dataB)
          f2.close()
        else:
          val=res.group(2)
          if type(val) is types.StringType: 
            if self.commaS.search(val): val=val.split(",")
          dataB[res.group[1]]=val
  return dataB


class sepFile:
  """A class to """
  def __init__(self,**kw):
    """"Initialize a sepFile object
    
        Method 1:
         hyper - Hypercube 
        Method 2:
         file  - File
    """


    if "hyper" in kw:
      self._params=self.buildParamsFromHyper(kw["hyper"])
    elif "file" in kw:
      self._params=self.buildParamsFromFile(kw["file"])

  def buildParamsFromHyper(self,hyper):
    """Build parameters from hypercube"""
    self._pars={}
    for i,ax in enumerate(hyper.getAxes()):
      self._pars[f"n{i+1}]=ax.n
      self._pars[f"o{i+1}]=ax.o
      self._pars[f"d{i+1}]=ax.d
      self._pars[f"label{i+1}]=ax.label
      self._pars[f"unit{i+1}]=ax.unit

  def buildParamsFromFile(self,fle):
    """Build parameters from file"""
    try:
      fl=open(fle)
    except:
      raise Exception(f"Trouble opening {fle}")
    mystr=byteafl.read(1000*1000)
    buf=encode(mystr)
    self._head=buf.find(4)

    eol = bytearray([4])
    byteA=str.encode(buf)
    self._pars={}
    self._pars=databaseFromStr(mystr,self._pars)
    ndim=1
    found=False
    axes=[]
    while not found:
      if not f"n{ndim}" in self._pars:
        found=True
      else:
        n=self._pars[f"n{ndim}"]
      if f"o{ndim}" in self._pars:
        o=self._pars[f"o{ndim}"]
      else:
        o=0
      if f"d{ndim}" in self._pars:
        d=self._pars[f"d{ndim}"]
      else:
        d=1
      if f"label{ndim}" in self._pars:
        label=self._pars[f"label{ndim}"]
      else:
        label=""
      if f"unit{ndim}" in self._pars:
        o=self._pars[f"unit{ndim}"]
      else:
        unit=""
      ndim+=1
      axes.append(Hypercube.axis(n,o,d,label,unit))
      self._hyper=Hypercube.hypercube(axes=axes)

  def getHyper(self):
    """Return hypercube"""
    return self._hyper

  def getWindowParam(self, **kw):
        """Return window parameters
                must supply n,f,or j"""
        axes = self.getHyper().axes
        ndim = len(axes)
        for a in ["f", "j", "n"]:
            if a in kw:
                if not isinstance(kw[a], list):
                    raise Exception(
                        "Expecting %s to be a list the dimensions of your file" %
                        a)
                if len(kw[a]) != ndim:
                    raise Exception(
                        "Expecting %s to be a list the dimensions of your file" %
                        a)
        if "j" in kw:
            js = kw["j"]
        else:
            js = [1] * ndim
        if "f" in kw:
            fs = kw["f"]
            for i in range(len(fs)):
                if fs[i] >= axes[i].n:
                    raise Exception(
                        "Invalid f parameter f(%d)>=ndata(%d) for axis %d" %
                        (fs[i], axes[i].n, i + 1))

        else:
            fs = [0] * ndim
        if "n" in kw:
            ns = kw["n"]
            for i in range(len(fs)):
                if ns[i] > axes[i].n:
                    raise Exception(
                        "Invalid n parameter n(%d) > ndata(%d) for axes %d" %
                        (ns[i], axes[i].n, i + 1))
        else:
            ns = []
            for i in range(ndim):
                ns.append(int((axes[i].n - 1 - fs[i]) / js[i] + 1))
        for i in range(ndim):
            if axes[i].n < (1 + fs[i] + js[i] * (ns[i] - 1)):
                raise Exception(
                    "Invalid window parameter (outside axis range) f=%d j=%d n=%d iax=%d ndata=%d" %
                    (fs[i], js[i], ns[i], i + 1, axes[i].n))
        return ns, fs, js


class sep_file:
  """A class for dealing with a SEPlib dataset

     Can handle regular, header, and grided datasets

     Can read and write files

   """

  def __init__(self,file=None,sepfile=None,name=None,fmt=None,remote=None):
    """Initialize a SEPlib file
 
       Initialize by specifying one or of the initialized parameters

       Initialize by a  (file), another sepfile (sepfile),
         an empty file (name and fmt), or a remote file (remote)

       Examples: 
         sfile=SEP.sepfile.sep_file(file="a.H")
         sfile=SEP.sepfile.sep_file(sepfile=sfile2)
         sfile=SEP.sepfile.sep_file(name="a.H",fmt="REGULAR")
         sfile=SEP.sepfile.sep_file(name="a.H",fmt="HEADERS")
         sfile=SEP.sepfile.sep_file(name="a.H",fmt="GRID")
         sfile=SEP.sepfile.sep_file(remote="sep201:a.H")
 

    """
    self.grid=None
    self.headers=None
    if remote!=None: self._remote_init(remote)
    elif file != None: self._file_init(file) 
    elif sepfile != None: 
      if name !=  None: self._sep_init(sepfile,name)
      else:
        SEP.util.err( "Must provide the name if initializing from another sepfile")
    elif name !=None: self._name_init(name,fmt)
    else:
      SEP.util.err( "Must initialize a sep_file with a file or another sep_file")
    self.sep_name=self.history.return_name()


  def _remote_init(self,remote):
    """Initialize a file from a remote location"""
    pts=remote.split(":")
    if len(pts)!=2: SEP.util.err( "Expecting remote location of the form host:file")
    mach=pts[0]
    file=pts[1]
    temp_file=SEP.util.temp_file("temp_rep")
    SEP.spawn.run_wait(SEP.rc.cp_from(mach,file,temp_file),oper_len=60,ntrys=3)
    self.history=SEP.sep_ff.history(temp_file)
    list={'hff':"@@",'gff':"@@@@"}
    cp_com=[]
    rm_com=["rm "+temp_file]
    for part,ext in list.items():
      val,err=self.history.par(part)       
      if err ==1 and val!="-1": 
        rm_com.append("rm "+temp_file+ext)
        SEP.spawn.run_wait(SEP.rc.cp_from(mach,val,temp_file+ext))
    for com in cp_com: SEP.spawn.run_wait(string.split(com),error=None,oper_len=10,ntrys=3)
    val,err=self.history_par("hff")
    if err==1 and val != "-1":
      self.hff=SEP.sep_ff.hff(temp_file+"@@")
      val,err=self.history_par("gff")
      if err==1 and val != "-1":
        self.gff=SEP.sep_ff.gff(temp_file+"@@@@")
        self.file_format="GRID"
      else:  self.file_format="HEADERS"
    else: self.file_format="REGULAR"
    for com in rm_com: SEP.spawn.run_wait(string.split(com),oper_len=90,ntrys=3)

  def file_name(self):
    """Return file name"""
    return  self.history.return_name()

  def _name_init(self,name,fmt):
    """Initialize a blank file"""
    if fmt==None: self.file_format="REGULAR"
    else: self.file_format=fmt
    if self.file_format != "REGULAR" and  self.file_format != "HEADERS" and  self.file_format != "GRID":
     SEP.util.err( fmt+"= format must be REGULAR,HEADERS,or GRID")
    self.history=SEP.sep_ff.history(name=name)
    self.history.set_axis(1,1,0.,1.,"none","none")
    if self.file_format=="REGULAR": 
      self.history.set_axis(2,1,0.,1.,"none","none")
    else:
      self.headers=SEP.sep_ff.hff(name=name+"@@")
      self.headers.set_axis(2,1,0.,1.,"none","none")
      self.headers.set_key("none","scalar_float","xdr_float")
      if self.file_format=="GRID":
        self.grid=SEP.sep_ff.gff(name=name+"@@@@")
        self.grid.set_axis(2,1,0.,1.,"none","none")

  def _sep_init(self,sepfile,name):
    """Initalize a sep_file object from a sep_file object"""
    self.history=SEP.sep_ff.history(sepfile=sepfile.history,name=name)
    self.file_format=sepfile.file_format
    if sepfile.file_format != "REGULAR":
      self.headers=SEP.sep_ff.hff(sepfile=sepfile.headers,name=name+"@@")
      if sepfile.file_format == "GRID":
        self.grid=SEP.sep_ff.gff(sepfile=sepfile.grid,name=name+"@@@@")

  def _file_init(self,file):
    """Initalize a sep_file object from a file"""
    try: f = open (file)   #OPEN THE FILE
    except: SEP.util.err( 'Trouble opening file:'+ file)
    f.close()
    self.history=SEP.sep_ff.history(file=file)
    val, err = self.history.par("hff")
    if err == 1 and val != "-1":
      self.headers=SEP.sep_ff.hff(file=val)
      val, err = self.history.par("gff")
      if err ==1 and val != "-1":
        self.grid=SEP.sep_ff.gff(file=val)
        self.file_format="GRID"
      else: self.file_format="HEADERS"
    else: self.file_format="REGULAR"

  def history_par(self,par,error=None):
     """Get parameter from history file"""
     return self.history.par(par,error=error)

  def header_par(self,par,error=None):
     """Get parameter from hff file"""
     if self.file_format == "REGULAR": return None,-1
     return self.headers.par(par,error=error)

  def grid_par(self,par,error=None):
     """Get parameter from gff file"""
     if self.file_format != "GRID": return None,-1
     return self.grid.par(par,error=error)

  def add_history_par(self,par,value,type="str"):
     """Set parameter in history file"""
     return self.history.add_param(par,value,type)

  def add_header_par(self,par,value,type="str"):
     """Set parameter in hff file"""
     if self.file_format == "REGULAR": return None,-1
     return self.headers.add_param(par,value,type)

  def add_grid_par(self,par,value,type="str"):
     """Set parameter in gff file"""
     if self.file_format != "GRID": return None,-1
     return self.grid.add_param(par,value,type)

  def axis(self,iax):
     """Set axis for a SEP dataset

        SEE: SEP.sepfile.format_file.axis

     """
     if self.file_format == "GRID" and iax != 1:  
       return self.grid.axis(iax)
     elif self.file_format == "HEADERS" and iax !=1:  
       return self.headers.axis(iax)
     else :  
       return self.history.axis(iax)

  def write_file(self,base=None):
    """Write a seplib file to disk"""
    if self.file_format!="REGULAR":
      if base == None: self.headers.write_pars(self.headers.return_name())
      else:
        nmh,val=self.history.par("hff")
        self.history.add_param("hff",base+"@@")
        self.headers.write_pars(base+"@@")
        if self.file_format=="GRID":
          if base == None: self.grid.write_pars(self.headers.return_name())
          else:
            nmg,val=self.history.par("gff")
            self.history.add_param("gff",base+"@@@@")
            self.grid.write_pars(base+"@@@@")
    if base == None: self.history.write_pars( self.history.return_name())
    else: self.history.write_pars(base)
    if self.file_format  != "REGULAR" and base != None:
      self.history.add_param("hff",nmh)
      if self.file_format=="GRID" : self.grid.add_param("gff",nmg)

  def set_axis(self,iax,n,o,d,label="",unit=""):
     """Set an axis in a seplib dataset

        SEE: SEP.sepfile.format_file.set_axis

     """
     if self.file_format == "GRID" and iax!=1:  
       return self.grid.set_axis(iax,n,o,d,label,unit)
     elif self.file_format == "HEADERS" and iax!=1:  
       return self.headers.set_axis(iax,n,o,d,label,unit)
     else :  return self.history.set_axis(iax,n,o,d,label,unit)

  def axis_min_max(self,iax):
     """Return axis minimum and maximum 

        SEE: SEP.sepfile.format_file.axis_min_max

     """
     if self.file_format == "GRID" and iax!=1:  return self.grid.axis_min_max(iax)
     elif self.file_format == "HEADERS" and iax!=1:  return self.headers.axis_min_max(iax)
     else : return self.history.axis_min_max(iax)
     
  def create_fake(self,host=None):
    """Open up 0 length files and set in="""
    self.history.create_fake_in(host=host)
    if self.headers != None: self.headers.create_fake_in(host=host)
    if self.grid != None: self.grid.create_fake_in(host=host)

  def remove(self,add="",verb=None):
    """Remove the format and binary files from disk if they exist"""
    parts=[self.history,self.headers,self.grid]
    list=[]
    for part in parts:
      if part != None:
        if os.path.isfile(part.return_name()): list.append("rm %s "%add+part.return_name())
        val,err = part.par("in")
        if err==1: 
          if os.path.isfile(val): list.append("rm %s %s"%(add,val))
    for com in list: SEP.spawn.run_wait(string.split(com),verb=verb,ntrys=3,oper_len=99)
     

  def send(self,host,file,binary=None):
     """Transfer a sepfile to another host

         transfer to host (host) with the filename (file) 

         If binary is defined also transfer binary associated with 
         format files

     """
     temp=SEP.util.temp_file("temp_rep")
     tempstruct=SEP.sepfile.sep_file(sepfile=self,name=temp)
     copy_list={}
     copy_list[tempstruct.history.return_name()]=file
     if binary:
         bin=SEP.datapath.datafile(file,host)
         val,err = self.history.par("in")
         if err == 1:  
           copy_list[val]=bin
           tempstruct.history.add_param("in",bin)
     erase_com=["rm "+temp]
     if tempstruct.headers != None:
        erase_com.append("rm "+temp+"@@")
        copy_list[tempstruct.headers.return_name()]=file+"@@"
        if binary:
           bin=SEP.datapath.datafile(file+"@@",host)
           val,err = self.headers.par("in")
           if err == 1:  
             copy_list[val]=bin
             tempstruct.headers.add_param("in",bin)
        tempstruct.history.add_param("hff",file+"@@")
     if tempstruct.grid != None:
        erase_com.append("rm "+temp+"@@@@")
        copy_list[tempstruct.grid.return_name()]=file+"@@@@"
        if binary:
           bin=SEP.datapath.datafile(file+"@@@@",host)
           val,err = self.grid.par("in")
           if err == 1:  
             copy_list[val]=bin
             tempstruct.grid.add_param("in",bin)
        tempstruct.history.add_param("gff",file+"@@@@")
     tempstruct.write_file(temp)
     copy_com=[]
     for myl,r in copy_list.items():
       copy_com.append(SEP.rc.cp_to(mch,myl,r))
     for com in copy_com: SEP.spawn.run_wait(string.split(com),ntrys=3,oper_len=99)
     for com in erase_com: SEP.spawn.run_wait(string.split(com),ntrys=3,oper_len=99)

  def size(self):
     """Return the size of a SEPlib3d dataset
        
        If grided  dataset returns the size of the grid
        If headers dataset returns the size of the headers
        If regular dataset returns the size of the regular dataset

        SEE: SEP.sepfile.format_file for more information

     """
     if self.file_format == "GRID":  return self.grid.size()*self.axis(1)[0]
     elif self.file_format == "HEADERS":  return self.headers.size()*int(self.history.axis(1)[0])
     elif self.file_format == "REGULAR":  return self.history.size()
     else: SEP.util.err("File format not specified")

  def ndims(self):
     """Return the number of dimensions in a SEPlib3d dataset

        SEE: SEP.sepfile.format_file for more information

     """
     if self.file_format == "GRID":  return self.grid.ndims()
     elif self.file_format == "HEADERS":  return self.headers.ndims()
     elif self.file_format == "REGULAR":  return self.history.ndims()
     else: SEP.util.err("File format not specified")

