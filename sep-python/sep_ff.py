import re
import string
import pwd
import sys
import os
import socket
import time
import commands
import SEP.args
from math import *
__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version = "2005.3.13"
                                                                                


def min_max(n,o,d):
  """The minimum and maximum for an axis""" 
  n=int(n)
  o=float(o)
  d=float(d)
  end=o+d*(n-1)
  if end > o: return o,end
  else : return end,o 

def axis_same(label1,n1,o1,d1,label2,n2,o2,d2):
  """Check to see if the axes are the same""" 
  err=[]
  n1=int(n1)
  n2=int(n2)
  if not o1: o1=0.
  if not o2: o2=0.
  o1=float(o1)
  o2=float(o2)
  d1=float(d1)
  d2=float(d2)
  if n1 != n2: err.append("ns(%d,%d) not the same"%(n1,n2))
  if abs(o1-o2) > abs(d1)/10.: err.append("os(%f,%f) not the same"%(o1,o2))
  if abs(d1-d2) > abs(d1)/10.: err.append("ds(%f,%f) not the same"%(d1,d2))
  if err: SEP.util.err("Incompatibility between %s and %s\n%s"%(label1,
     label2,string.join(err,"\n")))

class format_file(SEP.args.basic):
  """Extension of SEP.args.basic class that also knows about axes, in"""

  def __init__(self,file=None,name=None,sepfile=None):
    """Intialize a SEPlib format file

       Examples:

         file -  Initialize a format file from a file
            sfile.SEP.sepfile.format_file(file=file)
         name -  Intialize an empty format file
            sfile.SEP.sepfile.format_file(name="new.H")
         sepfile -  Intialize a format file from another format file structure
            sfile.SEP.sepfile.format_file(sepfile=sepfile)

    """
    SEP.args.basic.__init__(self,file,name,sepfile)

  def axis(self,iax,ext=None):
     """Return axis (n,o,d,label,unit) in a format file

        Returns a given axis in a file  

        Examples:

         sfile.axis(1) returns n1,o1,d1,label1,unit1
         sefile.axis(1,'_grid') return n1_grid,o1_grid, etc

     """
     if ext == None: e=""
     else:  e=ext
     n=1
     o=0
     d=1
     unit="none"
     label="none"
     val,err = self.par("n"+str(iax)+e,error=None)
     if err == 1 : n= val
     val,err = self.par("o"+str(iax)+e,error=None)
     if err == 1 : o= val
     val,err = self.par("d"+str(iax)+e,error=None)
     if err == 1 : d= val
     val,err = self.par("label"+str(iax)+e,error=None)
     if err == 1 : label= val
     val,err = self.par("unit"+str(iax)+e,error=None)
     if err == 1 : unit= val
     return int(n),float(o),float(d),label,unit

  def set_axis(self,iax,n,o,d,label="",unit="",e=None):
    """Set axis in a format file description

       Example:
        sfile.set_axis(1,7,0.,1.) sets
          n1=7,o1=0. ,d1=1. label1="" unit1="" 
        sfile.set_axis(1,4,0.,2.,'none',e='_grid')
          n1_grid=4,o1_grid=0.,d1_grid=2.,label1_grid='none',unit1_grid=''

    """
    if e == None: suf=""
    else : suf=e
    self.add_param('n'+str(iax)+suf,n)
    self.add_param('o'+str(iax)+suf,o)
    self.add_param('d'+str(iax)+suf,d)
    self.add_param('label'+str(iax)+suf,label)
    self.add_param('unit'+str(iax)+suf,unit)

  def first_axis(self):
    """Return the first axis in file"""
    return 1

  def axis_min_max(self,iax):
    """Return the minimum and maximum of a given axis

       Example:
         n1=10 o1=0. d1=1.
       sfile=axis_min_max(1) will return 0.,9.
         
    """
    n,o,d,label,unit=self.axis(iax)
    return min_max(n,o,d)

  def size(self):
    """Return the size of a dataset

       Example:
         n1=10 n2=1 n3=6 esize=8
       sfile.size() returns 240

    """
    sz=int(self.elem_size())
    for i in range(self.first_axis(),self.ndims()+1): 
      sz=sz*int(self.axis(i)[0])
    return sz
      

  def ndims(self,suf=None,ibeg=1):
    """Number of dimensions in file
   
       suf if we are looking for n2_grid rather than n2 (suf=_grid)
       ibeg is the first axis that should exist (defaults to 1)

    """
    if suf == None: e=""
    else : e=suf
    i=ibeg
    iax=i; err=1; not1=0
    s="n"+str(iax)+e
    val,err = self.par(s)
    if err == 1:  not1=1
    while err ==1:
       s="n"+str(iax)+e
       val,err = self.par(s)
       if err == 1 and int(val) !=1: not1=iax
       iax=iax+1
    return not1

  def create_fake_in(self,name=None,file=None,host=None):
    """Create a fake data file """
    if name==None: nm=SEP.datapath.datafile(self.return_name(),host)
    else: nm=name

    if file==None: fl=self.return_name()
    else: fl=file

    self.add_param("in",nm)
    try: f = open (nm,"w")   #OPEN THE FILE
    except: SEP.util.err( 'Trouble opening file '+ nm)
    f.write(" ") 
    f.close()

class history(format_file):
  """Extension of the format_file class for SEPlib history file"""
  def __init__(self,file=None,name=None,sepfile=None):
    """See format_file initialization"""
    SEP.sep_ff.format_file.__init__(self,file,name,sepfile)
  def elem_size(self):
     """Get the basic element size for the dataset"""
     val,err=self.par("esize")
     if err==0: val=4
     return val

class hff(format_file):
  """Extension of the format_file class """
  def __init__(self,file=None,name=None,sepfile=None):
    """See format_file initialization"""
    SEP.sep_ff.format_file.__init__(self,file,name,sepfile)
    self.nkeys=0

  def set_nkey(self,nkey):
    """Set the number of keys in a dataset"""
    self.nkeys=nkey
    self.add_param("n1",nkey,"int")

  def elem_size(self):
    """Element size for dataset"""
    return 4

  def key(self,ikey):
     """Return a key from a hff file

        Example:
          headerkey 3 is described by 
           hdrkey3='alpha'  hdrtype3='scalar_float' hdrformat3='xdr_float'

          sfile.key (3) will return

           ['alpha','scalar_float','xdr_float']

     """
     list=("hdrkey","hdrtype","hdrfmt")
     out=[]
     for name in list:
       grab=name+str(ikey)
       val,err = self.par(grab)
       if err==0: SEP.util.err( grab+" doesn't exist in file")
       out.append(val)
     return out[0],out[1],out[2]

  def set_key(self,name,type,fmt,ik=None):
     """Set a key in a hff file

        Example:
         sfile.set_key("alpha","scalar_float","xdr_float",3) will set

           hdrkey3='alpha'  hdrtype3='scalar_float' hdrformat3='xdr_float'
          
          If ik not specified the number of keys will be incremented and
           ik will default to nkeys

     """
     if ik==None: ikey=self.nkeys+1
     else: ikey=ik
     if ikey > self.nkeys: self.set_nkey(ikey)
     if ikey== None: SEP.util.err("ikey must be specified" )
     if name== None: SEP.util.err("name must be specified" )
     if type== None: SEP.util.err("type must be specified") 
     if fmt== None: SEP.util.err("fmt must be specified" )
     self.add_param("hdrkey"+str(ikey),name)
     self.add_param("hdrtype"+str(ikey),type)
     self.add_param("hdrmt"+str(ikey),fmt)

  def key_index(self,name):
    """Return the index of a given key or Null if it doesn't exist"""
    for i in range(int(self.param("n1"))):
      if name==self.key(i+1)[0]: return i+1
    return None

class gff(format_file):
  """Extension of the format_file class to handle a grid format file"""
  def __init__(self,file=None,name=None,sepfile=None):
    SEP.sep_ff.format_file.__init__(self,file,name,sepfile)

  def axis(self,iax):
    """Return a given axis see SEP.sepfile.format_file.axis"""
    if iax ==1 : SEP.util.err("Can't request first axis from a grid")
    return SEP.sep_ff.format_file.axis(self,iax,"_grid")

  def elem_size(self):
    """Eleement size for dataset"""
    return 4

  def first_axis(self):
    """First axis in file"""
    return 2 #shouldn't this be 2

  def set_axis(self,iax,ext,n,o,d,label="",unit=""):
    """Set a given axis see SEP.sep_ff.format_file.set_axis"""
    if iax ==1 : SEP.util.err("Can't set first axis from a grid")
    SEP.sep_ff.format_file.set_axis(self,iax,n,o,d,label,unit,"_grid")
 
  def ndims(self):
    """Return the number of dimensions in a dataset"""
    return SEP.sep_ff.format_file.ndims(self,"_grid",2)
