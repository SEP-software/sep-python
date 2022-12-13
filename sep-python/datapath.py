import os
import re
import SEP.util
                                                                                
"""
                                                                                
  Module that can read SEP datapaths and create datafile names
                                                                                
                                                                                
                                                                                
"""
                                                                                
__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version = "2004.11.18"

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
            check = re.match("(?:%s\s+)?datapath=(\S+)" % hst,line)
            if check:
                path = check.group(1)
            else:
              check = re.match("datapath=(\S+)",line)
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
