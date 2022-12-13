import re,string,pwd,sys,os,time,types
import SEP.util
import SEP.parameter

""" 

  Module for parameters SEP style parameter handling

  

"""
                                                                                
__author__ = "Robert G. Clapp"
__email__ = "bob@sep.stanford.edu"
__version = "2005.11.29"

def logic(val):
  """Return 0 if par i 'n',"N",'0' or None else return 1"""
  if not val: return None
  val=str(val)
  valit=1
  if val[0]=='n' or val[0]=="N" or val[0]=='0': valit=None
  return valit

                                                                                
class basic:
  """ A class to hold SEP style parameters"""

  def __init__(self,file=None,name=None,seppar=None):
     """Initializer for SEP.args.basic object

        Three initialization methods:

        From a file par=SEP.args.basic(file='file.P')
        From another parameter object par=SEP.args.basic(seppar=par1)
        An empty list par=SEP.args.basic(name="new")
     
     """
     self.added=[]    #the parameters that have been added to this object
     self.params={}   #all of the parameters in this object
     self.mylines=[]  #the lines in the input file
     self.parq1=re.compile('(\S+)="(\S+)"')
     self.parq2=re.compile("(\S+)='(\S+)'")
     self.parS=re.compile('(\S+)=(\S+)')
     self.commaS=re.compile(',')
     self.qbase="A1b2C"
     self.orig_pars={}
     if not name: name="ARGS"
     if seppar: self._init_seppar(seppar,name)
     if file: 
       self.arg_name=file
       self._build_database(file)
     else: self.arg_name=name
     for k,p in self.params.items():  self.orig_pars[k]=p
  

  def _build_database(self,file):
    """Build the database from a file"""
  
    try: f = open (file)   #OPEN THE FILE
    except:
      SEP.util.err( 'Trouble opening file (%s) for reading '%file)
    lines = f.readlines()  #READ IN THE ENTIRE FILE TO LINES
    for line in lines:
      self.mylines.append(line)
      self.add_string(line,None)
    f.close()  

  def add_string(self,line,new=1):
    """Parse a string adding paramters

       Example:
         par contains a=23,b=25
         par.add_string("c=27 d=28")
         par out a=23,b=25,c=27,d=28
      
    """
    args=line.split()
    self._parse_arg(args,new)

  def _parse_arg(self,args,add=None):
    """Parse an argument changing a=b to par{a}=b"""
    comment=0
    for arg in args:
      if arg[0]=="#": comment=1
      res=None
      if comment!=1:
        q=0
        res=self.parq1.search(arg)
        if  res: q=1
        else:
          res=self.parq2.search(arg)
          if res: q=1
          else:
            res=self.parS.search(arg)
      if res:
        if res.group(1)=="par":
          self._build_database(res.group(2))
        else:
          val=res.group(2)
          if type(val) is types.StringType: 
            if self.commaS.search(val): val=val.split(",")
          self.params[res.group(1)]=SEP.parameter.parameter(res.group(1),value=val)
          if add: self.add_change_param(res.group(1))

  def add_pars(self,seppar,overwrite=1):
    """Add the parameters from another par object

       par   contains a=23,b=24,c=25
       par2  contains d=21,e=a.H

       par.add_pars(par2)

       Resulting par contains a.23,b=24,c=25,d=21,e=a.H

    """
    doit=None
    for par,val in seppar.params.items():
      if not self.params.has_key(par):  doit=1
      elif self.params[par].is_default(): doit=1
      elif overwrite: doit=1
      if doit:
        self.params[par]=val.clone()
        self.add_change_param(par)

  def _init_seppar(self,seppar,name):
    """Initialize a seppar object from another seppar object"""

    self.add_pars(seppar)
    self.args_name=name
    self.mylines = seppar.mylines

  def del_par(self,par,delete_non_default=1):
    """Delete a paramter from the parameter list"""
    if self.params.has_key(par): 
      if delete_non_default or self.params[par].is_default():
        del self.params[par]

  def par(self,p,default=None,error=1,delete=None,doc=None):
    """Return a parameter value  and its number of elements

       Example:
         par contains a=27   c=14.4,16
         
        command                  return          par strucutre
        par.par('a')             27,1            a=27  c=14.4,16
        par.par('b')             None,0          a=27  c=14.4,16
        par.par('c')             [14.4,16],2     a=27  c=14.4,16
        par.par('b',3)           3,1             a=27 b=3 c=14.4,16
        par.par('b',error=1)     error
        par.par('c',delete=1)    [14.4,16],2     a=27 

    """ 
    
    if not self.params.has_key(p):
      self.params[p]=SEP.parameter.parameter(p,default,doc=doc,required=error)
    if type(default) != types.NoneType: self.params[p].set_default(default)
    val,err=self.params[p].par()
    self.add_change_param(p)
    if delete: self.del_par(p)
    return val,err

  def param(self,p,default=None,error=None,delete=None):
    """Return parameter value not number of elements see SEP.args.basic"""
    return self.par(p,default,error,delete)[0]

  def return_required(self,par):
    """Return the documentation for a parameter"""
    if not self.params.has_key(par):
       SEP.util.err("Request for required status for undefined parameter %s%s"%(prefix,par))
    return self.params[par].return_required()
     

  def return_doc(self,par,prefix):
    """Return the documentation for a parameter"""
    if not self.params.has_key(par):
       SEP.util.err("Request for documentation for undefined parameter %s%s"%(prefix,par))
    return self.params[par].return_doc(prefix)
     

  def logic(self,par,default=None,delete=None):
    """Return the status of a logical value in parameter object

       If logical parameter is n,N, or 0 returns Null
       Default parameter (None) will be returned if parameter doesn't exist
       delelte=1 Will delete the parameter

       Examples:
          par contains alpha=1, beta=n

                                    returns          par structure
          par.logic(alpha)             1              alpha=1, beta=n
          par.logic(beta,delete=1)    None            alpha=1
          par.logic(delta,1)            1             alpha=1, beta=n, delta=1
          par.logic(delta,None,1)     None            alpha=1, beta=n

     """
    val,err=self.par(par,default,delete=delete)
    if err==1:  return logic(val)
    return default

  def add_param(self,par,val,doc=None,default=None,required=None,export=1):
    """Add a parameter to the parameter class
  
       Add a parameter par to the class, give it the value val.
       Optional argument for the type

    """

    if self.params.has_key(par):
      self.params[par].update(value=val,doc=doc,
        required=required,default=default)
    else:
      self.params[par]=SEP.parameter.parameter(par,value=val,doc=doc,
        required=required,default=default,export=export)
    self.add_change_param(par)

  def add_doc_param(self,par,default=None,doc=None,required=None,export=1):
    """Add document parameter"""
    self.add_param(par,default=default, doc=doc, required=required,val=None,export=export)
 
  def extract_pars(self,prefix,delete=None):
    """Extract from parameter object all parameters begining with prefix

   
       Example:  object par contains mig_n1=23, n2=27, a=24
       par_out=par.extract_del_pars("mig_")
       par_out contains n1=23
       par contains n2=27,a=24

    """
    srS=re.compile("%s(\S+)"%prefix)
    args_out=SEP.args.basic(name="%s pars"%prefix);
    for key,val in self.params.items():
      a=srS.search(key)
      if a:
        args_out.params[a.group(1)]=val.clone(param=a.group(1))
        if delete: self.del_par(key)
    return args_out

  def return_pars(self):
    """Return the list of parameters that have been defined"""
    return self.params.keys()

  def return_dict(self):
    """Return the dictionary relating to a parameter structure"""
    dict_out={}
    for val in self.params.values(): dict_out[val.param]=val.value
    return dict_out

  def return_param_dict(self):
    """Return the dictionary relating to a parameter structure"""
    return self.params

  def return_basic_params(self,keys=None):
    """Return parameters that are lists,ints,floats,strings,and Nones"""
    par_out=SEP.args.basic(name="ARGS")
    if not keys:keys=self.return_pars()
    for key in keys: 
      v=self.params[key]
      val=v.value
      if type(val) is types.NoneType or type(val) is types.StringType or type(val) is types.IntType or type(val) is types.ListType or type(val) is types.FloatType: par_out.params[key]=v.clone()
    return par_out

  def set_required(self,param,required):
    """Set whether a parameter is required"""
    if not self.params.has_key(param):
      SEP.util.err("Parameter %s has not been setup"%param)
    self.params[param].set_required(required)

  def return_basic_params_dict(self,keys=None):
    """Return parameters that are lists,ints,floats,strings,and Nones"""
    dict_out={}
    for val in self.return_basic_params(keys).params.values(): 
       dict_out[val.param]=val.value
    return dict_out
    
  def return_set_params(self,keys=None):
    """Return parameters that are set and not complex objects"""
    if not keys:keys=self.return_pars()
    par_out=SEP.args.basic(name="ARGS")
    for key in keys: 
      v=self.params[key]
      val=v.par()[0]
      if type(val) is types.StringType or type(val) is types.IntType or type(val) is types.ListType or type(val) is types.FloatType: 
        if v.com_param(): par_out.params[key]=v.clone()
    return par_out

  def return_set_params_dict(self,keys=None):
    """Return parameters that are lists,ints,floats,strings,and Nones"""
    dict_out={}
    for val in self.return_set_params(keys).params.values(): 
       dict_out[val.param]=val.value
    return dict_out
    
  def return_set_params_list(self,quote=1):
    """Return the argument list with None converting "n"

       Example:
         par is a=23 b=12.,13.
         par.return_sep_params_strin() returns [a=23.,b=12.,13.]

    """
    alist=[]
    par=self.return_set_params()
    for val in par.params.values(): alist.append(val.sep_string())
    return alist

  def return_set_params_string(self,quote=1):
    """Return the argument list with None converting "n"

       Example:
         par is a=23 b=12.,13.
         par.return_sep_params_strin() returns [a=23. b=12. 13.]

    """
    return string.join(self.return_set_params_list(quote))

  def write_pars(self,file):
    """ Write the parameters to a file"""
    try: f = open (file,"w")   #OPEN THE FILE
    except:
      SEP.util.err( 'Trouble opening file for writing:', file)
    for line in self.mylines: f.write(line)
    for line in self._new_lines(): f.write(line)


  def add_change_param(self,par):
    """Add parameters that have changed or are new to the list to be
        written out"""
    if self.added.count(par)==0: 
      if self.orig_pars.keys().count(par)==0:
         self.added.append(par)
      elif self.params[par].value != self.orig_pars[par]: 
         self.added.append(par)

  def  _new_lines(self):
    """Return parameters that have been modified or added"""
    lines=[] 
    lines.append("%s :   %s@%s  %s\n"%(sys.argv[0],pwd.getpwuid(os.getuid())[0],
      os.uname()[1],time.strftime("%a %b %d %H:%M:%S %Y",time.localtime(int(time.time())))))
    for par in self.added: lines.append("%s\n"%self.params[par].sep_string());
    return lines

  def return_name(self):
    """Return the name of the parameter block"""
    return self.arg_name
                                                                                   
class sep_args(basic):
  """A SEP.args.basic class that is initialzed with the command line arguments"""
  def __init__(self):
    """Initialize a sep parameter object with command line arguments """
    basic.__init__(self,name="args")
    self._parse_arg(sys.argv[1:])
