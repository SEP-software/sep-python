import logging
import sep_python.sep_converter
import sep_python.sep_proto
from sep_python.hypercube import Hypercube,Axis

converter=sep_python.sep_converter.converter


class RegFile:

    def __init__(self):
        """Default class for a regular file


        """
        self._head=0
        self._hyper=None
        self._history=""
        self._data_type=None
        self._prog_name="Generic Python Program"
        self._io_type="Unknown"
        self._binary_path=None
        self._logger=logging.getLogger(None)

    def set_logger(self,logger:logging.Logger):
        """
        Set the logger

        logger - logger to use
        """
        self._logger=logger

    def set_hyper(self,hyper:Hypercube):
        """
        Set the hypercube for the dataset
        
            hyper- Hypercube for regular dataset
        
        """
        self._hyper=hyper
    
    def get_hyper(self)->Hypercube:
        """
        Get the hypercube associated with the regular dataset"""
        if self._hyper is None:
            self._logger.fatal("Hypercube has not been set")
        return self._hyper
    
    def set_history(self,hist:str):
        """
        Set the history assoicated with a dataset

            hist -> String assoicated with the history for the dataset
        """
        self._history=hist

    def set_prog_name(self,nm:str):
        """
            Set the name of the current program

        """
        self._prog_name=nm

    def get_prog_name(self)->str:
        """
        Return program name
        """
        return self._prog_name

    def set_data_type(self,typ):
        """
            Set the data type for the dataset

            typ - Data type for dataset
        """
        self._data_type=typ

    def get_data_type(self)->str:
        """Return the data type assoicated with the file

        """
        if type(self._data_type) is None:
            self._logger.fatal("Datatype has not been set")
            raise Exception("")
        return self._data_type

    def get_binary_path(self):
        """
        Return the path to the binary
        """
        return self._binary_path
    def set_binary_path(self,path:str):
        """
        Set the path to the binary
        """
        self._binary_path=path

    def __repr__(self):
        """Print information about file"""
        x=f"Regular file, type={self._io_type} storage={self.get_data_type()} binary={self.get_binary_path()}\n"
        x+=str(self.get_hyper())
        return x

    def remove(self,error_if_not_exists:bool=True):
        """Remove data
        
            error_if_not_exists : Return an error if the blockob does not exist
        
        
        """
        self._logger.fatal("Must override remove")
        raise Exception("")

    def get_int(self,param:str,default=None)->int:
        """Return parameter of int
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getInt")
        raise Exception("")

    def get_float(self,param:str,default=None)->float:
        """Return parameter of type float
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getFloat")
        raise Exception("")

    def get_string(self,param:str,default=None)->str:
        """Return parameter of type string
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getString")
        raise Exception("")

    def get_ints(self,param:str,default=None)->list:
        """Return parameter of type int arrau
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getInts")
        raise Exception("")

    def get_floats(self,param:str,default=None)->float:
        """Return parameter of float arry
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getFloats")
        raise Exception("")

    def put_par(self,param:str,val):
        """Store a parameter

            param - Parameter to store
            val  - Value"""
        self._logger.fatal("Must override putPar")
        raise Exception("")

    def read(self,vec:sep_python.sep_proto.MemReg,**kw):
        """
            Read dataset, potentially a window

        vec- Storage to read into
        
        Optional:
            n_wind,fwind,jwind - Windowing parameters
        """
        self._logger.fatal("Must override read")
        raise Exception("")

    def write(self,vec:sep_python.sep_proto.MemReg,**kw):
        """
            Write dataset, potentially a window

        vec- Storage to read into
        
        Optional:
            n_wind,fwind,jwind - Windowing parameters
        """
        self._logger.fatal("Must override read")
        raise Exception("")

    def hyper_to_dict(self,myd:dict):
        idim=1
        for ax in self._hyper.axes:
            myd[f"n{idim}"]=ax.n
            myd[f"o{idim}"]=ax.o
            myd[f"d{idim}"]=ax.d
            myd[f"label{idim}"]=ax.label
            myd[f"unit{idim}"]=ax.unit
            idim+=1
        return myd
    
    def hyper_to_str(self):
        idim=1
        out=""
        for ax in self._hyper.axes:
            out+=f"n{idim}={ax.n} o{idim}={ax.o} d{idim}={ax.d} label{idim}=\"ax.label\" unit{idim}=\"ax.unit\"\n"
            idim+=1
        return out

    def __del__(self):
        """
        Delete function
        """
        self.close()

    def condense(self,nwind_in:list,fwind_in:list,jwind_in:list):

        """Figure out the best way to write a given request
        
        nwind_in - list [int] windowing parameters (number of samples)
        fwind_in - list [int] windowing parameters (initial sample)
        jwind_in - list [int] skip parameter
        
        Output:
            ngOut,nwOut,fwindOut,jwindOut,block
        """
        nwind_out=[nwind_in[0]]
        ng_in=self._hyper.get_ns()
        ng_out=[ng_in[0]]
        fwind_out=[fwind_in[0]]
        jwind_out=[jwind_in[0]]
        iout=0
        block=nwind_out[0]
        for i in range(1,len(nwind_in)):
            #We have the whole axis
            if nwind_in[i]==ng_in[i]:
                nwind_out[iout]*=ng_in[i]
                block*=nwind_in[i]
                ng_out[iout]*=ng_in[i]

            #We have a single chunk of th axis
            elif jwind_in[i]==1:
                jwind_out[iout]=block*jwind_in[i]
                fwind_out[iout]=block*fwind_in[i]
                nwind_out[iout]=block*nwind_in[i]
                ng_out[iout]*=ng_in[i]
                if i!=len(nwind_in)-1:
                    jwind_out.append(1)
                    fwind_out.append(0)
                    nwind_out.append(1)
                    ng_out.append(1)
                    iout+=1
                else:
                    iout+=1
                    fwind_out.append(fwind_in[i])
                    jwind_out.append(jwind_in[i])
                    nwind_out.append(nwind_in[i])
                    ng_out.append(ng_in[i])

        for i in range(len(ng_out),8):
            ng_out.append(1)
            nwind_out.append(1)
            fwind_out.append(0)
            jwind_out.append(1)
        block=[1]
        for i in range(8):
            block.append(ng_out[i]*block[i])
        return ng_out,nwind_out,fwind_out,jwind_out,block

    def close(self):
        """ Close file"""
        pass

    def loop_it(self,ng:list,n_wind:list,fwind:list,jwind:list,block:list)->list:
        seeks=[]
        esize=converter.get_esize(self.get_data_type())

        for ig7 in range(ng[7]):
            pos7=self._head+(fwind[7]+jwind[7]*block[7])*ig7
            for ig6 in range(ng[6]):
                pos6=pos7+(fwind[6]+jwind[6]*block[6])*ig6
                for ig5 in range(ng[5]):
                    pos5=pos6+(fwind[5]+jwind[5]*block[5])*ig5
                    for ig4 in range(ng[4]):
                        pos4=pos5+(fwind[4]+jwind[4]*block[4])*ig4
                        for ig3 in range(ng[3]):
                            pos3=pos4+(fwind[3]+jwind[3]*block[3])*ig3
                            for ig2 in range(ng[2]):
                                pos2=pos3+(fwind[2]+jwind[2]*block[2])*ig2
                                for ig1 in range(ng[1]):
                                    pos1=pos2+(fwind[1]+jwind[1]*block[1])*ig1
                                    if jwind[0] ==1:
                                        seeks.append(pos1)
                                    else:
                                        for ig0 in range(ng[0]):
                                            seeks.append(pos1+(fwind[0]+jwind[0]*block[0])*ig0)
        if jwind[0]==1:
            return seeks,esize*n_wind[0],n_wind[0],len(seeks)==1
        else:
            return seeks,esize,1,False


class InOut:

    def __init__(self,mem_create):
        """Initialize default IO"""
        self._objs={}
        self.append_files={}
        self._mem_create=mem_create
        self._logger=logging.getLogger(None)
    
    def set_logger(self,log:logging.Logger):
        """

        Set logging for io base

        log - Logging for io

        """
        self._logger=log


    def get_reg_storage(self,  **kw):
        """Get object to deal with storage
                Requiered:
                        path - Path to file
                Optional:
                        
        """
        self._logger.fatal("must ovveride getRegFile")
        raise Exception("")
        return None

    def get_reg_vector(self,*arg,**kw):
        """
            Option 1 (supply hypercube):
                    hyper, kw args
            Option 2 (build hypercube):
                    ns = [] - list of sizes
                    os = [] - list of origins
                    ds = [] - list os sampling
                    labels = [] list of labels
                    axes = [] list of axes

            dataFormat = dataFormatType(float32[default], float64,doublocke64,int32,complex64,complex128)

            Option 4 (numpy)
                Provide hyper, ns, os, or ds,label,s axes

        """
        x= self._mem_create(*arg,**kw)
        return x

    def add_storage(self,path, storage_obj):
        """Add regFile to list of files

           path - Path to storage
            storage_obj - Object to add to list
        """
        self._objs[path]=storage_obj
       
    def get_storage(self, path):
        """Return object to interact with storage

            path - Tag associated with file
        """
        if path not in self._objs:
            self._logger.fatal("Requested path not loaded into this IO")
            raise Exception("")
        return self._objs[path]
 
    def vector_from_storage(self, path,  **kw):
        """Get vector from a file and read its contents

           path -Path for vector

           Optional:
           
           args, kw - arguments fo create vector

            

        """
        kw["path"]=path
        file = self.get_reg_storage( **kw)
        self.add_storage(path,file)
        self._objs[path] = file
    
        n_wind,fwind,jwind=file.getHyper().get_window_params(**kw)
        aout=[]
        ain=file.get_hyper().axes
        for i in range(len(n_wind)):
            aout.append(Axis(n=n_wind[i],label=ain[i].label,
             unit=ain[i].unit,d=ain[i].d*jwind[i],
             o=ain[i].o+ain[i].d*fwind[i]))

        hyper_out =Hypercube(axes=aout)

        vec=self._mem_create(hyper_out,data_format=file.get_data_type())
        file.read(vec)
        return vec

    def write_vector(self, path, vec):
        """Write entire sepVector to disk
           path - File to write to
           vec - Vector to write"""
        file=self.get_reg_storage(vec=vec,path=path)
        file.write(vec)
    def write_vectors(self, obj, vecs, ifirst):
        """Write a collection of vectors to a path
                obj - Object to interact with storage
                vecs - Vectors
                ifirst - Position in object for first vector"""
        n_wind = obj.get_hyper().get_ns()
        fwind = [0] * len(n_wind)
        jwind = [1] * len(n_wind)
        n_wind[len(n_wind) - 1] = 1
        iouter = ifirst
        for vec in vecs:
            fwind[len(fwind) - 1] = iouter
            iouter += 1
            obj.write(vec,n_wind=n_wind,fwind=fwind,jwind=jwind)

    def append_vector(self, path, vec, max_length=1000, flush=1):
        """Write entire sepVector to disk
           path - File to write to
           vec - Vector to write
           max_length - Maximum number of appended frames
           flush - How often to flush the files"""
        if path not in self.append_files:
            self.append_files[path] = Append_File(
                self, path, vec, max_length, flush)
        if self.append_files[path].add_vector(vec):
            vs = self.append_files[path].flush_vectors()
            if self.append_files[path].icount > self.append_files[path].nmax:
                self.append_files[path].finish()
            self.write_vectors(
                self.append_files[path].file,
                vs, self.append_files[path].icount - len(vs))

    def close_append_file(self, path):
        """Close an append file and fix the description to the right number of frames"""
        if path not in self.append_files:
            self._logger.fatal("No record of appended file")
            raise Exception("")
        vs = self.append_files[path].flushVectors()
        self.write_vectors(
            self.append_files[path].file,
            vs, self.append_files[path].icount - len(vs))
        self.append_files[path].finish(0)



class Append_File:
    """Class for append files"""

    def __init__(self, io,path, vec, max_length, flush):
        """
                io - IO to use
                path - path to file
                vec - Vector
                max_length - Maximum number of frames
                flush - How often to flush a file"""
        self.shape = vec.cloneSpace()
        self.vecs = []
        self.flush = flush
        self.hyper = vec.getHyper()
        self.hyper.add_axis(Axis(n=max_length))
        self.nmax = max_length
        self._data_format=vec.get_data_format()
 
        self.file = io.get_reg_storage(path, hyper=self.hyper, data_format=self._data_format)
        self.icount = 0

    def add_vector(self, vec):
        """Adds a vector to this of vectors being held"""
        self.vecs.append(vec.clone())
        self.icount += 1
        if len(self.vecs) == self.flush:
            return True
        return False

    def flush_vectors(self):
        """Return the list of vectors, zero out"""
        vs = []
        for v in self.vecs:
            vs.append(v)
        self.vecs = []
        return vs

    def finish(self, iextra=0):
        """Fix the correct number of frames in a file and update description"""
        self.hyper.axes[len(self.hyper.axes) - 1].n = self.icount + iextra
        self.file.set_hyper(self.hyper)
        self.file.write_description()
        self.nmax = self.icount + iextra
