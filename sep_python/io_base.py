import sepPython.sepConverter
import sepPython.sepProto
from sepPython.hypercube import Hypercube,Axis
import logging

converter=sepPython.sepConverter.converter


class reg_file:

    def __init__(self):
        """Default class for a regular file


        """
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
        if self._hyper==None:
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
        if type(self._data_type)==None:
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
        
            error_if_not_exists : Return an error if the blob does not exist
        
        
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

    def read(self,vec:sepPython.sepProto.memReg,**kw):
        """
            Read dataset, potentially a window

        vec- Storage to read into
        
        Optional:
            nw,fw,jw - Windowing parameters
        """
        self._logger.fatal("Must override read")
        raise Exception("")

    def write(self,vec:sepPython.sepProto.memReg,**kw):
        """
            Write dataset, potentially a window

        vec- Storage to read into
        
        Optional:
            nw,fw,jw - Windowing parameters
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

    def condense(self,nw_in:list,fw_in:list,jw_in:list)->tuple(list,list,list,list,list):

        """Figure out the best way to write a given request
        
        nw_in - list [int] windowing parameters (number of samples)
        fw_in - list [int] windowing parameters (initial sample)
        jw_in - list [int] skip parameter
        
        Output:
            ngOut,nwOut,fwOut,jwOut,bl
        """
        nw_out=[nw_in[0]]
        ng_in=self._hyper.get_ns()
        ng_out=[ng_in[0]]
        fw_out=[fw_in[0]]
        jw_out=[jw_in[0]]
        iout=0
        bl=nw_out[0]
        for i in range(1,len(nw_in)):
            #We have the whole axis
            if nw_in[i]==ng_in[i]:
                nw_out[iout]*=ng_in[i]
                bl*=nw_in[i]
                ng_out[iout]*=ng_in[i]

            #We have a single chunk of th axis
            elif jw_in[i]==1:
                jw_out[iout]=bl*jw_in[i]
                fw_out[iout]=bl*fw_in[i]
                nw_out[iout]=bl*nw_in[i]
                ng_out[iout]*=ng_in[i]
                if i!=len(nw_in)-1:
                    jw_out.append(1)
                    fw_out.append(0)
                    nw_out.append(1)
                    ng_out.append(1)
                    iout+=1
                else:
                    iout+=1
                    fw_out.append(fw_in[i])
                    jw_out.append(jw_in[i])
                    nw_out.append(nw_in[i])
                    ng_out.append(ng_in[i])

        for i in range(len(ng_out),8):
            ng_out.append(1)
            nw_out.append(1)
            fw_out.append(0)
            jw_out.append(1)
        bl=[1]
        for i in range(8):
            bl.append(ng_out[i]*bl[i])
        return ng_out,nw_out,fw_out,jw_out,bl

    def close(self):
        """ Close file"""
        pass

    def loop_it(self,ng:list,nw:list,fw:list,jw:list,bl:list)->list:
        seeks=[]
        esize=converter.get_esize(self.get_data_type())

        for ig7 in range(ng[7]):
            pos7=self._head+(fw[7]+jw[7]*bl[7])*ig7
            for ig6 in range(ng[6]):
                pos6=pos7+(fw[6]+jw[6]*bl[6])*ig6
                for ig5 in range(ng[5]):
                    pos5=pos6+(fw[5]+jw[5]*bl[5])*ig5
                    for ig4 in range(ng[4]):
                        pos4=pos5+(fw[4]+jw[4]*bl[4])*ig4
                        for ig3 in range(ng[3]):
                            pos3=pos4+(fw[3]+jw[3]*bl[3])*ig3
                            for ig2 in range(ng[2]):
                                pos2=pos3+(fw[2]+jw[2]*bl[2])*ig2
                                for ig1 in range(ng[1]):
                                    pos1=pos2+(fw[1]+jw[1]*bl[1])*ig1
                                    if jw[0] ==1:
                                        seeks.append(pos1)
                                    else:
                                        for ig0 in range(ng[0]):
                                            seeks.append(pos1+(fw[0]+jw[0]*bl[0])*ig0)
        if jw[0]==1:
            return seeks,esize*nw[0],nw[0],len(seeks)==1
        else:
            return seeks,esize,1,False


class In_out:

    def __init__(self,mem_create):
        """Initialize default IO"""
        self._objs={}
        self.append_files={}
        self._memCreate=memCreate
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

            dataFormat = dataFormatType(float32[default], float64,double64,int32,complex64,complex128)

            Option 4 (numpy)
                Provide hyper, ns, os, or ds,label,s axes

        """
        x= self._memCreate(*arg,**kw)
        return x

    def add_storage(self,path, storageObj):
        """Add regFile to list of files

           path - Path to storage
            storageObj - Object to add to list
        """
        self._objs[path]=storageObj
       
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
    
        nw,fw,jw=file.getHyper().get_window_params(**kw)
        aout=[]
        ain=file.get_hyper().axes
        for i in range(len(nw)):
            aout.append(axis(n=nw[i],label=ain[i].label,
             unit=ain[i].unit,d=ain[i].d*jw[i],
             o=ain[i].o+ain[i].d*fw[i]))

        hyperOut =hypercube(axes=aout)

        vec=self._mem_create(hyperOut,data_format=file.get_data_type())
        file.read(vec)
        return vec

    def write_vector(self, path, vec):
        """Write entire sepVector to disk
           path - File to write to
           vec - Vector to write"""
        
        file=self.getRegStorage(vec=vec,path=path)
        file.write(vec)
   
    def write_vectors(self, obj, vecs, ifirst):
        """Write a collection of vectors to a path
                obj - Object to interact with storage
                vecs - Vectors
                ifirst - Position in object for first vector"""
        nw = obj.getHyper().get_ns()
        fw = [0] * len(nw)
        jw = [1] * len(nw)
        nw[len(nw) - 1] = 1
        iouter = ifirst
        for vec in vecs:
            fw[len(fw) - 1] = iouter
            iouter += 1
            obj.write(vec,nw=nw,fw=fw,jw=jw)

    def append_vector(self, path, vec, maxLength=1000, flush=1):
        """Write entire sepVector to disk
           path - File to write to
           vec - Vector to write
           maxLength - Maximum number of appended frames
           flush - How often to flush the files"""
        if path not in self.appendFiles:
            self.appendFiles[path] = AppendFile(
                self, path, vec, maxLength, flush)
        if self.appendFiles[path].addVector(vec):
            vs = self.appendFiles[path].flushVectors()
            if self.appendFiles[path].icount > self.appendFiles[path].nmax:
                self.appendFiles[path].finish()
            self.writeVectors(
                self.appendFiles[path].file,
                vs, self.appendFiles[path].icount - len(vs))

    def close_append_file(self, path):
        """Close an append file and fix the description to the right number of frames"""
        if path not in self.appendFiles:
            self._logger.fatal("No record of appended file")
            raise Exception("")
        vs = self.append_files[path].flushVectors()
        self.write_vectors(
            self.append_files[path].file,
            vs, self.appendFiles[path].icount - len(vs))
        self.append_files[tag].finish(0)



class Append_file:
    """Class for append files"""

    def __init__(self, io,path, vec, maxLength, flush):
        """
                io - IO to use
                path - path to file
                vec - Vector
                maxLength - Maximum number of frames
                flush - How often to flush a file"""
        self.shape = vec.cloneSpace()
        self.vecs = []
        self.flush = flush
        self.hyper = vec.getHyper()
        self.hyper.add_axis(axis(n=maxLength))
        self.nmax = maxLength
        self._data_format=vec.get_data_for,at()
 
        self.file = io.gety_reg_storage(path, hyper=self.hyper, data_format=storage)
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
        self.file.setHyper(self.hyper)
        self.writeDescrption()
        self.nmax = self.icount + iextra
