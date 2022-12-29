import sepPython.sepConverter
import sepPython.sepProto
from sepPython.Hypercube as hypercube,axis
import logging

converter=sepPython.sepConverter.converter


class regFile:

    def __init__(self):
        """Default class for a regular file


        """
        self._hyper=None
        self._history=""
        self._dataType=None
        self._progName="Generic Python Program"
        self._IOtype="Unknown"
        self._binaryPath=None
        self._logger=logging.getLogger(None)

    def setLogger(self,logger:logging.Logger):
        """
        Set the logger

        logger - logger to use
        """
        self._logger=logger

    def setHyper(self,hyper:hypercube):
        """
        Set the hypercube for the dataset
        
            hyper- Hypercube for regular dataset
        
        """
        self._hyper=hyper
    
    def getHyper(self)->hypercube:
        """
        Get the hypercube associated with the regular dataset"""
        if self._hyper==None:
            self._logger.fatal("Hypercube has not been set")
        return self._hyper
    
    def setHistory(self,hist:str):
        """
        Set the history assoicated with a dataset

            hist -> String assoicated with the history for the dataset
        """
        self._history=hist

    def setProgName(self,nm:str):
        """
            Set the name of the current program

        """
        self._progName=nm

    def getProgramName(self)->str:
        """
        Return program name
        """
        return self._progName

    def setDataType(self,typ):
        """
            Set the data type for the dataset

            typ - Data type for dataset
        """
        self._dataType=typ

    def getDataType(self)->str:
        """Return the data type assoicated with the file

        """
        if type(self._dataType)==None:
            self._logger.fatal("Datatype has not been set")
            raise Exception("")
        return self._dataType

    def getBinaryPath(self):
        """
        Return the path to the binary
        """
        return self._binaryPath
    
    def setBinaryPath(self,path:str):
        """
        Set the path to the binary
        """
        self._binaryPath=path

    def __repr__(self):
        """Print information about file"""
        x=f"Regular file, type={self._ioType} storage={self.getDataType()} binary={self.getBinaryPath()}\n"
        x+=str(self.getHyper())
        return x

    def remove(self,errorIfNotExists:bool=True):
        """Remove data
        
            errorIfNotExists : Return an error if the blob does not exist
        
        
        """
        self._logger.fatal("Must override remove")
        raise Exception("")

    def getInt(self,param:str,default=None)->int:
        """Return parameter of int
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getInt")
        raise Exception("")

    def getFloat(self,param:str,default=None)->float:
        """Return parameter of type float
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getFloat")
        raise Exception("")

    def getString(self,param:str,default=None)->str:
        """Return parameter of type string
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getString")
        raise Exception("")

    def getInts(self,param:str,default=None)->list:
        """Return parameter of type int arrau
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getInts")
        raise Exception("")

    def getFloats(self,param:str,default=None)->float:
        """Return parameter of float arry
            param - Parameter to retrieve
            default - Default value 
        """
        self._logger.fatal("Must override getFloats")
        raise Exception("")

    def putPar(self,param:str,val):
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

    def hyperToDict(self,myd:dict):
        idim=1
        for ax in self._hyper.axes:
            myd[f"n{idim}"]=ax.n
            myd[f"o{idim}"]=ax.o
            myd[f"d{idim}"]=ax.d
            myd[f"label{idim}"]=ax.label
            myd[f"unit{idim}"]=ax.unit
            idim+=1
        return myd
    
    def hyperToStr(self):
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

    def condense(self,nwIn:list,fwIn:list,jwIn:list)->(list,list,list,list,list):

        """Figure out the best way to write a given request
        
        nwIn - list [int] windowing parameters (number of samples)
        fwIn - list [int] windowing parameters (initial sample)
        jwIn - list [int] skip parameter
        
        Output:
            ngOut,nwOut,fwOut,jwOut,bl
        """
        nwOut=[nwIn[0]]
        ngIn=self._hyper.getNs()
        ngOut=[ngIn[0]]
        fwOut=[fwIn[0]]
        jwOut=[jwIn[0]]
        iout=0
        bl=ngOut[0]
        for i in range(1,len(nwIn)):
            #We have the whole axis
            if nwIn[i]==ngIn[i]:
                nwOut[iout]*=ngIn[i]
                bl*=nwIn[i]
                ngOut[iout]*=ngIn[i]

            #We have a single chunk of th axis
            elif jw[i]==1:
                jwOut[iout]=bl*jwIn[i]
                fwOut[iout]=bl*fwIn[i]
                nwOut[iout]=bl*nwIn[i]
                ngOut[iout]*=ngIn[i]
                if i!=len(nwIn)-1:
                    jwOut.append(1)
                    fWout.append(0)
                    nwOut.append(1)
                    ngOut.append(1)
                    iout+=1
                else:
                    iout+=1
                    fwOut.append(fwIn[i])
                    jwOut.append(jwIn[i])
                    nwOut.append(nwIn[i])
                    ngOut.append(ngIn[i])

        for i in range(len(ngOut),8):
            ngOut.append(1)
            nwOut.append(1)
            fwOut.append(0)
            jwOut.append(1)
        bl=[1]
        for i in range(8):
            bl.append(ngOut[i]*bl[i])
        return ngOut,nwOut,fwOut,jwOut,bl

    def close(self):
        """ Close file"""
        pass

    def loopIt(self,ng:list,nw:list,fw:list,jw:list,bl:list)->list:
        seeks=[]
        esize=converter.getEsize(self.getDataType())

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


class io:

    def __init__(self,memCreate):
        """Initialize default IO"""
        self._objs={}
        self.appendFiles={}
        self._memCreate=memCreate
        self._logger=logging.getLogger(None)
    
    def setLogger(self,log:logging.Logger):
        """

        Set logging for io base

        log - Logging for io

        """
        self._logger=log


    def getRegStorage(self,  **kw):
        """Get object to deal with storage
                Requiered:
                        path - Path to file
                Optional:
                        
        """
        self._logger.fatal("must ovveride getRegFile")
        raise Exception("")

    def getRegVector(self,*arg,**kw):
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

    def addStorage(self,path, storageObj):
        """Add regFile to list of files

           path - Path to storage
            storageObj - Object to add to list
        """
        self._objs[path]=storageObj
       
    def getStorage(self, path):
        """Return object to interact with storage

            path - Tag associated with file
        """
        if path not in self._objs:
            self._logger.fatal("Requested path not loaded into this IO")
            raise Exception("")
        return self._objs[path]
 
    def vectorFromStorage(self, path,  **kw):
        """Get vector from a file and read its contents

           path -Path for vector

           Optional:
           
           args, kw - arguments fo create vector

            

        """
        kw["path"]=path
        file = self.getRegStorage( **kw)
        self.addStorage(path,file)
        self._objs[path] = file
    
        nw,fw,jw=file.getHyper().getWindowParams(**kw)
        aout=[]
        ain=file.getHyper().axes
        for i in range(len(nw)):
            aout.append(axis(n=nw[i],label=ain[i].label,
             unit=ain[i].unit,d=ain[i].d*jw[i],
             o=ain[i].o+ain[i].d*fw[i]))

        hyperOut =hypercube(axes=aout)

        vec=self._memCreate(hyperOut,dataFormat=file.getDataType())
        file.read(vec)
        return vec

    def writeVector(self, path, vec):
        """Write entire sepVector to disk
           path - File to write to
           vec - Vector to write"""
        
        file=self.getRegStorage(vec=vec,path=path)
        file.write(vec)
   
    def writeVectors(self, obj, vecs, ifirst):
        """Write a collection of vectors to a path
                obj - Object to interact with storage
                vecs - Vectors
                ifirst - Position in object for first vector"""
        nw = obj.getHyper().getNs()
        fw = [0] * len(nw)
        jw = [1] * len(nw)
        nw[len(nw) - 1] = 1
        iouter = ifirst
        for vec in vecs:
            fw[len(fw) - 1] = iouter
            iouter += 1
            obj.write(vec,nw=nw,fw=fw,jw=jw)

    def appendVector(self, path, vec, maxLength=1000, flush=1):
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

    def closeAppendFile(self, path):
        """Close an append file and fix the description to the right number of frames"""
        if path not in self.appendFiles:
            self._logger.fatal("No record of appended file")
            raise Exception("")
        vs = self.appendFiles[path].flushVectors()
        self.writeVectors(
            self.appendFiles[path].file,
            vs, self.appendFiles[path].icount - len(vs))
        self.appendFiles[tag].finish(0)



class AppendFile:
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
        self.hyper.addAxis(axis(n=maxLength))
        self.nmax = maxLength
        self._dataFormat=vec.getDataFormat()
 
        self.file = io.getRegStorage(path, hyper=self.hyper, dataFormat=storage)
        self.icount = 0

    def addVector(self, vec):
        """Adds a vector to this of vectors being held"""
        self.vecs.append(vec.clone())
        self.icount += 1
        if len(self.vecs) == self.flush:
            return True
        return False

    def flushVectors(self):
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
