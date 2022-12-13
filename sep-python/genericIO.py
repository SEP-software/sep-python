import pyGenericIO
import SepVector
import SepIrregVector
import Hypercube
import re
storageConvert = {"dataFloat": pyGenericIO.dataType.dataFloat,
                  "dataInt": pyGenericIO.dataType.dataInt,
                  "dataByte": pyGenericIO.dataType.dataByte,
                  "dataComplex": pyGenericIO.dataType.dataComplex,
                  "dataShort": pyGenericIO.dataType.dataShort,

                  "dataDouble": pyGenericIO.dataType.dataDouble,
                  "dataComplexDouble": pyGenericIO.dataType.dataComplexDouble
                  }
usageConvert = {"usageIn": pyGenericIO.usage_code.usageIn,
                "usageOut": pyGenericIO.usage_code.usageOut,
                "usageInOut": pyGenericIO.usage_code.usageInOut,
                "usageScr": pyGenericIO.usage_code.usageScr}

fileType ={
    "invalidFile": pyGenericIO.file_type.invalidFile,
    "regularFile": pyGenericIO.file_type.regularFile,
    "irregularFile": pyGenericIO.file_type.irregularFile}
fileTypeInv = {v: k for k, v in fileType.items()}


ioModes = pyGenericIO.ioModes([""])


class pythonParams:

    def __init__(self, inPars):
        """Create a parameter object that reads from a python dictionary"""
        self.pars = {}
        self.inPars = {}
        self._hyper=None
        if isinstance(inPars, list):
            prse = re.compile("(.+)=(.+)")
            for l in inPars:
                m = prse.search(l)
                if m:
                    self.inPars[m.group(1)] = m.group(2)
        elif isinstance(inPars, dict):
            self.inPars = inPars
        else:
            raise Exception("Must initialize with either list or dictionary")
        for k, v in self.inPars.items():
            found = False
            if isinstance(v, int) or isinstance(
                    v, float) or isinstance(v, str):
                vout = str(v)
                found = True
            elif isinstance(v, list):
                d = None
                found = True
                for l in v:
                    if not d:
                        d = l
                    else:
                        d += "," + l
            if found:
                self.pars[k] = vout
        self.cppMode = pyGenericIO.pythonParams(self.pars)
        ioModes.changeParamObj(self.cppMode)
        defaultIO.setParamObj(self.cppMode)

    def getCpp(self):
        return self.cppMode


class argParseParams:

    def __init__(self, inpars):
        """Create a parameter object that uses info from argsparse module"""
        self.pars = {}
        self.pars["junk"] = "jUnK"
        for k, v in inpars.items():
            if v:
                if isinstance(v, str):
                    self.pars[k] = v
            found = False
            if isinstance(v, int) or isinstance(
                    v, float) or isinstance(v, str):
                vout = str(v)
                found = True
            elif isinstance(v, list):
                d = None
                found = True
                for l in v:
                    if not d:
                        d = l
                    else:
                        d += "," + l
            if found:
                self.pars[k] = vout
        self.cppMode = pyGenericIO.pythonParams(self.pars)
        ioModes.changeParamObj(self.cppMode)
        defaultIO.setParamObj(self.cppMode)


class regFile:

    def __init__(self, ioM, tag, **kw):
        """Get regular file python object
                Requiered:
                    iom  - IO mode
                        tag  - Tag for file
                Optional:
                        fromHyper - Hypercube describing file (also must specify storage)
                        storage  - float,complex,byte,double, or int (defaults to float)
                        fromVector - Vector
                        usage - Defaults to IN for from file OUT for everything else
                        ndims - Minimum dimensions for the file
                        hist  - History to copy to file
        """
        self.tag = tag
        self.usage = None
        ndimMax = -1
        self._type=ioM.getType()
        if "ndims" in kw:
            ndimMax = kw["ndims"]
        if "usage" in kw:
            if kw["usage"] == "INPUT":
                self.usage = "UsageIn"
            elif kw["usage"] == "output":
                self.usage = "usageOut"
            elif kw["usage"] == "inout":
                self.usage = "usageInOut"
            else:
                raise Exception(
                    "Only understand input,output, and inout for usage")
        self.storage = "dataFloat"
        if "storage" in kw:
            if kw["storage"] == "dataFloat":
                self.storage = "dataFloat"
            elif kw["storage"] == "dataInt":
                self.storage = "dataInt"
            elif kw["storage"] == "dataByte":
                self.storage = "dataByte"
            elif kw["storage"] == "dataDouble":
                self.storage = "dataDouble"
            elif kw["storage"] == "dataComplex":
                self.storage = "dataComplex"
            elif kw["storage"] == "dataComplexDouble":
                self.storage = "dataComplexDouble"
            else:
                raise Exception(
                    "Only understand dataFloat,dataInt,dataDouble,dataByte, dataComplexDouble,and dataComplex for storage not=%s"%storage)

        if "fromHyper" in kw:
            if not isinstance(kw["fromHyper"], Hypercube.hypercube):
                raise Exception(
                    "Must pass a hypercube object when creating a file from a hypercube")
            if not self.usage:
                self.usage = "usageOut"
            elif self.usage == "usageIn":
                raise Exception(
                    "Can not have usageIn when creating from Hypercube")
            if isinstance(ioM,io):
                self.cppMode = ioM.cppMode.getReg(
                    self.tag, usageConvert[
                        self.usage], ndimMax)
            else:
                self.cppMode = ioM.getReg(
                    self.tag, usageConvert[
                        self.usage], ndimMax)
            self.cppMode.setHyper(kw["fromHyper"].getCpp())
            self.cppMode.setDataType(storageConvert[self.storage])
            if "hist" in kw:
                self.addHistory(kw["hist"])
            self.cppMode.writeDescription()
        elif "fromVector" in kw:
            if not isinstance(kw["fromVector"], SepVector.vector):
                raise Exception(
                    "When creating a file from a vector must be inherited from SepVector.vector")
            self.storage = kw["fromVector"].getStorageType()
            if not self.usage:
                self.usage = "usageOut"
            elif self.usage == "usageIn":
                raise Exception(
                    "Can not have usageIn when creating from Hypercube")
            
            self.cppMode = ioM.getReg(
                self.tag, usageConvert[
                    self.usage], ndimMax)
            self.cppMode.setHyper(kw["fromVector"].getCpp().getHyper())
            self.cppMode.setDataType(storageConvert[self.storage])
            if "hist" in kw:
                self.addHistory(kw["hist"])
            self.cppMode.writeDescription()
        else:  # Assuming from file
            if not self.usage:
                self.usage = "usageIn"
            elif self.usage == "usageOut":
                raise Exception(
                    "Can not specify usageOut when creating from a file")
            self.cppMode = ioM.getReg(
                self.tag, usageConvert[
                    self.usage], ndimMax)
            self.cppMode.readDescription(ndimMax)
            inv_map = {v: k for k, v in storageConvert.items()}
            self.storage = inv_map[self.cppMode.getDataType()]

    def __repr__(self):
        """Print information about file"""
        x="Regular file type=%s \t "%self._type
        if self.storage=="dataFloat":
            x+="Data type=float\n"
        elif  self.storage=="datInt":
            x+="Data type=integer\n"
        elif  self.storage=="dataByte":
            x+="Data type=byte\n"
        elif  self.storage=="dataDouble":
            x+="Data type=double\n"
        elif  self.storage=="dataComplex":
            x+="Data type=complex\n"
        elif  self.storage=="dataComplexDouble":
            x+="Data type=complexDouble\n"
        else:
            x+="Data type=UKNOWN\n"
        x+="Binary=%s\n"%self.cppMode.getBinary()
        x+=str(self.getHyper())
        return x
    def remove(self):
        """Remove the given dataset"""
        self.cppMode.remove()
    
    def addHistory(self,fle):
        """
            fle - File(s) to read description from
        """
        if isinstance(fle,regFile) or isinstance(fle,irregFile):
            self.cppMode.putDescriptionString(fle.tag,fle.cppMode.getDescriptionString())
        elif isinstance(fle,list):
            for f in fle:
                if isinstance(f,regFile) or isinstance(f,irregFile):
                    self.cppMode.putDescriptionString(f.tag,f.cppMode.getDescriptionString())
        elif isinstance(fle,dict):
            for f,v in fle.items():
                if isinstance(v,regFile) or isinstance(v,irregFile):
                    self.cppMode.putDescriptionString(f,v.cppMode.getDescriptionString())
    def getEsize(self):
        """Return element size"""
        if  self.storage=="dataByte":
           return 1
        elif  self.storage=="dataDouble" or self.storage=="dataComplex":
            return 8
        elif  self.storage=="dataComplexDouble":
            return 16
        else:
            return 4
    def getStorageType(self):
        """Return storage type"""
        return self.storage

    def getInt(self, tag, *arg):
        """Get integer from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getInt(tag, arg[0])
        return self.cppMode.getInt(tag)

    def getFloat(self, tag, *arg):
        """Get a float from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getFloat(tag, arg[0])
        return self.cppMode.getFloat(tag)

    def getBool(self, tag, *arg):
        """Get a boolean from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getBool(tag, arg[0])
        return self.cppMode.getBool(tag)

    def getString(self, tag, *arg):
        """Get a string from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getString(tag, arg[0])
        return self.cppMode.getString(tag)

    def getInts(self, tag, *arg):
        """Get integers from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getInts(tag, arg[0])
        return self.cppMode.getInts(tag)

    def getInts(self, tag, *arg):
        """Get integers from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getInts(tag, arg[0])
        return self.cppMode.getInts(tag)

    def getFloats(self, tag, *arg):
        """Get floats from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getFloats(tag, arg[0])
        return self.cppMode.getFloats(tag)

    def readWindow(self, vec, **kw):
        """Read a window of a file into the vector
                Required:
                  vec - sepVector
                Optional:
                  n,f,j - Standard windowing parameters"""
        if not kw:
            raise Exception("Must supply windowing parameters")
        nw, fw, jw = self.getWindowParam(**kw)
        if not isinstance(vec, SepVector.vector):
            raise Exception("vec must be deriverd SepVector.vector")
        n123 = 1
        for n in nw:
            n123 = n123 * n
        if n123 != vec.getHyper().getN123():
            raise Exception(
                "Data size of vector not the same size as window parameters window=%s vec=%s"%(str(nw),str(vec)))
        elif self.storage == "dataFloat":
            if not isinstance(vec, SepVector.floatVector):
                raise Exception("File type is float vector type is not")
            self.cppMode.readFloatWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataInt":
            if not isinstance(vec, SepVector.intVector):
                raise Exception("File type is int vector type is not")
            self.cppMode.readIntWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataComplex":
            if not isinstance(vec, SepVector.complexVector):
                raise Exception("File type is complex vector type is not")
            self.cppMode.readComplexWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataComplexDouble":
            if not isinstance(vec, SepVector.complexDoubleVector):
                raise Exception("File type is complex double vector type is not")
            self.cppMode.readComplexDoubleWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataByte":
            if not isinstance(vec, SepVector.byteVector):
                raise Exception("File type is nyte vector type is not")
            self.cppMode.readByteWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataDouble":
            if not isinstance(vec, SepVector.doubleVector):
                raise Exception("File type is double vector type is not")
            self.cppMode.readDoubleWindow(nw, fw, jw, vec.cppMode)
        else:
            print("Unknown or unhandled storage type "%self.storage)
    def writeWindow(self, vec, **kw):
        """Write  a window of a file into the vector
                Required:
                  vec - sepVector
                Optional:
                  n,f,j - Standard windowing parameters"""
        nw, fw, jw = self.getWindowParam(**kw)
        if not isinstance(vec, SepVector.vector):
            raise Exception("vec must be deriverd SepVector.vector")
        n123 = 1
        for n in nw:
            n123 = n123 * n
        if n123 != vec.getHyper().getN123():
            raise Exception(
                "Data size of vector not the same size as window parameters")
        elif self.storage == "dataFloat":
            if not isinstance(vec, SepVector.floatVector):
                raise Exception("File type is float vector type is not")
            self.cppMode.writeFloatWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataInt":
            if not isinstance(vec, SepVector.intVector):
                raise Exception("File type is int vector type is not")
            self.cppMode.writeIntWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataComplex":
            if not isinstance(vec, SepVector.complexVector):
                raise Exception("File type is complex vector type is not")
            self.cppMode.writeComplexWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataComplexDouble":
            if not isinstance(vec, SepVector.complexDoubleVector):
                raise Exception("File type is complexDouble vector type is not")
            self.cppMode.writeComplexDoubleWindow(nw, fw, jw, vec.cppMode)        
        elif self.storage == "dataByte":
            if not isinstance(vec, SepVector.byteVector):
                raise Exception("File type is nyte vector type is not")
            self.cppMode.writeByteWindow(nw, fw, jw, vec.cppMode)
        elif self.storage == "dataDouble":
            if not isinstance(vec, SepVector.doubleVector):
                raise Exception("File type is double vector type is not")
            self.cppMode.writeDoubleWindow(nw, fw, jw, vec.cppMode)
        else:
            print("Unknown or unhandled storage type "%self.storage)
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

    def close(self):
        """Close file"""
        self.cppMode.close()

    def putInt(self, tag, arg):
        """Put a float into description"""
        self.cppMode.putInt(tag, arg)

    def putFloat(self, tag, arg):
        """Put a float into description"""
        self.cppMode.putFloat(tag, arg)

    def putString(self, tag, arg):
        """Put a string into description"""
        self.cppMode.putString(tag, arg)

    def putBool(self, tag, arg):
        """Put a boolean into description"""
        self.cppMode.putBool(tag, arg)

    def putFloats(self, tag, arg):
        """Put floats into description"""
        self.cppMode.putFloats(tag, arg)

    def putInts(self, tag, arg):
        """Put ints into description"""
        self.cppMode.putInts(tag, arg)

    def getCpp(self):
        """Return cpp object"""
        return self.cppMode

    def getHyper(self):
        """Return hypercube for the file"""
        return Hypercube.hypercube(hypercube=self.cppMode.getHyper())


class irregFile:

    def __init__(self, ioM, tag, **kw):
        """Get irregular file python object
                Requiered:
                    iom  - IO mode
                        tag  - Tag for file
                Optional:
                        fromHeader - Just headers
                        fromFile -  From another irregFile description
                        storage  - float,complex,byte,double, or int (defaults to float)
                        fromVector - Irregular vector
                        usage - Defaults to IN for from file OUT for everything else
                        dataIn - When creating just a headers dataset must specify irregFile to get data description from
                        inOrder - When creating from a file, whether or not the traces and headers will be in order
                        fromDataHyper - Create from data header also need (keys,headerHyper, and possibly gridHyper)
                        headerHyper - Hypercube for the headers
                        keys - Dictionary of keys and types
                        gridHyper - Hypercube for the grid
                        hist - History to copy into file (regFile,irregFile,list,dict)
        """
        self.tag = tag
        self.usage = None
        self._dataDes=False
        self._headersOnly=False
        self._hyper=None
        ndimMax = -1
        self._type=ioM.getType()
        if "ndims" in kw:
            ndimMax = kw["ndims"]
        if "usage" in kw:
            if kw["usage"] == "INPUT":
                self.usage = "UsageIn"
            elif kw["usage"] == "output":
                self.usage = "usageOut"
            elif kw["usage"] == "inout":
                self.usage = "usageInOut"
            else:
                raise Exception(
                    "Only understand input,output, and inout for usage")
        self.storage = "dataFloat"
        if "storage" in kw:
            if kw["storage"] == "dataFloat":
                self.storage = "dataFloat"
            elif kw["storage"] == "dataInt":
                self.storage = "dataInt"
            elif kw["storage"] == "dataByte":
                self.storage = "dataByte"
            elif kw["storage"] == "dataDouble":
                self.storage = "dataDouble"
            elif kw["storage"] == "dataComplex":
                self.storage = "dataComplex"
            elif kw["storage"] == "dataComplexDouble":
                self.storage = "dataComplexDouble"
            else:
                raise Exception(
                    "Only understand dataFloat,dataInt,dataDouble,dataByte, dataComplexDouble,and dataComplex for storage not=%s"%storage)

        if "fromDataHyper" in kw:
            if not isinstance(kw["fromDataHyper"], Hypercube.hypercube):
                axes=kw["fromDataHyper"].getHyper().axes
                if len(axes) !=2:
                    raise SEPException("Expecting 2-D hypercube")
            if not self.usage:
                self.usage = "usageOut"
            elif self.usage == "usageIn":
                raise Exception(
                    "Can not have usageIn when creating from Hypercube")
            if isinstance(ioM,io):
                self.cppMode = ioM.cppMode.getIrreg(
                    self.tag, usageConvert[
                        self.usage], ndimMax)
            else:
                self.cppMode = ioM.getIrreg(
                    self.tag, usageConvert[
                        self.usage], ndimMax)                
            if "headerHyper" not in kw:
                raise Exception("Must provide hypercube for the headers (headerHyper) when fromDataHyper")
            if not isinstance(kw["headerHyper"],Hypercube.hypercube):
                raise Exception("headerHyper must be a Hypercube")
            if not "keys"  in kw:
                raise Exception("Must provide keys when providing fromDataHyper ")
            if not isinstance(kw["keys"],dict):
                raise Exception("keys must be a dictionary")
            self.cppMode.putHeaderKeyTypes(kw["keys"])
            self.cppMode.putHeaderKeyList(list(kw["keys"]))
            self.cppMode.setHyperHeader(kw["headerHyper"].cppMode)
            self.cppMode.setHyperData(kw["fromDataHyper"].cppMode)
            if "gridHyper" in kw:
                if not isinstance(kw["gridHyper"],Hypercube.hypercube()):
                    raise Exception("gridHyper must be a Hypercube")
                self.cppMode.setHyper(kw["gridHyper"])
                self.cppMode.setHaveGrid(True)
            else:
                self.cppMode.setHyper(kw["headerHyper"].cppMode)
                self.cppMode.setHaveGrid(False)
            self.cppMode.setDataType(storageConvert[self.storage])
            if "hist"  in kw:
                self.addHistory(kw["hist"])
            self.cppMode.writeDescription()
        elif "fromHeader" in kw:
            header=kw["fromHeader"]
            if not "dataIn" in kw:
                raise Exception("Must secify dataIn when creating just headers dataset")
            if not self.usage:
                self.usage = "usageOut"
            elif self.usage == "usageIn":
                raise Exception(
                    "Can not have usageIn when creating from a header")
            self._headersOnly=True
            self.cppMode = ioM.getIrregFile(
                self.tag, usageConvert[
                    self.usage], ndimMax)
            self.copyDataDescription(kw["dataIn"])
            self.cppMode.putHeaderKeyTypes(header.getKeyTypes())
            self.cppMode.putHeaderKeyList(header._keyOrder)
            self._keyOrder=header._keyOrder
            hyp=Hypercube.hypercube(ns=[1000,header._nh])
            self.cppMode.setHyperHeader(hyp.cppMode)
            if header._gridHyper:
                self.cppMode.setHyper(vec.getCpps().getHyper())
            else:
                hyp=Hypercube.hypercube(ns=[len(header._keyOrder),header._nh])
                self.cppMode.setHyper(hyp.cppMode)
                self.cppMode.setHaveGrid(False)
            if header._drn is None:
                self.cppMode.setInOrder(True)
            else:
                self.cppMode.setInOrder(False)
            
            self.cppMode.setDataType(storageConvert[self.storage])
            if "hist"  in kw:
                self.addHistory(kw["hist"])
            self.cppMode.writeDescription()
        elif "fromFile" in kw:
            fle=kw["fromFile"]
            if not self.usage:
                self.usage = "usageOut"
            elif self.usage == "usageIn":
                raise Exception(
                    "Can not have usageIn when creating from a header")    
            ioU=ioM
            if isinstance(ioU,io):
                ioU=ioM.cppMode
            self.cppMode = ioU.getIrregFile(
                self.tag, usageConvert[
                    self.usage], ndimMax)
            if not isinstance(fle,irregFile):
                raise Exception("fromFile must be of type  irregFile")
            if not "inOrder" in kw:
                self.cppMode.setInOrder(fle.cppMode.getInOrder())
            else:
                self.cppMode.setInOrder(kw["inOrder"])
            self.storage=fle.storage
            self.cppMode.setHaveGrid(fle.cppMode.getHaveGrid())
            self.cppMode.putHeaderKeyTypes(fle.cppMode.getHeaderKeyTypes())
            self.cppMode.putHeaderKeyList(fle.cppMode.getHeaderKeyList())
            self._keyOrder=fle.cppMode.getHeaderKeyList()
            self.cppMode.setDataType(storageConvert[self.storage])
            self.cppMode.setHyperData(fle.cppMode.getHyperData())
            self.cppMode.setHaveGrid(fle.cppMode.getHaveGrid())
            self.cppMode.setHyper(fle.cppMode.getHyper())
            self.cppMode.setHyperHeader(fle.cppMode.getHyperHeader())
            if "hist"  in kw:
                self.addHistory(kw["hist"])
            self.cppMode.writeDescription()

        elif "fromVector" in kw:
            vec=kw["fromVector"]
            if not isinstance(kw["fromVector"], SepIrregVector.vector):
                raise Exception(
                    "When creating a file from a vector must be inherited from SepIrregVector.vector")
            self.storage = kw["fromVector"].getStorageType()
            if not self.usage:
                self.usage = "usageOut"
            elif self.usage == "usageIn":
                raise Exception(
                    "Can not have usageIn when creating from Hypercube")
            self.cppMode = ioM.getIrregFile(
                self.tag, usageConvert[
                    self.usage], ndimMax)
            if vec._header._gridHyper:
                self.cppMode.setHaveGrid(True)
            else:
                self.cppMode.setHaveGrid(False)
            mp2={}
            for k,v in vec._header.getKeyTypes().items():
                mp2[k]=str(v)
            self.cppMode.putHeaderKeyTypes(mp2)

            self.cppMode.putHeaderKeyList(vec._header._keyOrder)
            self.cppMode.setHyperData(vec._traces.getHyper().cppMode)
            hyp=Hypercube.hypercube(ns=[1000,vec._header._nh])
            self.cppMode.setHyperHeader(hyp.cppMode)
            self._keyOrder=vec._header._keyOrder

            self.cppMode.setHyper(vec.getHyper().cppMode)
            self.cppMode.setDataType(storageConvert[self.storage])
            if "hist"  in kw:
                self.addHistory(kw["hist"])
            self.cppMode.writeDescription()
        else:  # Assuming from file
            if not self.usage:
                self.usage = "usageIn"
            elif self.usage == "usageOut":
                raise Exception(
                    "Can not specify usageOut when creating from a file")
            ioU=ioM
            if isinstance(ioU,io):
                ioU=ioU.cppMode
            self.cppMode = ioU.getIrregFile(
                self.tag, usageConvert[
                    self.usage], ndimMax)
            self.cppMode.readDescription(ndimMax)
            self._keyOrder=self.cppMode.getHeaderKeyList()
            inv_map = {v: k for k, v in storageConvert.items()}
            self.storage = inv_map[self.cppMode.getDataType()]
    def getDataDescription(self):
        """Get the data description"""
        self._dataDes=self.cppMode.getDataDescription()

    def getHyperHeader(self):
        """Get the hypercube describing the headers"""
        return Hypercube.hypercube(hypercube=self.cppMode.getHyperHeader())

    def getHeaderKeys(self):
        """Return header keys associated with file"""
        return list(self.cppMode.getHeaderKeyTypes().keys())
    def getHeaderKeyTypes(self):
        """Return header key types in a dictionary"""
        return self.cppMode.getHeaderKeyTypes()

    def putDataDescription(self,des):
        """Put a description of the data"""
        self.cppMode.putDataDescrption(des)
        self._dataDesSet=True;

    def copyDataDescription(self,frm):
        """Copy the data description from frm to this file representation"""
        if not isinstance(frm,irregFile):
            raise Exception("Expecting from to be an irregFile got ",type(frm))
        self.cppMode.putDataDescriptionString(frm.cppMode.getDataDescriptionString())
    
    def setDataType(self,typ):
        """Set the data type for vector"""
        self.storage=typ
        self.cppMode.setDataType(storageConvert[self.storage])
    def addHistory(self,fle):
        """
            fle - File(s) to read description from
        """
        if isinstance(fle,regFile) or isinstance(fle,irregFile):
            print("IN ADD HISTORY")
            self.cppMode.putDescriptionString(fle.tag,fle.cppMode.getDescriptionString())
        elif isinstance(fle,list):
            for f in fle:
                if isinstance(f,regFile) or isinstance(f,irregFile):
                    self.cppMode.putDescriptionString(f.tag,f.cppMode.getDescriptionString())
        elif isinstance(fle,dict):
            for f,v in fle.items():
                if isinstance(v,regFile) or isinstance(v,irregFile):
                    self.cppMode.putDescriptionString(f,v.cppMode.getDescriptionString())
    def __repr__(self):
        """Print information about file"""
        x="Irregular file type=%s \t "%self._type
        if self.storage=="dataFloat":
            x+="Data type=float\n"
        elif  self.storage=="datInt":
            x+="Data type=integer\n"
        elif  self.storage=="dataByte":
            x+="Data type=byte\n"
        elif  self.storage=="dataDouble":
            x+="Data type=double\n"
        elif  self.storage=="dataComplex":
            x+="Data type=complex\n"
        elif  self.storage=="dataComplexDouble":
            x+="Data type=complexDouble\n"
        else:
            x+="Data type=UKNOWN\n"
        m=self.cppMode.getHyperData()
        x+=str("Data description\n%s\n"%(Hypercube.hypercube(hypercube=m)))
        order=self.cppMode.getHeaderKeyList()
        typ=self.cppMode.getHeaderKeyTypes()
        x+=str("Headers:\n")
        for k in order:
            if len(k)<8:
                x+="\t%s \t \t \t %s\n"%(k,typ[k])
            elif len(k)<16:
                x+="\t%s \t \t %s\n"%(k,typ[k])
            else:
                x+="\t%s \t  %s\n"%(k,typ[k])

        x+=str("Hypercube for headers:\n%s"%self.getHyper())
        return x
    def remove(self):
        """Remove the given dataset"""
        self.cppMode.remove()
        
    def addHistory(self,fle):
        """
            fle - File(s) to read description from
        """
        if isinstance(fle,regFile) or isinstance(fle,irregFile):
            self.cppMode.putDescriptionString(fle.tag,fle.cppMode.getDescriptionString())
        elif isinstance(fle,list):
            for f in fle:
                if isinstance(f,regFile) or isinstance(f,irregFile):
                    self.cppMode.putDescriptionString(f.tag,f.cppMode.getDescriptionString())
        elif isinstance(fle,dict):
            for f,v in fle.items():
                if isinstance(v,regFile) or isinstance(v,irregFile):
                    self.cppMode.putDescriptionString(f,v.cppMode.getDescriptionString())
    def getEsize(self):
        """Return element size"""
        if  self.storage=="dataByte":
           return 1
        elif  self.storage=="dataDouble" or self.storage=="dataComplex":
            return 8
        elif  self.storage=="dataComplexDouble":
            return 16
        else:
            return 4
    def getStorageType(self):
        """Return storage type"""
        return self.storage

    def getInt(self, tag, *arg):
        """Get integer from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getInt(tag, arg[0])
        return self.cppMode.getInt(tag)

    def getFloat(self, tag, *arg):
        """Get a float from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getFloat(tag, arg[0])
        return self.cppMode.getFloat(tag)

    def getBool(self, tag, *arg):
        """Get a boolean from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getBool(tag, arg[0])
        return self.cppMode.getBool(tag)

    def getString(self, tag, *arg):
        """Get a string from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getString(tag, arg[0])
        return self.cppMode.getString(tag)

    def getInts(self, tag, *arg):
        """Get integers from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getInts(tag, arg[0])
        return self.cppMode.getInts(tag)

    def getInts(self, tag, *arg):
        """Get integers from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getInts(tag, arg[0])
        return self.cppMode.getInts(tag)

    def getFloats(self, tag, *arg):
        """Get floats from a given IO"""
        if(len(arg) == 1):
            return self.cppMode.getFloats(tag, arg[0])
        return self.cppMode.getFloats(tag)


    def readDataWindow(self, **kw):
        """Read a window of a file into the vector
                @returns:
                  vec - sepIrregVector.vector
                Optional:
                  n,f,j - Standard windowing parameters"""


        axes=self.getHyper().axes
        axes[0]=Hypercube.hypercube(hypercube=self.cppMode.getHyperData()).axes[0]
        nw, fw, jw = SepVector.fixWindow(axes,**kw)
        if nw[0] != axes[0].n:
            raise Exception("Right now can no handle windowing the first axis")
        if self.storage == "dataFloat":
            head,v,grid=self.cppMode.readFloatTraceWindow(nw,fw,jw)
            vec=SepVector.floatVector(fromCpp=v)
        elif self.storage == "dataInt":
            head,v,grid=self.cppMode.readFloatTraceWindow(nw,fw,jw)
            vec=SepVector.floatVector(fromCpp=v)
        elif self.storage == "dataComplex":
            head,v,grid=self.cppMode.readComplexTraceWindow(nw,fw,jw)
            vec=SepVector.complexVector(fromCpp=v)
        elif self.storage == "dataComplexDouble":
            head,v,grid=self.cppMode.readComplexDoubleTraceWindow(nw,fw,jw)
            vec=SepVector.complexDoubleVector(fromCpp=v)
        elif self.storage == "dataByte":
            head,v,grid=self.cppMode.readByteTraceWindow(nw,fw,jw)
            vec=SepVector.byteVector(fromCpp=v)
        elif self.storage == "dataDouble":
            head,v,grid=self.cppMode.readDoubleTraceWindow(nw,fw,jw)
            vec=SepVector.doubleVector(fromCpp=v)
        else:
            print("Unknown or unhandled storage type "%self.storage)
        header=self.byte2DToHeader(head,grid=grid)
        return SepIrregVector.vector(traces=vec,header=header)

    def readHeaderWindow(self, **kw):
        """Read a window of a file into the vector
                @returns:
                  vec - sepIrregVector.vector
                Optional:
                  n,f,j - Standard windowing parameters"""
        axes=self.getHyper().axes
        axes[0]=Hypercube.axis(n=10000)
        nw, fw, jw = SepVector.fixWindow(axes,**kw)
        head,drn,grid=self.cppMode.readHeaderWindow(nw,fw,jw)
        header=self.byte2DToHeader(head,grid=grid)
        hyper=Hypercube.hypercube(ns=[1000,header._nh])
        return SepIrregVector.vector(header=header,hyper=hyper)
        
    def writeDataWindow(self, vec, **kw):
        """Write  a window of a file into the vector
                Required:
                  vec - sepVector
                Optional:
                  n,f,j - Standard windowing parameters"""
        
        ax=self.getHyper().axes
        hyp=Hypercube.hypercube(hypercube=self.cppMode.getHyperData())
        axes=[hyp.axes[0]]
        for i in range(1,len(ax)):
            axes.append(ax[i])
        nw, fw, jw = SepVector.fixWindow(axes,**kw)
        if nw[0] != axes[0].n:
            raise Exception("Right now can not handle windowing the first axis")
        
        if not isinstance(vec, SepIrregVector.vector):
            raise Exception("vec must be deriverd SepVector.irregVector")
        head,drn,grid=self.headerToByte2D(vec._header)
        if grid is None:
            self.cppMode.setHaveGrid(False)
        if self.storage == "dataFloat":
            self.cppMode.writeFloatTraceWindow(nw, fw, jw,head.cppMode, vec._traces.cppMode,grid.cppMode)
        elif self.storage == "dataInt":
            self.cppMode.writeIntTraceWindow(nw, fw, jw, head.cppMode,vec._traces.cppMode,grid.cppMode)
        elif self.storage == "dataByte":
            self.cppMode.writeByteTraceWindow(nw, fw, jw,head.cppMode, vec._traces.cppMode,grid.cppMode)
        elif self.storage == "dataShort":
            self.cppMode.writeShortTraceWindow(nw, fw, jw, head.cppMode,vec._traces.cppMode,grid.cppMode)
        elif self.storage == "dataComplex":
            self.cppMode.writeComplexTraceWindow(nw, fw, jw, head.cppMode,vec._traces.cppMode,grid.cppMode)
        elif self.storage == "dataDouble":
            self.cppMode.writeDoubleTraceWindow(nw, fw, jw,head.cppMode, vec._traces.cppMode,grid.cppMode)
        elif self.storage == "datComplexDouble":
            self.cppMode.writeComplexDoubleTraceWindow(nw, fw, jw,head.cppMode, vec._traces.cppMode,grid.cppMode)
    def headerToByte2D(self,headS):
        """Convert headers into a 2-D byte array

            headS - SepIrregVector.vector

            @returns
            header  Byte2DArray
            drn - Data record int1DReg
            grid  - Grid 
            """
        off,sz=self.cppMode.createOffsetMap()
        typ=self.cppMode.getHeaderKeyTypes()
        klast=headS._keyOrder[len(headS._keyOrder)-1]
        n1=off[klast]+4
        if typ[klast]=="dataDouble":
            n1+=4
        elif typ[klast]=="dataShort":
            n1-=2
        elif typ[klast]=="dataByte":
            n1-=3
        head=SepVector.getSepVector(storage="dataByte",ns=[n1,headS._nh])
        for k,v in off.items():     
            x=SepVector.getSepVector(headS._keys[k]._vals)
            self.cppMode.insertValue(head.cppMode,
            x.cppMode,
            off[k],sz[k],n1,headS._nh)
        drn=headS._drn
        if headS._drn is None:
            self.cppMode.setInOrder(True)
            drn=SepVector.getSepVector(storage="dataInt",ns=[1])
        else:
            self.cppMode.setInOrder(False)
        return head,drn,headS.getCreateGrid()
   

    def byte2DToHeader(self,head,drn=None,grid=None):
        """Convert  a 2-D byte array into a header

           return  head - SepIrregVector.vector

            head  Byte2DArray
            drn - Data record int1DReg
            grid - Grid for the data
            
            """
        off,sz=self.cppMode.createOffsetMap()
        typ=self.cppMode.getHeaderKeyTypes()
        headS=SepVector.byteVector(fromCpp=head)
        if grid is not None:
            gridS=SepVector.byteVector(fromCpp=grid)
            header=SepIrregVector.headerBlock(nh=headS.getHyper().getAxis(2).n,grid=gridS,gridHyper=self.getHyper())
        else:
            header=SepIrregVector.headerBlock(nh=headS.getHyper().getAxis(2).n)

        if drn:
            header._drn=int1DVector(fromCpp==drn)
        for k in self._keyOrder:
            v=off[k]
            if typ[k]=="dataByte":
                key=SepVector.byteVector(fromCpp=self.cppMode.extractByte(head,v))
            if typ[k]=="dataShort":
                key=SepVector.shortVector(fromCpp=self.cppMode.extractShort(head,v))
            if typ[k]=="dataInt":
                key=SepVector.intVector(fromCpp=self.cppMode.extractInt(head,v))
            if typ[k]=="dataFloat":
                key=SepVector.floatVector(fromCpp=self.cppMode.extractFloat(head,v))
            if typ[k]=="dataComplex":
                key=SepVector.complexVector(fromCpp=self.cppMode.extractComplex(head,v))
            if typ[k]=="dataDouble":
                key=SepVector.doubleVector(fromCpp=self.cppMode.extractDouble(head,v))
            if typ[k]=="dataComplexDouble":
                key=SepVector.complexDoubleVector(fromCpp=self.cppMode.extractComplexDouble(head,v)) 
            header.addKey(k,vals=key.getNdArray())
        return header
    def writeHeaderWindow(self, vec, **kw):
        """Write  a window of a file into the vector
                Required:
                  vec - sepVector
                Optional:
                  n,f,j - Standard windowing parameters"""
        
        axes=self.getHyper().axes
        axes[0]=Hypercube.axis(n=10000)
        nw, fw, jw = SepVector.fixWindow(axes,**kw)
        if not isinstance(vec, SepIrregVector.headerBlock):
            raise Exception("vec must be deriverd SepVector.header")
        head,drn,grid=self.headerToByte2D(vec)
        self.cppMode.writeHeaderWindow(nw, fw,jw, drn.cppMode,head.cppMode,grid.cppMode)    
         

    def close(self):
        """Close file"""
        self.cppMode.close()

    def putInt(self, tag, arg):
        """Put a float into description"""
        self.cppMode.putInt(tag, arg)

    def putFloat(self, tag, arg):
        """Put a float into description"""
        self.cppMode.putFloat(tag, arg)

    def putString(self, tag, arg):
        """Put a string into description"""
        self.cppMode.putString(tag, arg)

    def putBool(self, tag, arg):
        """Put a boolean into description"""
        self.cppMode.putBool(tag, arg)

    def putFloats(self, tag, arg):
        """Put floats into description"""
        self.cppMode.putFloats(tag, arg)

    def putInts(self, tag, arg):
        """Put ints into description"""
        self.cppMode.putInts(tag, arg)

    def getCpp(self):
        """Return cpp object"""
        return self.cppMode

    def getHyper(self):
        """Return hypercube for the file"""
        return Hypercube.hypercube(hypercube=self.cppMode.getHyper())


class AppendFile:
    """Class for append files"""

    def __init__(self, io, tag, vec, maxLength, flush):
        """io -  IO
                tag - filename
                vec - Vector
                maxLength - Maximum number of frames
                flush - How often to flush a file"""
        self.shape = vec.cloneSpace()
        self.vecs = []
        self.flush = flush
        self.hyper = vec.getHyper()
        self.hyper.addAxis(Hypercube.axis(n=maxLength))
        self.nmax = maxLength
        if isinstance(vec, SepVector.floatVector):
            storage = "float"
        elif isinstance(vec, SepVector.doubleVector):
            storage = "double"
        elif isinstance(vec, SepVector.intVector):
            storage = "int"
        elif isinstance(vec, SepVector.complexVector):
            storage = "complex"
        elif isinstance(vec, SepVector.byteVector):
            storage = "byte"
        self.file = regFile(io, tag, fromHyper=self.hyper, storage=storage)
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
        self.hyper.buildCpp()
        self.file.cppMode.setHyper(self.hyper.cppMode)
        self.file.cppMode.writeDescription()
        self.nmax = self.icount + iextra


class io:

    def __init__(self, *arg, **kw):
        """Initialize IO"""
        if len(arg) > 0:
            self.cppMode = ioModes.getIO(arg[0])
        else:
            self.cppMode = ioModes.getDefaultIO()
        self.param = self.cppMode.getParamObj()
        if "params" in kw:
            if isinstance(kw["params"], dict):
                self.param.addParams(kw["params"])
            elif isinstance(kw["params"], list):
                self.param.addParams(kw["params"])
        self.appendFiles = {}
        self._files = {}

    def getType(self):
        """Return type"""
        return self.cppMode.getType()

    def getType(self,nm):
        """Return fileType"""
        return fileTypeInv[self.cppMode.getFileType(nm)]

    def getInt(self, tag, *arg):
        """Get integer from a given IO"""
        if(len(arg) == 1):
            return self.param.getInt(tag, arg[0])
        return self.param.getInt(tag)

    def getFloat(self, tag, *arg):
        """Get a float from a given IO"""
        if(len(arg) == 1):
            return self.param.getFloat(tag, arg[0])
        return self.param.getFloat(tag)

    def getBool(self, tag, *arg):
        """Get a boolean from a given IO"""
        if(len(arg) == 1):
            return self.param.getBool(tag, arg[0])
        return self.param.getBool(tag)

    def getString(self, tag, *arg):
        """Get a string from a given IO"""
        if(len(arg) == 1):
            return self.param.getString(tag, arg[0])
        return self.param.getString(tag)

    def getInts(self, tag, *arg):
        """Get integers from a given IO"""
        if(len(arg) == 1):
            return self.param.getInts(tag, arg[0])
        return self.param.getInts(tag)

    def getInts(self, tag, *arg):
        """Get integers from a given IO"""
        if(len(arg) == 1):
            return self.param.getInts(tag, arg[0])
        return self.param.getInts(tag)

    def getFloats(self, tag, *arg):
        """Get floats from a given IO"""
        if(len(arg) == 1):
            return self.param.getFloats(tag, arg[0])
        return self.param.getFloats(tag)

    def getRegFile(self, tag, **kw):
        """Get regular file python object
                Requiered:
                        tag  - Tag for file
                Optional:
                        fromHyper - Hypercube describing file (also must specify storage)
                        storage  - float,complex,byte,double, or int (defaults to float)
                        fromVector - Vector
                        ndims   - Minimum dimension of file
        """
        x = regFile(self.cppMode, tag, **kw)
        self._files[tag] = x
        return x
    def getIrregFile(self, tag, **kw):
        """Get irregular file python object
                Requiered:
                        tag  - Tag for file
                Optional:
                        fromDataHyper - Hypercube describing file (also must specify storage)
                        storage  - float,complex,byte,double, or int (defaults to float)
                        fromVector -Irregular vector
                        fromHeader - Header 
        """
        x = irregFile(self.cppMode, tag, **kw)
        self._files[tag] = x
        return x
    def getFile(self, tag):
        """Return file assumed it has been read through io

            tag - Tag associated with file
        """
        if tag not in self._files:
            raise Exception("Requested tag not loaded into this IO")
        return self._files[tag]

    def getIrregVector(self,tag,**kw):
        """Get vector from a file and read its contents
           Optional
             """
        #NEED TO ADD STUFF HERE
        #ADD ADDD AAAAAA
        file = self.getIrregFile(tag, **kw)
        self._files[tag] = file
        hyper = file.getHyper()
        nw = file.getHyper().getNs()
        fw = [0] * len(nw)
        jw = [1] * len(nw)
        vec = SepVector.getSepVector(hyper, storage=file.storage)
        if file.storage == "dataFloat":
    
            file.getCpp().readFloatWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplex":
            file.getCpp().readComplexWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplexDouble":
            file.getCpp().readComplexDoubleWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataByte":
            file.getCpp().readByteWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataInt":
            file.getCpp().readIntWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataDouble":
            file.getCpp().readDoubleWindow(nw, fw, jw, vec.getCpp())
        file.close()
        return vec

    def getVector(self, tag, **kw):
        """Get vector from a file and read its contents
           Optional
             ndims - Force the hypercube to at least ndim axes"""
        file = self.getRegFile(tag, **kw)
        self._files[tag] = file
        hyper = file.getHyper()
        nw = file.getHyper().getNs()
        fw = [0] * len(nw)
        jw = [1] * len(nw)

        vec = SepVector.getSepVector(hyper, storage=file.storage)
        if file.storage == "dataFloat":
            file.getCpp().readFloatWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplex":
            file.getCpp().readComplexWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplexDouble":
            file.getCpp().readComplexDoubleWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataByte":
            file.getCpp().readByteWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataInt":
            file.getCpp().readIntWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataDouble":
            file.getCpp().readDoubleWindow(nw, fw, jw, vec.getCpp())
        file.close()
        return vec

    def writeVector(self, tag, vec):
        """Write entire sepVector to disk
           tag - File to write to
           vec - Vector to write"""
        file = regFile(self.cppMode, tag, fromVector=vec)
        nw = file.getHyper().getNs()
        fw = [0] * len(nw)
        jw = [1] * len(nw)
        if file.storage == "dataFloat":
            file.getCpp().writeFloatWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplex":
            file.getCpp().writeComplexWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplexDouble":
            file.getCpp().writeComplexDoubleWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataByte":
            file.getCpp().writeByteWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataInt":
            file.getCpp().writeIntWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataDouble":
            file.getCpp().writeDoubleWindow(nw, fw, jw, vec.getCpp())
    def writeIrregVector(self, tag, vec):
        """Write entire sepVector to disk
           tag - File to write to
           vec - Vector to write"""
        #ADD ADD AAAAAAAAAAAA
        file = regFile(self.cppMode, tag, fromVector=vec)
        nw = file.getHyper().getNs()
        fw = [0] * len(nw)
        jw = [1] * len(nw)
        if file.storage == "dataFloat":
            file.getCpp().writeFloatWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplex":
            file.getCpp().writeComplexWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataComplexDouble":
            file.getCpp().writeComplexDoubleWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataByte":
            file.getCpp().writeByteWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataInt":
            file.getCpp().writeIntWindow(nw, fw, jw, vec.getCpp())
        elif file.storage == "dataDouble":
            file.getCpp().writeDoubleWindow(nw, fw, jw, vec.getCpp())
    def writeVectors(self, file, vecs, ifirst):
        """Write a collection of vectors to a file
                file - regFile
                vecs - Vectors
                ifirst - Position in file for first vector"""
        nw = file.getHyper().getNs()
        fw = [0] * len(nw)
        jw = [1] * len(nw)
        nw[len(nw) - 1] = 1
        iouter = ifirst
        for vec in vecs:
            fw[len(fw) - 1] = iouter
            iouter += 1
            if file.storage == "dataFloat":
                file.getCpp().writeFloatWindow(nw, fw, jw, vec.getCpp())
            elif file.storage == "dataComplex":
                file.getCpp().writeComplexWindow(nw, fw, jw, vec.getCpp())
            elif file.storage == "dataComplexDouble":
                file.getCpp().writeComplexDoubleWindow(nw, fw, jw, vec.getCpp())
            elif file.storage == "dataByte":
                file.getCpp().writeByteWindow(nw, fw, jw, vec.getCpp())
            elif file.storage == "dataInt":
                file.getCpp().writeIntWindow(nw, fw, jw, vec.getCpp())
            elif file.storage == "dataDouble":
                file.getCpp().writeDoubleWindow(nw, fw, jw, vec.getCpp())

    def appendVector(self, tag, vec, maxLength=1000, flush=1):
        """Write entire sepVector to disk
           tag - File to write to
           vec - Vector to write
           maxLength - Maximum number of appended frames
           flush - How often to flush the files"""
        if tag not in self.appendFiles:
            self.appendFiles[tag] = AppendFile(
                self.cppMode, tag, vec, maxLength, flush)
        if self.appendFiles[tag].addVector(vec):
            vs = self.appendFiles[tag].flushVectors()
            if self.appendFiles[tag].icount > self.appendFiles[tag].nmax:
                self.appendFiles[tag].finish()
            self.writeVectors(
                self.appendFiles[tag].file,
                vs, self.appendFiles[tag].icount - len(vs))

    def closeAppendFile(self, tag):
        """Close an append file and fix the description to the right number of frames"""
        if tag not in self.appendFiles:
            raise Exception("No record of appended file")
        vs = self.appendFiles[tag].flushVectors()
        self.writeVectors(
            self.appendFiles[tag].file,
            vs, self.appendFiles[tag].icount - len(vs))
        self.appendFiles[tag].finish(0)

    def setParamObj(self, par):
        """Set parameter object"""
        self.param = par
defaultIO = io()
