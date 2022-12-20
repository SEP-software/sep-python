import sepIO
import sepVector


class modes:
    """Class for selecting modes"""
    def __init__(self):
       self._modes=[]

    def addMode(self,name,ioP,memC):
        """
        Add a new mode 

        name - Name for the mode
        ioP -  Pointer to the IO Class
        memC - Pointer on how to create memory
        """
        self._modes[name]=ioP(memC)

    def getMode(self,typ):
        """Get a specific io mode"""
        if typ not in self._modes:
            raise Exception(f"Unknown mode {typ}")
        return self._modes[typ]
    
    def getModes(self):
        """Return all available modes"""
        return self._modes.keys()


ioModes=modes()
ioModes.addMode("sepDefault",sepIO,sepVector.getSepVector)
defaultIO=ioMode("sepDefault")