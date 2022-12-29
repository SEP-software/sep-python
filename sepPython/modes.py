import sepPython.sepIO
import sepPython.SepVector
import logging


class modes:
    """Class for selecting modes"""
    def __init__(self):
       self._modes={}
       self._logger=logging.getLogger(None)


    def setLogger(self,logger:logging.Logger):
        """

        Set the logger for mode

        logger 

        """
        self._logger(logger)


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
            self._logger.fatal(f"Unknown mode {typ}")
            raise Exception("")
        return self._modes[typ]
    
    def getModes(self):
        """Return all available modes"""
        return self._modes.keys()


ioModes=modes()
ioModes.addMode("sepDefault",sepPython.sepIO.io,sepPython.SepVector.getsepPython.SepVector)
defaultIO=ioModes.getMode("sepDefault")