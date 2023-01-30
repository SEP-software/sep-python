"""

    Module to select mode of operation (what vector and what io to use)


"""
import logging
import sep_python.sep_io
import sep_python.sep_vector


class Modes:
    """Class for selecting modes"""

    def __init__(self):
        self._modes = {}
        self._logger = logging.getLogger(None)

    def set_logger(self, logger: logging.Logger):
        """

        Set the logger for mode

        logger

        """
        self._logger(logger)

    def add_mode(self, name, io_pointer, mem_creator):
        """
        Add a new mode

        name - Name for the mode
        io_pointer -  Pointer to the IO Class
        mem_creator - Pointer on how to create memory
        """
        self._modes[name] = io_pointer(mem_creator)

    def get_mode(self, typ):
        """Get a specific io mode"""
        if typ not in self._modes:
            self._logger.fatal("Unknown mode %s", typ)
            raise Exception("")
        return self._modes[typ]

    def get_modes(self):
        """Return all available modes"""
        return self._modes.keys()


ioModes = Modes()

ioModes.add_mode(
    "sepDefault", sep_python.sep_io.InOut, sep_python.sep_vector.get_sep_vector
)
default_io = ioModes.get_mode("sepDefault")
