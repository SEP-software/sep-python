from sep_python._base_grid import grid_base
from sep_python._io_base import RegFile
from math import prod
import numpy as np


class sep_grid(grid_base):
    """A grid SEP old-style trace_index, and -1"""

    def __init__(self, file_obj):
        """Initialize an abstract SEP grid object
        file_obj - A regular file object containing the grid
        """
        if not isinstance(file_obj, RegFile):
            raise Exception("Expecting file_obj to be inherited from RegFile")
        super().___init__(file_obj.get_hyper())
        self._reg_file = file_obj
        self._extend = False
        if self.get_hyper().axes[0].label == "trace_in_bin":
            self._extend = True

    def get_grid_block(self, **kw):
        """Get all of the traces withing a given window

        nw - number of elements along each axis
        fw - First sample along each exis
        jw - Skip parameter along each axis
        """
        nw, fw, jw = self._hyper.window_pars(**kw)
        if self._extend:
            nw.insert(0, self._trace_in_bin)
            fw.insert(0, 0)
            jw.insert(0, 1)
        tmp = np.array((prod(nw),), dtype=np.int32)
        self._reg_file.read(tmp, nw=nw, fw=fw, jw=jw)
        return np.delete(tmp, np.where(tmp == -1))

    def put_grid_element(self, iloc, trace_nums):
        """Put all the traces correspond to a given grid cell

        iloc - the grid block location to store
        trace_nums - the trace nums in that grid block

        """
        if self._extend:
            fw, jw, nw = [self.get_hyper().axes[0].n], [1], [1]
        else:
            fw = [], jw = [], nw = []
        fw += iloc
        jw += [1] * len(iloc)
        nw += [1] * len(iloc)
        self._reg_file.write(tmp, nw=nw, fw=fw, jw=jw)
