from sep_python._base_grid import grid_base
from sep_python._io_base import RegFile
from math import prod
import numpy as np


class database_grid(grid_base):
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


import sqlite3
import numpy as np

# Connect to SQLite database in memory
conn = sqlite3.connect(":memory:")
c = conn.cursor()

# Create a table
c.execute(
    """CREATE TABLE grid
             (x INTEGER, y INTEGER, array BLOB)"""
)

# Create numpy arrays
array1 = np.array([1, 2, 3, 4])
array2 = np.array([5, 6, 7, 8])

# Convert numpy arrays to bytes
array1_bytes = array1.tostring()
array2_bytes = array2.tostring()

# Insert numpy array bytes into the table
c.execute("INSERT INTO grid VALUES (?, ?, ?)", (0, 0, array1_bytes))
c.execute("INSERT INTO grid VALUES (?, ?, ?)", (1, 1, array2_bytes))

# Commit the changes
conn.commit()

# Define the range of x and y values you are interested in
x_range = (0, 1)
y_range = (0, 1)

# Retrieve the array bytes from the table within the specified range
c.execute(
    "SELECT array FROM grid WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ?",
    (*x_range, *y_range),
)
array_bytes_retrieved = c.fetchall()

# Loop over the results and convert each blob back to a numpy array
arrays_retrieved = [
    np.frombuffer(blob[0], dtype=array1.dtype) for blob in array_bytes_retrieved
]

# Print the retrieved arrays
for array in arrays_retrieved:
    print(array)
