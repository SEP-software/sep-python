import pandas as pd


class headers:
    """Basic headers class using a pandas frame as its bas class"""

    def __init__(self, frame):
        """Initialize a pandas data frame

        frame - Pandas frame

        """
        self._frame = frame
        self.check_valid()

    def get_pandas(self):
        """Return pandas frame representation"""
        return self._frame

    def check_valid(self):
        """Check to make sure that we have a valid pandas frame"""
        cols = self._frame.cols()
        for ikey, key in enumerate(cols):
            if ikey == 0:
                ntraces = self._frame[key].shape[0]
            elif ntraces != self._frame[key].shape[0]:
                raise Exception("Number of rows not consistent")

    def add_key(self, keyname, vals):
        if len(vals) != self._frame[self._frame.cols[0]].shape[0]:
            raise Exception("Number of headers does not match stored frame")
        self._frame[keyname] = vals

    def get_key(self, keyname):
        if keyname not in self._frame.cols:
            raise Exception(f"Key {keyname} does not exist")
        return self._frame[keyname]
