from abc import ABC, abstractclassmethod


class grid_base(ABC):
    """A Grid like object for accessing irregular dataset"""

    def __init__(self):
        """Initialize the base grid object

        hyper -hypercube
        """
        self._hyper = hyper

    @abstractclassmethod
    def get_grid_block(self, **kw):
        """Get all of the traces withing a given window

        nw - number of elements along each axis
        fw - First sample along each exis
        jw - Skip parameter along each axis
        """

    @abstractclassmethod
    def put_grid_element(self, iloc, trace_nums):
        """Put all the traces correspond to a given grid cell

        iloc - the grid block location to store
        trace_nums - the trace nums in that grid block

        """
