"""    Module for describing regular spaces """
import logging
import math
import copy
import numpy as np


class Axis:
    """Describe a regular sampled axis"""

    def __init__(self, **kw):
        """Axis
        defaults to n=1, o=0., d=1.
        """
        self.n = 1
        self.o = 0.0
        self.d = 1.0
        self.label = ""
        self.unit = ""
        if "n" in kw:
            self.n = int(kw["n"])
        if "o" in kw:
            self.o = float(kw["o"])
        if "d" in kw:
            self.d = float(kw["d"])
        if "label" in kw:
            self.label = str(kw["label"])
        if "unit" in kw:
            self.unit = str(kw["unit"])
        if "axis" in kw:
            self.n = kw["axis"].n
            self.o = kw["axis"].o
            self.d = kw["axis"].d
            self.label = kw["axis"].label
            self.unit = kw["axis"].unit

    def __repr__(self):
        """Define print method for class"""
        if self.unit != "":
            mystr = f"n={self.n}\to={self.o}\td={self.d}\t"
            mystr += f"label={self.label}\tunit={self.unit}"
            return mystr
        elif self.label != "":
            return f"n={self.n}\to={self.o}\td={self.d}\tlabel={self.label}"
        else:
            return f"n={self.n}\to={self.o}\td={self.d}"


class Hypercube:
    """A class defining a regular sampled n-dimensional cube"""

    def __init__(self, axes: list):
        """Initialize a hyperube

        Args:
            axes (List): List of axes

        Returns:
            _type_: Hyperube
        """

        self.axes = copy.deepcopy(axes)

    @classmethod
    def set_with_ns(cls, ns: list, **kw):
        """Set hypercube up using ns,os,ds, labels, units

        Args:
            ns (List[int]): Size of axes

          Optional:
            os (List[float]): Origin of axes
            ds (List[float]): Sampling of axes
            labels (List(string): Label for axes
            units (List(string)): Units for axes

        """

        axes = []
        for n in ns:
            axes.append(Axis(n=n))

        if "os" in kw:
            for i in range(len(kw["os"]) - len(axes)):
                axes.append(Axis(n=1))
            for i in range(len(kw["os"])):
                axes[i].o = kw["os"][i]
        if "ds" in kw:
            for i in range(len(kw["ds"]) - len(axes)):
                axes.append(Axis(n=1))
            for i in range(len(kw["ds"])):
                axes[i].d = kw["ds"][i]
        if "labels" in kw:
            for i in range(len(kw["labels"]) - len(axes)):
                axes.append(Axis(n=1))
            for i in range(len(kw["labels"])):
                axes[i].label = kw["labels"][i]
        if "units" in kw:
            for i in range(len(kw["units"]) - len(axes)):
                axes.append(Axis(n=1))
            for i in range(len(kw["units"])):
                axes[i].unit = kw["units"][i]
        return Hypercube(axes)

    def clone(self):
        """Clone hypercube"""
        return Hypercube(axes=self.axes)

    def sub_cube(self, nwind, fwind, jwind):
        """Return a sub-cube"""
        axes = []
        for iaxis, axis in enumerate(self.axes):
            axes.append(
                Axis(
                    n=nwind[iaxis],
                    o=axis.o + axis.d * fwind[iaxis],
                    d=axis.d * jwind[iaxis],
                    label=axis.label,
                    unit=axis.unit,
                )
            )
        return Hypercube(axes=axes)

    def get_ndim(self):
        """Return the number of dimensions"""
        return len(self.axes)

    def get_axis(self, i):
        """Return an axis"""
        return self.axes[i - 1]

    def get_n123(self):
        """Get the number of elements"""
        n123 = 1
        for ax in self.axes:
            n123 = n123 * ax.n
        return n123

    def get_ns(self):
        """Get a list of the sizes of the axes"""
        ns = []
        for axis in self.axes:
            ns.append(axis.n)
        return ns

    def check_same(self, hyper):
        """CHeck to see if hypercube is the same space"""
        for i in range(max(len(self.axes), len(hyper.axes))):
            if i < len(self.axes) and i < len(hyper.axes):
                if self.axes[i].n != hyper.axes[i].n:
                    return False
                elif (
                    abs(
                        (self.axes[i].o - hyper.axes[i].o)
                        / max(0.001, abs(self.axes[i].o))
                    )
                    > 0.001
                ):
                    return False
                elif (
                    abs(
                        (self.axes[i].d - hyper.axes[i].d)
                        / max(0.001, abs(self.axes[i].d))
                    )
                    > 0.001
                ):
                    return False
            elif i < len(self.axes):
                if self.axes[i].n != 1:
                    return False
            elif hyper.axes[i].n != 1:
                return False
        return True

    def __repr__(self):
        """Define print method for hypercube class"""
        str_out = ""
        for iax, axis in enumerate(self.axes):
            str_out += f"Axis {iax+1}: {axis}\n"
        return str_out

    def add_axis(self, axis):
        """Add an axis to the hypercube"""
        self.axes.append(axis)

    def calc_axis(iax, ax, **kw):
        """Given  windowing params for axis return n,f,w"""
        ng = ax.n
        f, j = 0, 1
        if f"j{iax+1}" in kw:
            j = kw[f"j{iax+1}"]
        if "j" in kw:
            j = kw["j"][iax]

        if f"min{iax+1}" in kw:
            f = math.floor((kw[f"min{iax+1}"] - ax.o) / ax.d + 0.5)
            if f < 0 or f > ng - 1:
                raise Exception(f"Illegal min param axis {iax}")
        if f"f{iax+1}" in kw:
            f = kw[f"f{iax+1}"]
            if f < 0 or f > ng - 1:
                raise Exception(f"Illegal min param axis {iax}")
        if "f" in kw:
            f = kw["f"][iax]
        n = math.ceil((ng - f) / j)
        if f"max{iax+1}" in kw:
            ncalc = int((kw[f"max{iax+1}"] - ax.o) / ax.d)
            if ncalc > ng:
                raise Exception(f"Illegal max {iax+1}")
            n = int((ncalc - 1) / j + 1)
        if f"n{iax+1}" in kw:
            n = kw[f"n{iax+1}"]
        if "n" in kw:
            n = kw["n"][iax]
        if ng <= f + j * (n - 1):
            raise ValueError(
                f"Illegal window options for axis {iax} n={n} f={f} j={j} ng={ng}"
            )
        return n, f, j

    def get_window_params(self, **kw):
        """Return window parameters
        must supply n=[],f,or j,
        or f1,f2,f3 j1,j2,j3, n1, n2, n3
        or min=[], max=[],
        or min1,min2,min3 or max1, max2, max3

        Overrides: f->f1->min->min1
        """
        ndim = len(self.axes)
        for par in ["f", "j", "n"]:
            if par in kw:
                if not isinstance(kw[par], list):
                    logging.getLogger(None).fatal(
                        "Expecting %s to be a list the dimensions of your Path", par
                    )
                    raise Exception(
                        f"Expecting {par} to be a list the dimensions of your Path"
                    )
                if len(kw[par]) != ndim:
                    logging.getLogger(None).fatal(
                        "Expecting %s to be a list the dimensions of your Path", par
                    )
                    raise Exception(
                        f"Expecting {par} to be a list the dimensions of your Path"
                    )
        ns, fs, js = [], [], []
        for iax in range(ndim):
            n, f, j = Hypercube.calc_axis(iax, self.axes[iax], **kw)
            ns.append(n)
            fs.append(f)
            js.append(j)
        return ns, fs, js

    def calc_sub_window(self, hyper_out):
        """Calculate the window to transform window in to window out

        hyper_out - Hypercubes describing input/output

        """
        fs = []
        js = []
        ns = []
        for ax_i, ax_o in zip(self.axes, hyper_out.axes):
            js.append(int((ax_o.d / ax_i.d) + 0.5))
            fs.append(int((ax_o.o - ax_i.o) / ax_i.d + 0.5))
            ns.append(int(ax_o.n))
        return {"n": ns, "f": fs, "j": js}

    def trace_locations(self, n, f, j):
        """
        Calculate the beginning location of each trace (2:) described by

            n - number of samples
            f - first sample along each axis
            j - sampling along each axis
        """

        if len(self.axes) == 1:
            return np.zeros((1, 1))
        coords = [
            np.arange(f_i, f_i + n_i * j_i, j_i)
            for f_i, n_i, j_i in zip(f[1:], n[1:], j[1:])
        ]

        # Create a grid of indices for multidimensional indexing
        grid = np.meshgrid(*coords, indexing="ij")

        # Flatten and stack the grid to get a 2D array of indices
        return np.stack([g.ravel() for g in grid], axis=-1)

    def reverse_hypercube(self):
        """
        Reverse hypercube axes (useful to switch between numpy rep and SEPlib style)
        
        
        
        """
        return Hypercube(axes=self.axes[::-1])

    def compact_hyper(
        self, with_os=True, with_ds=True, with_label=True, with_unit=True
    ):
        """Compact a hypercube remove all outer axes that are default values

        with_os - remove if o=0
        with_ds - remove if d=1
        with_label - remove if label=""
        with_unit - remove if unit=""
        """
        default_shape = []
        for ax in self.axes:
            defs = True
            if ax.n != 1:
                defs = False
            elif ax.o != 0.0 and with_os:
                print("Hello")
                defs = False
            elif ax.d != 1.0 and with_ds:
                defs = False
            elif ax.label != "" and with_label:
                defs = False
            elif ax.unit != "" and with_unit:
                defs = False
            default_shape.append(defs)
        print(default_shape)
        true_index = default_shape.index(True)
        print(true_index)
        if true_index != -1:
            if true_index == 0:
                raise Exception("Found no valid axes???")
            return Hypercube(self.axes[:true_index])
        return self
    
    def concatenate_hypercubes(self, other_hyper, axis):
        """
        Concatenates two hypercubes along the specified axis.

        Args:
            other_hyper (Hypercube): The hypercube to be concatenated.
            axis (int): The axis along which to concatenate the hypercubes.

        Returns:
            Hypercube: The concatenated hypercube.

        Raises:
            ValueError: If no axis is specified or if the hypercubes are not compatible for concatenation.
        """

        if axis is None:
            raise ValueError("No axis specified for concatenation.")

        if len(self.axes) != len(other_hyper.axes):
            raise ValueError("Hypercubes must have the same number of axes for concatenation.")

        if axis < 0 or axis >= len(self.axes):
            raise ValueError("Invalid axis specified for concatenation.")

        for i in range(1, len(self.axes)):
            if i != axis and self.axes[i] != other_hyper.axes[i]:
                raise ValueError("Hypercubes must have compatible axes for concatenation.")

        new_axes = self.axes.copy()
        new_axes[axis].n += other_hyper.axes[axis].n

        return Hypercube(new_axes)
 