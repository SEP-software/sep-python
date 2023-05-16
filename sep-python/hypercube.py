"""    Module for describing regular spaces """
import logging


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
            return f"n={self.n}\to={self.o}\td={self.d}\tlabel={self.label}\tunit={self.unit}"
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

        self.axes = axes

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
        for i in range(len(self.axes), len(hyper.axes)):
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

    def get_window_params(self, **kw):
        """Return window parameters
        must supply n,f,or j"""
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
        if "j" in kw:
            js = kw["j"]
        else:
            js = [1] * ndim
        if "f" in kw:
            fs = kw["f"]
            for ival, fval in enumerate(fs):
                if fval >= self.axes[ival].n:
                    logging.getLogger(None).fatal(
                        "Invalid f parameter f %d>=ndata(%d) for axis {i+1}",
                        fval,
                        self.axes[ival].n,
                    )
                    raise Exception(
                        "Invalid f parameter f"
                        + f"({fval})>=ndata({self.axes[ival].n}) for axis {ival+1}"
                    )

        else:
            fs = [0] * ndim
        if "n" in kw:
            ns = kw["n"]
            for i in range(len(fs)):
                if ns[i] > self.axes[i].n:
                    logging.getLogger(None).fatal(
                        "Invalid n parameter n(%d) > ndata(%d) for axes %d",
                        ns[i],
                        self.axes[i].n,
                        i + 1,
                    )
                    raise Exception(
                        f"Invalid n parameter n({ns[i]}) > ndata({self.axes[i].n}) for axes {i+1}"
                    )
        else:
            ns = []
            for i in range(ndim):
                ns.append(int((self.axes[i].n - 1 - fs[i]) / js[i] + 1))
        for i in range(ndim):
            if self.axes[i].n < (1 + fs[i] + js[i] * (ns[i] - 1)):
                logging.getLogger(None).fatal(
                    "Invalid window parameter (outside axis range)"
                    + f"f={fs[i]} j={js[i]} n={ns[i]} iax={i+1} ndata={self.axes[i].n}"
                )
                raise Exception(
                    "Invalid window parameter (outside axis range) "
                    + f"f={fs[i]} j={js[i]} n={ns[i]} iax={i+1} ndata={self.axes[i].n}"
                )
        return ns, fs, js
