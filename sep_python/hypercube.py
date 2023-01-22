"""    Module for describing regular spaces """

import logging
from typing import List

class Axis:

    def __init__(self, **kw):
        """Axis
                defaults to n=1, o=0., d=1.
        """
        self.n = 1
        self.o = 0.
        self.d = 1.
        self.label = ""
        self.unit = ""
        if "n" in kw:
            self.n =int( kw["n"])
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
        if self.unit!="":
            return "n=%d\to=%f\td=%f\tlabel=%s\tunit=%s"%(self.n,self.o,self.d,self.label,self.unit)
        elif self.label!="":
            return "n=%d\to=%f\td=%f\tlabel=%s"%(self.n,self.o,self.d,self.label)
        else:
            return "n=%d\to=%f\td=%f"%(self.n,self.o,self.d)



class Hypercube:

    def __init__(self, axes: list):
        """Initialize a hyperube

        Args:
            axes (List): List of axes

        Returns:
            _type_: Hyperube 
        """
        
        self.axes=axes
        

    @classmethod
    def set_with_ns(cls,ns:list,**kw):
        """ Set hypercube up using ns,os,ds, labels, units

        Args:
            ns (List[int]): Size of axes
          
          Optional:
            os (List[float]): Origin of axes
            ds (List[float]): Sampling of axes
            labels (List(string): Label for axes
            units (List(string)): Units for axes

        """

        axes=[]
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
    
    def sub_cube(self,nwind,fwind,jwind):
        """Return a sub-cube"""
        axes=[]
        for i in range(len(self.axes)):
            axes.append(Axis(n=nwind[i],o=self.axes[i].o+self.axes[i].d*fwind[i],d=self.axes[i].d*jwind[i],label=self.axes[i].label,unit=self.axes[i].unit))
        return Hypercube(axes=axes)

    def get_ndim (self):
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
        for a in self.axes:
            ns.append(a.n)
        return ns

    def check_same(self,hyper):
        """CHeck to see if hypercube is the same space"""
        same=True
        for i in range(len(self.axes),len(hyper.axes)):
            if i < len(self.axes) and i < len(hyper.axes):
                if self.axes[i].n != hyper.axes[i].n:
                    return False
                elif abs((self.axes[i].o-hyper.axes[i].o)/max(.001,abs(self.axes[i].o))) > .001:
                    return False
                elif abs((self.axes[i].d-hyper.axes[i].d)/max(.001,abs(self.axes[i].d))) >.001:
                    return False
            elif i < len(self.axes):
                if self.axes[i].n !=1:
                    return False
            elif hyper.axes[i].n !=1:
                return False
        return True

    def __repr__(self):
        """Define print method for hypercube class"""
        x=""
        for i in range(len(self.axes)):
            x+="Axis %d: %s\n"%(i+1,str(self.axes[i]))
        return x

    def add_axis(self, axis):
        """Add an axis to the hypercube"""
        self.axes.append(axis)

    def get_window_params(self, **kw):
        """Return window parameters
                must supply n,f,or j"""
        ndim = len(self.axes)
        for a in ["f", "j", "n"]:
            if a in kw:
                if not isinstance(kw[a], list):
                    logging.getLogger(None).fatal( f"Expecting {a} to be a list the dimensions of your Path" )
                    raise Exception(f"Expecting {a} to be a list the dimensions of your Path")
                if len(kw[a]) != ndim:
                    logging.getLogger(None).fatal(f"Expecting {a} to be a list the dimensions of your Path")
                    raise Exception(f"Expecting {a} to be a list the dimensions of your Path")
        if "j" in kw:
            js = kw["j"]
        else:
            js = [1] * ndim
        if "f" in kw:
            fs = kw["f"]
            for i in range(len(fs)):
                if fs[i] >= self.axes[i].n:
                    logging.getLogger(None).fatal(f"Invalid f parameter f({fs[i]})>=ndata({self.axes[i].n}) for axis {i+1}")
                    raise Exception(f"Invalid f parameter f({fs[i]})>=ndata({self.axes[i].n}) for axis {i+1}")

        else:
            fs = [0] * ndim
        if "n" in kw:
            ns = kw["n"]
            for i in range(len(fs)):
                if ns[i] > self.axes[i].n:
                    logging.getLogger(None).fatal(f"Invalid n parameter n({ns[i]}) > ndata({self.axes[i].n}) for axes {i+1}")
                    raise Exception(f"Invalid n parameter n({ns[i]}) > ndata({self.axes[i].n}) for axes {i+1}")
        else:
            ns = []
            for i in range(ndim):
                ns.append(int((self.axes[i].n - 1 - fs[i]) / js[i] + 1))
        for i in range(ndim):
            if self.axes[i].n < (1 + fs[i] + js[i] * (ns[i] - 1)):
                logging.getLogger(None).fatail(f"Invalid window parameter (outside axis range) f={fs[i]} j={js[i]} n={ns[i]} iax={i+1} ndata={self.axes[i].n}" )
                raise Exception(f"Invalid window parameter (outside axis range) f={fs[i]} j={js[i]} n={ns[i]} iax={i+1} ndata={self.axes[i].n}")
        return ns, fs, js
  