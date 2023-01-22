#!/usr/bin/env python
import pytest 
from sep_python.hypercube import Hypercube,Axis

def test_just_n():
    ax=Axis(n=10)
    assert ax.n==10
    assert ax.o==0.
    assert ax.d==1.
    assert ax.label==""
    assert ax.unit==""
