#!/usr/bin/env python
import pytest
from sepPython.Hypercube import hypercube,axis

def test_just_n():
    ax=axis(n=10)
    assert ax.n==10
    assert ax.o==0.
    assert ax.d==1.
    assert ax.label==""
    assert ax.unit==""
