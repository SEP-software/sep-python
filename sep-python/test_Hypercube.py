#!/usr/bin/env python
import pytest
import Hypercube 

def test_just_n():
    ax=Hypercube.axis(n=10)
    assert ax.n==10
    assert ax.o==0.
    assert ax.d==1.
    assert ax.label==""
    assert ax.unit==""
