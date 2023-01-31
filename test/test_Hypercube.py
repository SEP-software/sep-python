"""Module test hypercube"""
import pytest
from sep_python.hypercube import Axis


def test_just_n():
    """Test with it does the correct thring when specifying just n"""
    axis = Axis(n=10)
    assert axis.n == 10
    assert axis.o == 0.0
    assert axis.d == 1.0
    assert axis.label == ""
    assert axis.unit == ""
