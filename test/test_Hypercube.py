import pytest
from sep_python._hypercube import Axis, Hypercube
import numpy as np


def test_Axis_default():
    "Basic test to check initialization of Axis class"
    axis = Axis()
    assert axis.n == 1
    assert axis.o == 0.0
    assert axis.d == 1.0
    assert axis.label == ""
    assert axis.unit == ""


def test_Axis_set_kw():
    "Customized initialization"
    axis = Axis(n=2, o=3.0, d=4.0, label="axis_label", unit="axis_unit")
    assert axis.n == 2
    assert axis.o == 3.0
    assert axis.d == 4.0
    assert axis.label == "axis_label"
    assert axis.unit == "axis_unit"


def test_Hypercube_init():
    axes = [Axis(n=i) for i in range(1, 4)]
    hypercube = Hypercube(axes)
    assert hypercube.get_ndim() == 3


def test_Hypercube_set_with_ns():
    hypercube = Hypercube.set_with_ns(
        [2, 3],
        os=[1.0, 1.0],
        ds=[1.0, 1.0],
        labels=["x", "y"],
        units=["unit_x", "unit_y"],
    )
    assert hypercube.get_ndim() == 2
    assert hypercube.get_axis(1).n == 2
    assert hypercube.get_axis(2).n == 3
    assert hypercube.get_axis(1).label == "x"
    assert hypercube.get_axis(2).label == "y"


def test_Hypercube_clone():
    axes = [Axis(n=i) for i in range(1, 4)]
    hypercube = Hypercube(axes)
    clone = hypercube.clone()
    assert hypercube.get_ndim() == clone.get_ndim()
    for i in range(hypercube.get_ndim()):
        assert hypercube.get_axis(i + 1).n == clone.get_axis(i + 1).n


def test_Hypercube_add_axis():
    axes = [Axis(n=i) for i in range(1, 4)]
    hypercube = Hypercube(axes)
    assert hypercube.get_ndim() == 3
    hypercube.add_axis(Axis(n=5))
    assert hypercube.get_ndim() == 4
    assert hypercube.get_axis(4).n == 5
    assert hypercube.get_axis(4).o == 0 
    assert hypercube.get_axis(4).d == 1



def test_Hypercube_get_window_params_single_params():
    hypercube = Hypercube.set_with_ns([3, 3])
    ns, fs, js = hypercube.get_window_params(n=[1, 1], f=[0, 0], j=[1, 1])
    assert ns == [1, 1]
    assert fs == [0, 0]
    assert js == [1, 1]
    with pytest.raises(Exception):
        hypercube.get_window_params(n=[4, 1], f=[0, 0], j=[1, 1])

    with pytest.raises(Exception):
        hypercube.get_window_params(n=[1, 1], f=[4, 0], j=[1, 1])


def test_get_window_params_min_max():
    axes = [Axis(n=15, o=0, d=1), Axis(n=20, o=0, d=2), Axis(n=30, o=0, d=3)]
    hyper = Hypercube(axes)
    params = {
        "min": [5, 5, 5],
        "max": [10, 10, 10]
    }
    expected_result = ([15, 20, 30], [0, 0, 0], [1, 1, 1])
    assert hyper.get_window_params(**params) == expected_result


def test_get_window_params_multiple_params():
    axes = [Axis(n=15, o=0, d=1), Axis(n=20, o=0, d=1), Axis(n=30, o=0, d=3)]
    hyper = Hypercube(axes)
    params = {
        "f1": 0,
        "f2": 0,
        "f3": 0,
        "j1": 1,
        "j2": 1,
        "j3": 1,
        "n1": 15,
        "n2": 20,
        "n3": 30,
    }
    expected_result = ([15, 20, 30], [0, 0, 0], [1, 1, 1])
    assert hyper.get_window_params(**params) == expected_result

def test_Hypercube_check_same():
    hypercube1 = Hypercube.set_with_ns([3, 3])
    hypercube2 = Hypercube.set_with_ns([3, 3])
    assert hypercube1.check_same(hypercube2)  # Same hypercubes


def test_Hypercube_check_not_same():
    hypercube1 = Hypercube.set_with_ns([3, 3])
    hypercube2 = Hypercube.set_with_ns([4, 3])
    assert not hypercube1.check_same(hypercube2)  # Different hypercubes
    

def test_Hypercube_get_ns():
    hypercube = Hypercube.set_with_ns([3, 3])
    assert hypercube.get_ns() == [3, 3]


def test_Hypercube_get_n123():
    hypercube = Hypercube.set_with_ns([3, 3])
    assert hypercube.get_n123() == 9  # 3 * 3


def test_Hypercube_sub_cube():
    hypercube = Hypercube.set_with_ns([10, 10], os=[0, 0], ds=[1, 1])
    sub_hypercube = hypercube.sub_cube([5, 5], [0, 0], [1, 1])
    assert sub_hypercube.get_ndim() == 2
    assert sub_hypercube.get_ns() == [5, 5]


def test_Axis_repr():
    axis = Axis(n=2, o=3.0, d=4.0, label="axis_label", unit="axis_unit")
    assert str(axis) == "n=2\to=3.0\td=4.0\tlabel=axis_label\tunit=axis_unit"


def test_Hypercube_repr():
    axes = [Axis(n=i, label=f"label_{i}") for i in range(1, 4)]
    hypercube = Hypercube(axes)
    x = "Axis 1: n=1\to=0.0\td=1.0\tlabel=label_1\n"
    x += "Axis 2: n=2\to=0.0\td=1.0\tlabel=label_2\n"
    x += "Axis 3: n=3\to=0.0\td=1.0\tlabel=label_3\n"
    assert str(hypercube) == x


@pytest.fixture
def hypercube():
    axes = [Axis(n=15, o=0, d=1), Axis(n=20, o=0, d=2), Axis(n=30, o=0, d=3)]
    return Hypercube(axes)


def test_calc_axis(hypercube):
    n, f, j = Hypercube.calc_axis(0, hypercube.get_axis(1), j=[2, 3, 4], f=[1, 2, 3])
    assert n == 7  # Adjust according to your expected output
    assert f == 1  # Adjust according to your expected output
    assert j == 2  # Adjust according to your expected output

    #testing for min and max arguments
    n1, f1, j1 = Hypercube.calc_axis(0, hypercube.get_axis(1), min1 = 5.0, max1 = 10.0)
    print(n1, f1, j1)
    assert n1 == 10  # Adjust according to your expected output
    assert f1 == 5  # Adjust according to your expected output
    assert j1 == 1  # Adjust according to your expected output

def test_get_window_params(hypercube):
    ns, fs, js = hypercube.get_window_params(n=[10, 20, 30], f=[0, 0, 0])
    assert ns == [10, 20, 30]
    assert fs == [0, 0, 0]
    assert js == [1, 1, 1]

def test_calc_sub_window(hypercube):
    axes = [Axis(n=5, o=0, d=2), Axis(n=10, o=0, d=4), Axis(n=15, o=0, d=6)]
    sub_hypercube = Hypercube(axes)
    result = hypercube.calc_sub_window(sub_hypercube)
    assert result == {
        "n": [5, 10, 15],
        "f": [0, 0, 0],
        "j": [2, 2, 2],
    }  # Adjust according to your expected output


def test_compact_hypercube():
    
    axes = [
        Axis(n=10, o=0, d=1, label="Axis 1", unit=""),
        Axis(n=10, o=5, d=2, label="Axis 2", unit="m"),
        Axis(n=1, o=0.0, d=1.0, label="", unit=""),
        Axis(n=20, o=2.0, d=2.0, label="Axis 4", unit="s"),
    ]
    hyper = Hypercube(axes)

    compacted_hyper = hyper.compact_hyper()

    assert compacted_hyper.get_axis(1).n == 10
    assert compacted_hyper.get_axis(1).o == 0
    assert compacted_hyper.get_axis(1).d == 1

    assert compacted_hyper.get_axis(2).n == 10
    assert compacted_hyper.get_axis(2).o == 5
    assert compacted_hyper.get_axis(2).d == 2


def test_concatenate_hypercubes():
    
    axis1 = Axis(n=3, o=0.0, d=1.0)
    axis2 = Axis(n=4, o=10.0, d=2.0)
    hypercube1 = Hypercube([axis1, axis2])

    axis3 = Axis(n=2, o=100.0, d=0.5)
    axis4 = Axis(n=5, o=50.0, d=5.0)
    hypercube2 = Hypercube([axis3, axis4])

    concatenated_hypercube = hypercube1.concatenate_hypercubes(hypercube2, axis=1)

    assert concatenated_hypercube.get_ndim() == 2
    assert concatenated_hypercube.get_axis(1).n == 3
    assert concatenated_hypercube.get_axis(2).n == 4 + 5

    assert concatenated_hypercube.get_axis(1).o == 0.0
    assert concatenated_hypercube.get_axis(1).d == 1.0
    assert concatenated_hypercube.get_axis(2).o == 10.0
    assert concatenated_hypercube.get_axis(2).d == 2.0
    
    assert concatenated_hypercube.get_axis(1).label == ""
    assert concatenated_hypercube.get_axis(1).unit == ""