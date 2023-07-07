import pytest
import numpy as np
from sep_python._hypercube import Hypercube, Axis
from sep_python._sep_vector import (
    get_sep_vector,
    Vector,
    FloatVector,
    NonInteger,
    RealNumber,
    DoubleVector,
    ComplexVector,
    ComplexDoubleVector,
    ByteVector,
    IntVector,
)


# Generate some dummy data
@pytest.fixture
def data():
    return np.random.rand(50).astype("float32")


@pytest.fixture
def data64():
    return np.random.rand(50)


# Generate a dummy Hypercube
@pytest.fixture
def hyper():
    axis = Axis(n=50, o=0, d=1)
    return Hypercube(axes=[axis])


def test_vector(hyper, data):
    vector = Vector(hyper, "float32", vals=data)
    assert vector.get_nd_array().shape == (50,)
    assert np.all(vector.get_nd_array() == data)


def test_noninteger(hyper, data):
    nonint = NonInteger(hyper, "float32", vals=data)
    assert nonint.get_nd_array().shape == (50,)
    assert np.all(nonint.get_nd_array() == data)


def test_realnumber(hyper, data):
    real = RealNumber(hyper, "float32", vals=data)
    assert real.get_nd_array().shape == (50,)
    assert real.clip(0.1, 0.9) is None
    assert real.cent(50) == np.percentile(data, 50)


def test_floatvector(hyper, data):
    float_vec = FloatVector(hyper, vals=data)
    assert float_vec.get_nd_array().shape == (50,)
    assert np.all(float_vec.get_nd_array() == data)
    assert str(float_vec) == f"FloatVector\n{str(float_vec.get_hyper())}"
    clone = float_vec.clone()
    assert np.all(clone.get_nd_array() == data)
    clone_space = float_vec.clone_space()
    assert clone_space.shape == (50,)


def test_vector_wrong_data_type(hyper, data):
    with pytest.raises(Exception):
        Vector(hyper, "float64", vals=data)


def test_vector_non_ndarray(hyper):
    with pytest.raises(Exception):
        Vector(hyper, "float32", vals=[1, 2, 3])


def test_floatvector_check_same(hyper, data):
    float_vec1 = FloatVector(hyper, vals=data)
    float_vec2 = FloatVector(hyper, vals=data)
    assert float_vec1.checkSame(float_vec2)


def test_floatvector_check_same_different(hyper, data):
    axis = Axis(n=100, o=0, d=2)
    hyper2 = Hypercube(axes=[axis])
    float_vec1 = FloatVector(hyper, vals=data)
    float_vec2 = FloatVector(hyper2, vals=np.random.rand(100).astype("float32"))
    assert not float_vec1.checkSame(float_vec2)


def test_floatvector_check_same_different_class(hyper, data):
    float_vec = FloatVector(hyper, vals=data)
    real = RealNumber(hyper, "float32", vals=data)
    assert not float_vec.checkSame(real)


def test_double_vector(hyper, data64):
    dv = DoubleVector(hyper, vals=data64)
    assert dv.shape == (50,)
    assert np.all(dv.get_nd_array() == data64)
    assert dv.__repr__() == "Double_Vector\n" + str(hyper)
    dv_clone = dv.clone()
    assert dv.checkSame(dv_clone)
    dv_space_clone = dv.clone()
    assert dv_space_clone.get_nd_array().shape == (50,)
    assert np.all(dv_space_clone.get_nd_array() == data64)


def test_int_vector(hyper):
    int_data = np.random.randint(0, 50, 50).astype("int32")
    iv = IntVector(hyper, vals=int_data)
    assert iv.get_nd_array().shape == (50,)
    assert np.all(iv.get_nd_array() == int_data)
    assert iv.__repr__() == "IntVector\n" + str(hyper)
    iv_clone = iv.clone()
    assert iv.checkSame(iv_clone)


def test_complex_vector(hyper, data):
    datac = data + data * 1.0j
    cv = ComplexVector(hyper, vals=datac)
    assert cv.get_nd_array().shape == (50,)
    assert np.all(cv.get_nd_array() == datac)
    assert cv.__repr__() == "ComplexVector\n" + str(hyper)
    cv_clone = cv.clone()
    assert cv.checkSame(cv_clone)
    assert cv_clone.get_nd_array().shape == (50,)
    assert np.all(cv_clone.get_nd_array() == datac)


def test_complex_double_vector(hyper, data64):
    datac = data64 + data64 * 1.0j
    cv = ComplexDoubleVector(hyper, vals=datac)
    assert cv.get_nd_array().shape == (50,)
    assert np.all(cv.get_nd_array() == datac)
    assert cv.__repr__() == "ComplexDoubleVector\n" + str(hyper)
    cv_clone = cv.clone()
    assert cv.checkSame(cv_clone)
    assert cv_clone.get_nd_array().shape == (50,)
    assert np.all(cv_clone.get_nd_array() == datac)


def test_byte_vector(hyper):
    byte_data = np.random.randint(0, 256, 50).astype("uint8")
    bv = ByteVector(hyper, vals=byte_data)
    assert bv.get_nd_array().shape == (50,)
    assert np.all(bv.get_nd_array() == byte_data)
    assert bv.__repr__() == "ByteVector\n" + str(hyper)
    bv_clone = bv.clone()
    assert bv.checkSame(bv_clone)


def check_same(v1, v2):
    return np.allclose(v1, v2, atol=1e-5)


def test_get_sep_vector_with_hypercube():
    hyper = Hypercube(axes=[Axis(n=3, d=1.0, o=0.0, label="x1")])
    vec = get_sep_vector(hyper)
    assert isinstance(vec, FloatVector)
    assert vec.get_hyper() == hyper


def test_get_sep_vector_with_numpy():
    np_array = np.array([1.0, 2.0, 3.0]).astype("float32")
    vec = get_sep_vector(np_array)
    assert isinstance(vec, FloatVector)
    assert check_same(vec.get_nd_array(), np_array)


def test_get_sep_vector_with_numpy_and_data_type():
    np_array = np.array([1, 2, 3]).astype("int32")
    vec = get_sep_vector(np_array, data_type="int32")
    assert isinstance(vec, IntVector)
    assert check_same(vec.get_nd_array(), np_array.astype("int32"))


def test_get_sep_vector_with_numpy_and_incorrect_shape():
    hyper = Hypercube(axes=[Axis(n=2, d=1.0, o=0.0, label="x1")])
    np_array = np.array([1.0, 2.0, 3.0])
    with pytest.raises(Exception):
        vec = get_sep_vector(np_array, hyper=hyper)


def test_get_sep_vector_with_incorrect_data_type():
    hyper = Hypercube(axes=[Axis(n=3, d=1.0, o=0.0, label="x1")])
    with pytest.raises(Exception):
        vec = get_sep_vector(hyper, data_type="not a type")


def test_get_sep_vector_with_incorrect_args():
    with pytest.raises(Exception):
        vec = get_sep_vector("Not a hypercube or numpy array")


def test_get_sep_vector_with_no_args():
    with pytest.raises(Exception):
        vec = get_sep_vector()


def test_get_sep_vector_with_multiple_args():
    hyper1 = Hypercube(axes=[Axis(n=3, d=1.0, o=0.0, label="x1")])
    hyper2 = Hypercube(axes=[Axis(n=3, d=1.0, o=0.0, label="x2")])
    with pytest.raises(Exception):
        vec = get_sep_vector(hyper1, hyper2)
