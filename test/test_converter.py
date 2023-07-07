import pytest
import numpy as np
import logging
from sep_python._sep_converter import DtypeConvert

def test_dtype_convert():
    logger = logging.getLogger("Test Logger")
    converter = DtypeConvert()
    converter.set_logger(logger)

    # Adding data types
    converter.add_data_type("float32", "native_float", "dataFloat", np.float32, 4)
    converter.add_data_type("float64", "native_double", "dataDouble", np.float64, 8)
    converter.add_data_type("int32", "native_int", "dataInt", np.int32, 4)
    converter.add_data_type("uint8", "native_byte", "dataByte", np.uint8, 1)
    converter.add_data_type("complex64", "dataComplex", "native_complex", np.complex64, 8)
    converter.add_data_type("complex128", "dataComplexDouble", "native_complex_double", np.complex128, 16)

    # Testing get_name()
    assert converter.get_name('float32') == 'float32'
    assert converter.get_name(np.float32) == 'float32'
    with pytest.raises(Exception):
        converter.get_name('unknown_type')

    # Testing sep_name_to_numpy()
    assert converter.sep_name_to_numpy('native_float') == np.float32
    with pytest.raises(Exception):
        converter.sep_name_to_numpy('unknown_type')

    # Testing from_SEP_name()
    assert converter.from_SEP_name('native_float') == 'float32'
    with pytest.raises(Exception):
        converter.from_SEP_name('unknown_type')

    # Testing get_numpy()
    assert converter.get_numpy('float32') == np.float32
    with pytest.raises(Exception):
        converter.get_numpy('unknown_type')

    # Testing get_esize()
    assert converter.get_esize('float32') == 4
    with pytest.raises(Exception):
        converter.get_esize('unknown_type')

    # Testing get_SEP_name()
    assert converter.get_SEP_name('float32') == 'native_float'
    with pytest.raises(Exception):
        converter.get_SEP_name('unknown_type')

    # Testing valid_type()
    assert converter.valid_type('float32') == True
    assert converter.valid_type('unknown_type') == False