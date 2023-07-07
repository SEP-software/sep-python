import pytest
import io
import numpy as np
import tempfile
from sep_python._sep_description import (sep_description_base,
                                         sep_description_file,
                                         inmem_description)


def test_sep_description_base_init():
    with pytest.raises(Exception):
        sep_description_base("path", "INPUT")


def test_inmem_description_init():

    bytes = io.BytesIO()
    inmem_desc = inmem_description(bytes, "OUTPUT",
                                   array=np.array([1., 2., 3.]),
                                   os=[0.1], ds=[0.2], labels=["label1"],
                                   units=["unit1"])
    assert isinstance(inmem_desc, sep_description_base)

 
def test_sep_description_file_read_write_description():
    x = tempfile.NamedTemporaryFile(prefix="/tmp/")
    nm = x.name
    sep_desc_file = sep_description_file(nm, "OUTPUT", 
                                         array=np.array([1., 2., 3.]),
                                         os=[0.1], ds=[0.2],
                                         labels=["label1"], units=["unit1"])
    sep_desc_file.set_binary_path("junk")
    sep_desc_file.write_description()
    (valid, dictionary, binary,
     file_ptr, eot) = sep_description_file.read_description(nm)
    des = sep_description_file(nm, "INPUT", dictionary=dictionary)
    assert des.to_dictionary() == sep_desc_file.to_dictionary()
