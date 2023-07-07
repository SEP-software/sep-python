import pytest
import numpy as np
from sep_python._base_helper import (check_valid, calc_blocks)


def test_check_valid():
    param_dict = {'key1': 'str', 'key2': 123}
    args = {'key1': str, 'key2': int}
    check_valid(param_dict, args)  # This should not raise an exception

    # Let's check a case where check_valid should fail
    param_dict = {'key1': 'str', 'key2': '123'}
    with pytest.raises(Exception):
        check_valid(param_dict, args)


def test_calc_blocks():
    ns = np.array([10, 10, 10])
    esize = 4
    blk = 900
    blocks = calc_blocks(ns, esize, blk=blk)
    assert len(blocks) == 5
    assert blocks[-1] == 400  # Checking the last element
