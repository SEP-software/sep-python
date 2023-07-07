import pytest
from sep_python import Spike


def test_simple_spike():
    spk = Spike()
    spk.process_commandline("-n1 3 -k1 1 -data_type float32".split())
    spk.calc_output_hypercube(None)
    vec_out = spk.do_block(None, spk.get_output_hypercube())
    axes = vec_out.get_hyper().get_axes()
    assert len(axes) == 1
    assert axes[0].n == 3
    assert axes[0].o == 0.0
    assert axes[0].d == 1.0
    mat = vec_out.get_nd_array()
    assert mat[0] == 0
    assert mat[1] == 1
    assert mat[2] == 0
