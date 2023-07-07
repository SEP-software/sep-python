import pytest
from sep_python._sep_proto import MemReg
from sep_python._base_prog import base_prog, Hypercube, Axis


class valid_class(base_prog):
    def __init__(self, **kw):
        super().__init__("dumb", **kw)

    def do_block(self, vec_in: MemReg, hyper_out: Hypercube) -> MemReg:
        pass

    def input_from_output(self, out_hyper: Hypercube) -> Hypercube:
        aout = []
        for ax in out_hyper.axes:
            aout.append(
                Axis(
                    n=int(ax.n / 2), o=ax.o + ax.d, d=ax.d, label=ax.label, unit=ax.unit
                )
            )
        return Hypercube(aout)


def test_full():
    cls = valid_class()

    hyper = Hypercube.set_with_ns([100, 100, 100])
    hypers_in, hypers_out = cls.block_out(hyper, "float32", "float32")

    assert len(hypers_in) == 1

    assert len(hypers_out) == 1

    assert hypers_out[0].check_same(hyper)

    assert hypers_in[0].axes[0].n == 50

    assert hypers_in[0].axes[0].o == 1


def test_half():
    cls = valid_class(mem=0.008)

    hyper = Hypercube.set_with_ns(
        [100, 100, 100],
    )
    hypers_in, hypers_out = cls.block_out(hyper, "float32", "float32")

    assert len(hypers_in) == 2

    assert len(hypers_out) == 2

    assert hypers_out[0].axes[0].n == hyper.axes[0].n
    assert hypers_out[0].axes[1].n == hyper.axes[1].n
    assert hypers_out[0].axes[2].n == hyper.axes[2].n / 2
    assert hypers_out[1].axes[2].o == hyper.axes[2].o + hyper.axes[2].d * 50
    assert hypers_out[0].axes[2].o == hyper.axes[2].o


def test_small():
    cls = valid_class(mem=0.00008)

    hyper = Hypercube.set_with_ns(
        [100, 100, 100],
    )
    hypers_in, hypers_out = cls.block_out(hyper, "float32", "float32")

    assert len(hypers_in) == 200

    assert len(hypers_out) == 200

    assert hypers_out[0].axes[0].n == hyper.axes[0].n
    assert hypers_out[0].axes[1].n == hyper.axes[1].n / 2
    assert hypers_out[0].axes[2].n == 1
    assert hypers_out[1].axes[1].o == hyper.axes[1].o + hyper.axes[1].d * 50
    assert hypers_out[0].axes[1].o == hyper.axes[1].o
