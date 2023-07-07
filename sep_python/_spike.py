#!/usr/bin/env python
from numba import njit, prange
import numpy as np
from sep_python import base_prog, Hypercube, Axis, get_sep_vector


class Spike(base_prog):
    """ """

    def __init__(self, **kw):
        """
        Put some spikes in a dataset

        nX,oX,dX,labelX,unitX - Descrbe output dataset
        nsp - Number of spikes
        mag - Magnitude of spikes
        kX  - Location along each axis for spike (-1) means along entire axis

        """
        super().__init__(
            "Spike", "Make delta functions and impulsive plane waves", False, True
        )
        self._kw = kw

    def calc_output_hypercube(self, in_hyper):
        """
        Calculate window parameters

        """
        if in_hyper is not None:
            raise Exception("Expecting hyper_in to to be None")
        axes = []
        for iax in range(8):
            axes.append(
                Axis(
                    n=self._kw[f"n{iax+1}"],
                    o=self._kw[f"o{iax+1}"],
                    d=self._kw[f"d{iax+1}"],
                    label=self._kw[f"label{iax+1}"],
                )
            )
        self._hyper_out = Hypercube(axes).compact_hyper()
        self._hyper_in = self._hyper_out.clone()
        ns = self._hyper_out.get_ns() + [1] * (8 - len(self._hyper_out.axes))

        self._nsp = self._kw["nsp"]

        nsp = self._nsp
        self._mags = np.zeros((self._nsp))
        self._mags[:] = base_prog.convert_floats(self._kw["mag"])
        if len(self._mags) != nsp:
            if len(self._mags) == 1:
                self._mags = [self._mags] * nsp
            else:
                raise Exception(f"Mag length {len(self._mags)} not equal nsp={nsp}")

        self._ks = np.zeros((self._nsp, 8), dtype=np.int32)
        self._ks.fill(-1)

        for i in range(8):
            if self._kw[f"k{i+1}"] is not None:
                my_ints = base_prog.convert_ints(self._kw[f"k{i+1}"])
                if len(my_ints) != nsp:
                    raise Exception(
                        f"nsp={nsp} but k{i+1}"
                        + f" is length of {len(self._kw[f'k{i+1}'])}"
                    )
                self._ks[:, i] = my_ints
        for ik in range(self._ks.shape[0]):
            for ip in range(self._ks.shape[1]):
                if self._ks[ik, ip] < -1 or self._ks[ik, ip] >= ns[ip]:
                    raise Exception(f"Illegal k{ip+1} value {self._ks[ik,ip]}")

    def do_block(self, vec_in, hyper_out):
        """Process a block

        vec_in  - Input vector
        hyper_out - Output hypercube

        """
        vec_out = get_sep_vector(hyper_out, data_type=self._kw["data_type"])
        ns = vec_out.get_hyper().get_ns()
        vec_2d = np.reshape(
            vec_out.get_nd_array(),
            (int(vec_out.get_hyper().get_n123() / ns[0]), ns[0]),
        )
        my_dict = self._hyper_out.calc_sub_window(hyper_out)
        trace_map = self._hyper_out.trace_locations(
            my_dict["n"], my_dict["f"], my_dict["j"]
        )
        do_spike(trace_map, self._ks, self._mags, vec_2d)
        return vec_out

    def output_from_input(self, inp_hyper):
        """ "
        From input description create output description

        inp_hyper - Input hypercube description

        """
        return self._hyper_out

    def build_args(self):
        super().build_args()
        for i in range(8):
            self._parser.add_argument(
                f"-n{i+1}",
                type=int,
                default=1,
                required=False,
                help=f"Number of elements along {i+1} axis",
            )
            self._parser.add_argument(
                f"-o{i+1}",
                type=float,
                required=False,
                default=0.0,
                help=f"Origin of axis {i+1}",
            )
            d = 1
            if i == 0:
                d = 0.004
            self._parser.add_argument(
                f"-d{i+1}",
                type=float,
                default=d,
                required=False,
                help=f"Sampling along {i+1} axis",
            )
            self._parser.add_argument(
                f"-label{i+1}",
                type=str,
                required=False,
                default="",
                help=f"Label for axis {i+1}",
            )
            self._parser.add_argument(
                f"-k{i+1}",
                required=False,
                help="Specify index of location"
                + "of delta function. If a k is absent, the delta function"
                + f"becomes a constant function in the {i+1} dimension."
                + "If any kN is -1, no spike will be produced.",
            )
        self._parser.add_argument(
            "-mag", default=1.0, required=False, help="Magnitude  of spikes"
        )
        self._parser.add_argument(
            "-nsp", type=int, default=1, required=False, help="Number of spikes"
        )

        self._parser.add_argument(
            "-data_type",
            type=str,
            default="float32",
            choices=["float32", "float64", "int32", "complex64", "complex128"],
            required=False,
            help="Type of data",
        )


@njit(parallel=True)
def do_spike(trace_map, ks, mags, vec_2d):
    for itrace in prange(trace_map.shape[0]):
        for isp, mag in enumerate(mags):
            valid = True
            index = 0
            while valid and index < trace_map.shape[0]:
                if (
                    ks[isp, index + 1] != -1
                    and ks[isp, index + 1] != trace_map[itrace, index]
                ):
                    valid = False
                index += 1
            if valid:
                if ks[isp, 0] == -1:
                    vec_2d[itrace, :] += mag
                else:
                    vec_2d[itrace, ks[isp, 0]] += mag
