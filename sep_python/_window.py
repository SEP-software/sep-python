#!/usr/bin/env python

from sep_python import base_prog, Hypercube, Axis


class Window(base_prog):
    """
    Window a dataset
    """

    def __init__(self, **kw):
        """
        Initialize a window operation
            Describe windowing with
                nX - number of samples
                fX - First sample
                jX - Skip parameter
                minX - Minimum value along axis
                maxX - Maximum value along axis

        """
        super().__init__("Window", "Window a dataset", True, True)
        self._kw = kw

    def calc_output_hyperucbe(self, in_hyper):
        """
        Calculate window parameters

        hyper_in - Calculate window parameters

        """
        self._hyper_in = in_hyper
        kw_no_nones = {k: v for k, v in self._kw.items() if v is not None}
        self._ns, self._fs, self._js = in_hyper.get_window_params(**kw_no_nones)

    def do_block(self, vec_in, hyper_out):
        """Process a block

        vec_in  - Input vector
        hyper_out - Output hypercube

        """

        vec_out = vec_in.clone()
        if not hyper_out.check_same(vec_out.get_hyper()):
            raise Exception("Output hypercube does not match expected shape")
        return vec_out

    def output_from_input(self, inp_hyper):
        """ "
        From input description create output description

        inp_hyper - Input hypercube description

        """

        axes = []
        for ax, n, f, j in zip(self._hyper_in.axes, self._ns, self._fs, self._js):
            axes.append(
                Axis(n=n, o=ax.o + ax.d * f, d=ax.d * j, label=ax.label, unit=ax.unit)
            )
        self._hyper_out = Hypercube(axes)
        return self._hyper_out

    def build_args(self):
        super().build_args()
        for i in range(8):
            self._parser.add_argument(
                f"-n{i+1}",
                type=int,
                required=False,
                help=f"Number of elements along {i+1} axis",
            )
            self._parser.add_argument(
                f"-f{i+1}",
                type=int,
                required=False,
                help="Number of samples to skip at begining",
            )
            self._parser.add_argument(
                f"-j{i+1}", type=int, required=False, help=f"Skip along {i+1} axis"
            )
            self._parser.add_argument(
                f"-min{i+1}",
                type=float,
                required=False,
                help=f"Minimum value along {i+1} axis",
            )
            self._parser.add_argument(
                f"-max{i+1}", type=float, required=False, help=f"Maximim {i+1} axis"
            )
