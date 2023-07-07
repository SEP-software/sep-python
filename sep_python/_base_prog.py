import math
import argparse
import logging
import sys
from abc import ABC, abstractmethod
from sep_python._hypercube import Hypercube, Axis
from sep_python._sep_vector import get_sep_vector
from sep_python._sep_proto import MemReg
from sep_python._sep_converter import converter
from sep_python._sep_logger import sep_loggers


class base_prog(ABC):
    """ "Basic program"""

    def __init__(self, prog_name, description, input_req, output_req):
        """
        prog_name - Name of program
        description - Description of program
        input_req  - Whether the program has an input
        output_req - Whether the program has an output

        """
        self._prog_name = prog_name
        self._description = description
        self._input_req = input_req
        self._output_req = output_req
        self._hyper_in = None
        self._hyper_out = None

    def build_args(self):
        """Build command line arguments"""

        self._parser = argparse.ArgumentParser(
            prog=self._prog_name, description=self._description
        )

        if self._input_req:
            self._parser.add_argument(
                "-input_file",
                required=False,
                type=str,
                help="Input file (otherwise stdin)",
            )
        if self._output_req:
            self._parser.add_argument(
                "-output_file",
                required=False,
                type=str,
                help="Output file (otherwise stdout)",
            )
        self._parser.add_argument(
            "-mem", type=int, default=4, help="DEfault memory to use in GB"
        )
        self._parser.add_argument(
            "-v",
            "--verbosity",
            type=int,
            choices=[1, 2, 3],
            help="Increase output verbosity: 1 for warnings, 2 for info, 3 for debug",
        )

    def process_commandline(self, command_line=None):
        self.build_args()
        if command_line is not None:
            args = self._parser.parse_args(command_line)
        else:
            args = self._parser.parse_args()
        if args.verbosity is None:
            sep_loggers.set_default_level(logging.ERROR)
        elif args.verbosity == 1:
            sep_loggers.set_default_level(logging.WARNING)
        elif args.verbosity == 2:
            sep_loggers.set_default_level(logging.INFO)
        else:
            sep_loggers.set_default_level(logging.DEBUG)

        file_in = None
        file_out = None

        if self._input_req:
            if not sys.stdin.isatty():
                file_in = "<"
            elif args.input_file is None:
                self._parser.print_help()
            else:
                file_in = args.input_file
        if self._output_req:
            if not sys.stdout.isatty():
                file_out = ">"
            elif args.output_file is None:
                self._parser.print_help()
                sys.exit(-1)
            else:
                file_out = args.output_file

        self._kw = vars(args)
        return file_in, file_out

    def output_from_input(self, inp_hyper):
        """ "
        From input description create output description

        inp_hyper - Input hypercube description

        """

        return inp_hyper.clone()

    def input_from_output(self, out_hyper):
        """ "
        From output description figure out input description

        out_hyper - Output hypercube description

        """
        return out_hyper.clone()

    @abstractmethod
    def calc_output_hypercube(self, in_hyper):
        """Calculate the output hypercube from an input hypercube and supplied parameters

        in_hyper - Input hypercube (or None)
        """

    @abstractmethod
    def do_block(self, vec_in: MemReg, hyper_out: Hypercube) -> MemReg:
        """Process a block

        vec_in  - Input vector
        vec_out - Output vector

        """

    def pipe_work(self, obj_in, obj_out, hypers_in, hypers_out):
        """ "
        obj_in, obj_out - Objects that handle IO
        hypers_in, hypers_out - Hypercubes that descripbe
          input, output chunks

        """
        # redho this with overlapping memory
        vec_in = None
        vec_out = None
        for hyp_in, hyp_out in zip(hypers_in, hypers_out):
            if obj_in is not None:
                vec_in = get_sep_vector(hyp_in, data_type=obj_in.get_data_type())
                obj_in.read(vec_in, **obj_in.get_hyper().calc_sub_window(hyp_in))
            vec_out = self.do_block(vec_in, hyp_out)
            if obj_out is not None:
                obj_out.write(vec_out, **obj_out.get_hyper().calc_sub_window(hyp_out))

    def block_out(self, hyper_out, data_type_in, data_type_out, ncopies=2):
        """ "
        Figure out how to best block

        hyper_out     - Hypercube output
        data_type_in  - Input data type
        data_type_out - Output data type
        ncopies - Number of copies to store simultaneously

        """
        esize = converter.get_esize(data_type_in) + converter.get_esize(data_type_out)
        mem = 10
        kw = self._kw
        if "mem" in kw:
            mem = kw["mem"]

        mem = mem * 1000 * 1000 * 1000 / esize / ncopies

        ns_in = hyper_out.get_ns()
        os = [0.0] * 8
        ds = [1.0] * 8
        ns = [1] * 8
        labels = [""] * 8
        units = [""] * 8
        for iax, ax in enumerate(hyper_out.axes):
            ns[iax], os[iax], ds[iax] = ax.n, ax.o, ax.d
            labels[iax], units[iax] = ax.label, ax.unit

        if ns[0] > mem:
            raise Exception("Can't read single trace axis")

        iax = 1
        blk = ns_in[0]
        nblock = [1] * 8
        nblock[0] = ns_in[0]
        found = False
        while iax < len(ns) and not found:
            if blk * ns[iax] < mem:
                blk *= ns[iax]
                nblock[iax] = ns[iax]
                iax += 1
            else:
                found = True
        if iax < 8:
            nblock[iax] = math.floor(mem / blk)

        nbuf = [1] * 8
        axes = [Axis(n=1)] * 8
        axes[0] = Axis(axis=hyper_out.axes[0])
        fdone = [0] * 8
        hyps_in = []
        hyps_out = []

        while fdone[7] < ns[7]:
            nbuf[7] = min(ns[7] - fdone[7], nblock[7])
            axes[7] = Axis(
                n=nbuf[7],
                o=os[7] + ds[7] * fdone[7],
                d=ds[7],
                label=labels[7],
                unit=units[7],
            )
            fdone[7] += nbuf[7]
            fdone[6] = 0
            while fdone[6] < ns[6]:
                nbuf[6] = min(ns[6] - fdone[6], nblock[6])
                axes[6] = Axis(
                    n=nbuf[6],
                    o=os[6] + ds[6] * fdone[6],
                    d=ds[6],
                    label=labels[6],
                    unit=units[6],
                )
                fdone[6] += nbuf[6]
                fdone[5] = 0
                while fdone[5] < ns[5]:
                    nbuf[5] = min(ns[5] - fdone[5], nblock[5])
                    axes[5] = Axis(
                        n=nbuf[5],
                        o=os[5] + ds[5] * fdone[5],
                        d=ds[5],
                        label=labels[5],
                        unit=units[5],
                    )
                    fdone[5] += nbuf[5]
                    fdone[4] = 0
                    while fdone[4] < ns[4]:
                        nbuf[4] = min(ns[4] - fdone[4], nblock[4])
                        axes[4] = Axis(
                            n=nbuf[4],
                            o=os[4] + ds[4] * fdone[4],
                            d=ds[4],
                            label=labels[4],
                            unit=units[4],
                        )
                        fdone[4] += nbuf[4]
                        fdone[3] = 0
                        while fdone[3] < ns[3]:
                            nbuf[3] = min(ns[3] - fdone[3], nblock[3])
                            axes[3] = Axis(
                                n=nbuf[3],
                                o=os[3] + ds[3] * fdone[3],
                                d=ds[3],
                                label=labels[3],
                                unit=units[3],
                            )
                            fdone[3] += nbuf[3]
                            fdone[2] = 0
                            while fdone[2] < ns[2]:
                                nbuf[2] = min(ns[2] - fdone[2], nblock[2])
                                axes[2] = Axis(
                                    n=nbuf[2],
                                    o=os[2] + ds[2] * fdone[2],
                                    d=ds[2],
                                    label=labels[2],
                                    unit=units[2],
                                )
                                fdone[2] += nbuf[2]

                                fdone[1] = 0
                                while fdone[1] < ns[1]:
                                    nbuf[1] = min(ns[1] - fdone[1], nblock[1])
                                    axes[1] = Axis(
                                        n=nbuf[1],
                                        o=os[1] + ds[1] * fdone[1],
                                        d=ds[1],
                                        label=labels[1],
                                        unit=units[1],
                                    )
                                    fdone[1] += nbuf[1]
                                    hyps_out.append(Hypercube(axes[0 : len(ns_in)]))
                                    hyps_in.append(self.input_from_output(hyps_out[-1]))
        return hyps_in, hyps_out

    def convert_ints(input_str):
        """Check if a string can be converted to a integer or list of integers"""
        try:
            # Attempt to convert to an integer
            x = int(input_str)
            return [x]
        except ValueError:
            # If it can't be an integer, try to convert to a list
            try:
                if "," in input_str:
                    xlist = list(input_str.split(","))
                    out_list = []
                    for x in xlist:
                        try:
                            out_list.append(int(x))
                        except ValueError:
                            raise Exception("Can not be converted to int or int list")
                    return out_list
                raise Exception("Can not be converted to int or int list")
            except ValueError:
                raise Exception("Can not be converted to int or int list")

    def convert_floats(input_str):
        """Check if a string can be converted to a integer or list of integers"""
        try:
            # Attempt to convert to an integer
            x = float(input_str)
            return [x]
        except ValueError:
            # If it can't be an integer, try to convert to a list
            try:
                if "," in input_str:
                    xlist = list(input_str.split(","))
                    out_list = []
                    for x in xlist:
                        try:
                            out_list.append(float(x))
                        except ValueError:
                            raise Exception(
                                "Can not be converted to float or float list"
                            )
                    return out_list
                raise Exception("Can not be converted to float or float list")
            except ValueError:
                raise Exception("Can not be converted to float or float list")

    def get_input_hypercube(self):
        """REturn input hypecrcube"""
        return self._hyper_in

    def get_output_hypercube(self):
        """REturn output hypecrcube"""
        return self._hyper_out
