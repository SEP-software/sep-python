import logging
import numpy as np
import math


def check_valid(param_dict: dict, args: dict):
    """Check to make sure keyword is of the correct type

    param_dict - dictionary of kw
    args - Dictionary of argument names and required types
    """
    for arg, typ in args.items():
        if arg in param_dict:
            if not isinstance(param_dict[arg], typ):
                str = f"Expecting  {arg} to be of type"
                + f" {typ} but is type {type(arg)}"
                logging.getLogger().fatal(str)
                raise Exception("")


def base_class_doc_string(other_func):
    """Function that returns the doc-string of parent class"""
    def dec(func):
        func.__doc__ = func.__doc__+"\n\n"+other_func.__doc__
        return func
    return dec


def options_class_doc_string(other_func):
    """Function that returns the doc-string of parent class"""
    def dec(func):
        func.__doc__ = func.__doc__ + "\n\n" + other_func()
        return func
    return dec


def calc_blocks(ns, esize, **kw):
    """
        Calculate blocks to break in dataset

        ns - Dimensions of the dataset
        esize - Element size
        blk   - Approximatte blocksize

        Returns blocks
    """
    if "blk" not in kw:
        raise Exception("Expecting blk")
    blk = kw["blk"]
    nelem = np.prod(ns)*esize
    nparts = math.ceil(nelem/blk)
    blocks = [blk]*(nparts-1)
    left = nelem-(nparts-1)*blk
    if left > 0:
        blocks.append(left)
    return blocks
