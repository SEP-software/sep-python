from sep_python._data import dataset, file_portion, file_concat
from sep_python._base_helper import calc_blocks
from sep_python._sep_converter import converter


class file_dataset(dataset):

    def __init__(self, path, intent):
        """

            path - Path to the binary
            intent - Intent of the data

        """
        self._intent = intent
        super.__init__(file_portion(path))

    def check_valid(path, description_type):
        """Check to maske we have a valid path"""
        if path[0:5] == "gs://":
            raise Exception("Can't initiate file dataset with object")
        if len(path.split(":") > 1):
            raise Exception("can't have a : in path name")
        return True


class file_datasets(dataset):

    def __init__(self, paths, intent, **kw):
        """

            path - Path to the binary
            intent - Intent of the data
            Required if output:
                blk    -  Approximate blocksize
                description - Description object

        """
        self._intent = intent
        portions = []
        beg_locs = [0]
        if intent == "INPUT":
            for pth in paths.split(":"):
                portions.append(file_portion[pth])
                beg_locs.append(beg_locs[-1]+portions[-1].get_size())

        elif intent == "OUTPUT":
            if "blk" not in kw or "esize" not in kw or "ns" not in kw:
                raise Exception("When specifying output"
                                + " must supply esize,ns,blk")
            if "description" not in kw:
                raise Exception("description not specified")
            if "blk" not in kw:
                raise Exception("Must specifty blk")
            ns = kw["description"].get_hyper().get_ns()
            esize = converter.get_esize(kw["description"].get_data_type())
            blocks = calc_blocks(ns, esize, kw["blk"])
            for index, element in enumerate(blocks):
                portions.append(f"{file_portion[pth]}@{index}")
                beg_locs.append(beg_locs[-1]+element)

        super.__init__(portions, beg_locs)

    def check_valid(path, intent, **kw):
        """Check to maske we have a valid path"""
        if intent == "INPUT":
            paths = path.split(":")
            if len(path) == 1:
                raise Exception("Only specified one path for input")
            for pth in paths:
                if pth[0:5] == "gs://":
                    raise Exception("Can't specify "
                                    + "object path in file dataset")
        return True


class file_pipe(file_concat):

    def __init__(self, path, intent, **kw):
        """

            path - File pointer to stdin or stdout
            intent - Intent of the data

            required:
              file_ptr - Required file_ptr

        """
        super.__init__(path, intent, **kw)

    def check_valid(path, intent, **kw):
        """Check to maske we have a valid path"""
        if path == ">" and intent == "INPUT":
            raise Exception("Can't have > and input")
        if path == "<" and intent == "OUTPUT":
            raise Exception("Can't have < and output")
        raise Exception("Pipe and intent don't match")


class file_follow_hdr(file_concat):

    def __init__(self, path, intent, **kw):
        """

            path -  Path to file
            intent - Intent of the data

            file_ptr - Required

        """
        super.__init__(path, intent, **kw)

    def check_valid(path, intent, **kw):
        """Check to maske we have a valid path"""
        return True
