import os
from sep_python._data import dataset, concat, portion_base, data_base, datasets
from sep_python._base_helper import calc_blocks
from sep_python._sep_converter import converter
from sep_python._base_helper import base_class_doc_string


class file_portion(portion_base):
    """How to do IO on a dataset storead on GCS"""

    def __init__(self, path):
        self._path = path

    def read(self, seeks, blks):
        """
        Read a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read


        Returns a byte array of the given portion of the data

        """
        byte_array = bytearray()
        with open(self._path, "rb") as file_pointer:
            for seek, blk in zip(seeks, blks):
                file_pointer.seek(seek)
                byte_array += file_pointer.read(blk)
        return byte_array

    def write(self, seeks, blks, byte_array):
        """
        Write a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read
        byte_array - Data to write out

        """
        with open(self._path, "wb") as file_pointer:
            old = 0
            for seek, blk in zip(seeks, blks):
                new = old + blk
                file_pointer.write(byte_array[old:new])
                old = new

    def remove(self):
        """Remove file"""
        os.remove(self._path)

    def get_size(self):
        """Get File size"""
        return os.stat(self._path).st_size


class file_dataset(dataset):
    def __init__(self, path, intent, **kw):
        """

        path - Path to the binary
        intent - Intent of the data

        """
        self._intent = intent
        super().__init__(intent, file_portion(path), **kw)

    @base_class_doc_string(data_base.__init__)
    def get_data_path(path, intent, **kw):
        """Get binary for dataset writen to binary file"""
        if "description" not in kw:
            raise Exception("Expecting description")
        dictionary = kw["description"].get_dictionary()

        if intent == "INPUT":
            if "in" not in dictionary:
                raise Exception("Expecting in to be specified")
            inb = dictionary["in"]
            if len(inb.split(";")) > 1:
                raise Exception("Expecting a single file")
            if "replace_path" in kw:
                inb = "/".join([kw["replace_path"], inb.split("/")[-1]])
            return inb
        else:
            return kw["description"].get_binary_path_func()(path, nfiles=1)

    def close(self):
        """CLose file dataset"""
        self._portion.close()

    def remove(self):
        """Remove file"""
        self._portion.remove()


class file_datasets(datasets):
    def __init__(self, paths, intent, **kw):
        """

        paths - Path to the binary
        intent - Intent of the data
        Required if output:
            blk    -  Approximate blocksize
            description - Description object

        """
        self._intent = intent
        portions = []
        beg_locs = [0]
        if intent == "INPUT":
            for pth in paths:
                portions.append(file_portion(pth))
                beg_locs.append(beg_locs[-1] + portions[-1].get_size())

        elif intent == "OUTPUT":
            if "description" not in kw:
                raise Exception("description not specified")
            if "blk" not in kw:
                raise Exception("Must specifty blk")
            blocks = calc_blocks(
                kw["description"].get_hyper().get_ns(),
                converter.get_esize(kw["description"].get_data_type()),
                **kw
            )
            if len(paths) != len(blocks):
                raise Exception("paths not the same size as blocks")
            for path, element in zip(paths, blocks):
                portions.append(file_portion(path))
                beg_locs.append(beg_locs[-1] + element)

        super().__init__(intent, portions, beg_locs, **kw)

    @base_class_doc_string(data_base.__init__)
    def get_data_path(path, intent, **kw):
        """Get binary for dataset writen to binary files"""

        if "description" not in kw:
            raise Exception("Expecting description")
        dictionary = kw["description"].get_dictionary()

        if intent == "INPUT":
            if "in" not in dictionary:
                raise Exception("Expecting in to be specified")
            inb = dictionary["in"]
            if len(inb.split(";")) == 1:
                raise Exception("Expecting multiple files")
            outb = []
            for fl in inb.split(";"):
                if "replace_path" in kw:
                    outb.append("/".join([kw["replace_path"], inb.split("/")[-1]]))
                else:
                    outb.append(fl)
            return outb
        else:
            if "blk" not in kw:
                raise Exception("Must specify blk")
            ns = kw["description"].get_hyper().get_ns()
            esize = converter.get_esize(kw["description"].get_data_type())
            blocks = calc_blocks(ns, esize, **kw)
            return kw["description"].get_binary_path_func()(path, nfiles=len(blocks))


class file_pipe(concat):
    def __init__(self, path, intent, **kw):
        """

        path - File pointer to stdin or stdout
        intent - Intent of the data

        required:
          file_ptr - Required file_ptr

        """
        super.__init__(path, intent, **kw)

    @base_class_doc_string(data_base.__init__)
    def get_data_path(path, intent, **kw):
        """Get binary for dataset writen to binary files"""
        if path == ">" and intent == "INPUT":
            raise Exception("Can't have > and input")
        if path == "<" and intent == "OUTPUT":
            raise Exception("Can't have < and output")
        if path != "<" and path != ">":
            raise Exception("Pipe and intent don't match")
        return "stdin"
