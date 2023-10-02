from abc import ABC, abstractmethod
import copy
import os
import numpy as np
from sep_python._sep_converter import converter
from sep_python._sep_proto import MemReg
from sep_python._base_helper import base_class_doc_string


class portion_base(ABC):
    """Abstract class for storing data"""

    def __init__(self):
        pass

    @abstractmethod
    def read(self, seeks, blks):
        """
        Read a portion of a dataset from storage source
        seeks - Locations to seek to
        blks - Blocks to read

        Returns a byte array of the given portion of the data

        """

    @abstractmethod
    def write(self, seeks, blks, byte_array):
        """
        Write a portion of a dataset from storage source
        seeks - Locations to seek to
        blks - Blocks to read
        byte_array - Data to write out

        """

    @abstractmethod
    def get_size(self):
        """Get the size of the dataset"""

    def close(self):
        """Close the dataset"""
        pass

    @abstractmethod
    def remove(self):
        """Remove the given data block"""


class inmem_portion(portion_base):
    """How to do IO on a dataset stored in memory"""

    def __init__(self, **kw):
        """
        Option 1:
            sz - Amount of memory to use
        Option 2:
            byte_array - The byte array to use

        """
        if "sz" in kw:
            self._byte_array = bytearray(kw["sz"])
        elif "byte_array" in kw:
            self._byte_array = copy.deepcopy(kw["byte_array"])
        else:
            raise Exception("Didn't find any acceptable initializaiton method")

    def read(self, seeks, blks):
        """
        Read a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read


        Returns a byte array of the given portion of the data

        """

        byte_array = bytearray()
        for seek, blk in zip(seeks, blks):
            byte_array += self._byte_array[seek : seek + blk]
        return byte_array

    def write(self, seeks, blks, byte_array):
        """
        Write a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read
        byte_array - Data to write out

        """

        old = 0

        for seek, blk in zip(seeks, blks):
            new = old + blk
            self._byte_array[seek : seek + blk] = byte_array[old:new]
            old = new

    def get_size(self):
        """Get the size of the data"""
        return len(self._byte_array)

    def close(self):
        """Close file"""
        pass

    def remove(self):
        """Remove portion"""
        pass


class data_base(ABC):
    """Base class for dealing with binay data"""

    def __init__(self, **kw):
        """Base class init"""
        self._binary = None
        if "description" not in kw:
            raise Exception("Expecting descrption to be set")
        self._description = kw["description"]

    def get_binary_str(self):
        """Return binary"""
        bin = self.get_binary()
        if isinstance(bin, str):
            return bin
        elif isinstance(bin, list):
            return ":".join(bin)

    def get_binary(self):
        if self._binary is None:
            raise Exception("binary not set")
        return self._binary

    def set_binary(self, binary):
        """Set binary"""
        self._binary = binary

    @abstractmethod
    def read(self, seeks, blk):
        """
        read from storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        """

    @abstractmethod
    def write(self, seeks, blk, buffer):
        """
        write to storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        buffer - Bytes buffer containing what to write
        """

    @abstractmethod
    def get_data_path(path, intent, **kw):
        """
        Get the binary associated with a given description
        """

    @abstractmethod
    def remove(self):
        """Remove the dataset"""


class dataset(data_base):
    """ "Base class for dataset"""

    def __init__(self, intent, portion: portion_base, **kw):
        """
        A dataset that is in a single file

            intent - Intent for dataset
            portion - Dataset storage object

        """
        self._portion = portion
        self._intent = intent
        super().__init__(**kw)

    def read(self, seeks, blk):
        """
        read from storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        """
        return self._portion.read(seeks, [blk] * len(seeks))

    def write(self, seeks, blk, byte_array):
        """
        write to storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        byte_array - Byte array containing what to write
        """
        self._portion.write(seeks, [blk] * len(seeks), byte_array)

    def remove(self):
        """Remove the given data"""
        self._portion.remove()


class datasets(data_base):
    """ "Base class for dataset"""

    def __init__(self, intent, portions: list, beg_locs, parallel=False, **kw):
        """
        A dataset that is in a single file
        intent   - Intent for dataset
        portions - The list of portions that make up the file
        beg_locs - The begining location for each block
        parallel - Whether or not to do the IO in parallel

        """
        self._portions = portions
        self._beg_locs = beg_locs
        self._parallel = parallel
        if "description" not in kw:
            raise Exception("Expecting description to be set")

    def read(self, seeks_in, blk):
        """
        read from storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        """
        portions, seeks_out, reads_out = self.seek_io_multi(seeks_in, blk)[0:3]
        if self._parallel:
            raise Exception("Not implemented yet")
        else:
            array = bytearray()
            for portion, seeks, blks in zip(portions, seeks_out, reads_out):
                array += portion.read(seeks, blks)

        return array

    def write(self, seeks_in, blk, byte_array):
        """
        write to storage to a 1-D numpy vector

        seeks_in - The seeks to do to the given dataset
        blk   - The block size for each read
        byte_array - Byte array
        """
        portions, seeks_out, reads_out, blocks_out = self.seek_io_multi(seeks_in, blk)
        if self._parallel:
            raise Exception("Not implemented yet")
        else:
            old_beg = 0
            for portion, seeks, blks, sz in zip(
                portions, seeks_out, reads_out, blocks_out
            ):
                portion.write(seeks, blks, byte_array[old_beg : old_beg + sz])
                old_beg += sz

    def remove(self):
        """Remove all the portions of a dataset"""
        for port in self._portions:
            port.remove()

    def close(self):
        """ "Close the file"""
        for port in self._portions:
            port.close()

    def seek_io_multi(self, seeks, blk_read):
        """
        Figure out data store for each part of


        seeks - List containing the seek positions
        blk_read - The amount to read of each block [integer]


        Returns
        files - List of storage locatsions
        seeks_out - 2 D list [outer files, inner seeks within file]
        reads_out - 2-D list [outer files, inners reads]
        assigns_out - 2-D list [outer files, inner assigns]

        """
        files = []
        icur_file = 0
        iold_file = -1
        seeks_out = []
        reads_out = []
        assigns_out = []
        blocks_out = []
        if seeks[-1] >= self._beg_locs[-1] or seeks[0] < 0:
            raise Exception("Invalid begining seek location")
        for seek in seeks:
            while (
                icur_file < len(self._beg_locs) - 1
                and seek >= self._beg_locs[icur_file + 1]
            ):
                icur_file += 1
            blk_done = 0
            while blk_done < blk_read:
                if icur_file != iold_file:
                    files.append(self._portions[icur_file])
                    seeks_out.append([])
                    reads_out.append([])
                    assigns_out.append([])
                    blocks_out.append(0)
                seeks_out[-1].append(seek - self._beg_locs[icur_file] + blk_done)
                blk = min(
                    self._beg_locs[icur_file + 1] - self._beg_locs[icur_file],
                    blk_read - blk_done,
                )
                blocks_out[-1] += blk
                reads_out[-1].append(blk)
                blk_done += blk
                if blk_done != blk_read:
                    icur_file += 1
        return files, seeks_out, reads_out, blocks_out


class concat(dataset):
    """Class for when binary follows the description"""

    def __init__(self, path, intent, **kw):
        super().__init__(intent, path, **kw)

        if "file_ptr" not in kw:
            raise Exception("file_ptr must be specified")

        self._file_ptr = kw["file_ptr"]

        self._cur_pos = 0
        self._intent = intent
        if intent == "INPUT":
            if "binary" not in kw:
                raise Exception("binary must be passed when input")
            self._binary = kw["binary"]

    def read(self, seeks_in, blk):
        """
        read from storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        """
        byte_array = bytearray()
        if self._cur_pos > seeks_in[0]:
            raise Exception("Can not seek backwards on input pipe")

        for seek in seeks_in:
            # SEEK TO BEGINING OF READ, SKIPPING IF NECESSARY
            if self._cur_pos != seek:
                seek_more = seek - self._cur_pos
                done_seek = 0
                while done_seek < seek_more:
                    bk = min(1024 * 1024 * 16, seek_more - done_seek)
                    self._file_ptr.read(bk)
                    done_seek += bk

            if seek > len(self._binary):
                byte_array += self._file_pointer.read(blk)
            else:
                #
                #  ex: binary_len==1000
                #
                # blk=50, seek=990
                #
                # 10 from memory, 40 from
                from_mem = min(blk, len(self._binary) - seek)
                from_pipe = blk - from_mem
                byte_array += self._binary[seek : seek + from_mem]
                byte_array += self._file_ptr.read(from_pipe)
            self._cur_pos = seek + blk
        return byte_array

    def write(self, seeks_in, blk, byte_array):
        """
        write to storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        byte_array - Byte array to write out
        """
        if not self._description._written:
            self._description.close()
        beg_pos = 0
        for seek in seeks_in:
            if seek != self._cur_pos:
                raise Exception("Can not seek  on a out pipe")
            self._file_ptr.write(byte_array[beg_pos : beg_pos + blk])
            beg_pos += blk
            self._cur_pos = seek + blk

    @base_class_doc_string(data_base.__init__)
    def get_data_path(path, intent, **kw):
        """Get binary for concatenated data"""
        if intent == "INPUT":
            if "description" not in kw:
                raise Exception("Expecting description kw")
            mydict = kw["description"].get_dictionary()
            if "in" not in mydict:
                raise Exception("in must be in description, not a valid SEP file")
            if mydict["in"] != "stdin":
                raise Exception("Expecting stdin when piping")
        return "stdin"

    def close(self):
        """Close concat file"""
        pass


class inmem_data(dataset):
    """Read write text data"""

    def __init__(self, filename, intent, **kw):
        """

        Initialze a an in memory datawset

        filename - Name of the file to open
        intent      - Intent of file


        Required description


        """

        if "description" not in kw:
            raise Exception("Expecting description in initialization")

        hyper = kw["description"].get_hyper()
        esize = converter.get_esize(kw["description"].get_data_type())

        nbuf = hyper.get_n123() * esize

        if "mem" in kw:
            mem = kw["mem"]
            if isinstance(mem, MemReg):
                array = mem.get_nd_array()
            elif isinstance(mem, np.ndarray):
                array = mem
            else:
                raise Exception("Do not how to read into type %s ", type(mem))
            byte_array = array.tobytes()
            if len(byte_array) != nbuf:
                raise Exception("mem passed not equal to hyoer/n123")
        else:
            byte_array = bytearray(nbuf)
        super().__init__(intent, inmem_portion(byte_array=byte_array), **kw)

    def read(self, seeks, blk):
        """
        read from storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        """
        return self._portion.read(seeks, [blk] * len(seeks))

    def write(self, seeks, blk, byte_array):
        """
        write to storage to a 1-D numpy vector

        seeks - The seeks to do to the given dataset
        blk   - The block size for each read
        byte_array - Byte array containing what to write
        """
        if not self._description._written:
            self._description.close()
        self._portion.write(seeks, [blk] * len(seeks), byte_array)

    def get_data_path(path, intent, **kw):
        """Check to maske we have a valid path"""
        return path

    def remove(self):
        """Remove the given data"""
        pass

    def close(self):
        """Close in-memory dataset"""
        pass


class text_data(inmem_data):
    """Read write text data"""

    def __init__(self, filename, intent, **kw):
        """

        Initialze a text dataset

        filename - Name of the file to open
        intent   - Intent for file
        data_type-      - Data type to read in

        Option 1 (reading):
            skip_lines - (optional) Number of lines to skip at the begininig

        Option 2 (writing):
            nbytes - Number of bytes
            header - Optional header to write data

        """

        self._filename = filename

        if "description" not in kw:
            raise Exception("Must specify description")

        self._data_type = kw["description"].get_data_type()
        hyper = kw["description"].get_hyper()
        n123 = hyper.get_n123()
        self._header = ""

        if "header" in kw:
            self._header = kw["header"]

        if intent == "OUTPUT":
            super().__init__(filename, intent, **kw)
        else:
            if "skip_lines" in kw:
                ar = np.loadtxt(
                    self._filename, dtype=self._data_type, skiprows=kw["skip_lines"]
                )
            else:
                ar = np.loadtxt(self._filename, dtype=self._data_type)
            ar2 = np.ravel(ar)
            if ar2.shape[0] != n123:
                raise Exception("Text file size doesn't match")
            super().__init__(filename, intent, mem=ar2, **kw)

    def close(self):
        if self._intent == "OUTPUT":
            ar = np.frombuffer(self._portion._byte_array, self._data_type)
            np.savetxt(self._filename, ar, footer=self._header)

    def remove(self):
        os.remove(self._filename)
