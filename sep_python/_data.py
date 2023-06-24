from abc import ABC, abstractmethod
import copy
from numpy import np


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

    def read_portion(self, seeks, blks):
        """
        Read a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read


        Returns a byte array of the given portion of the data

        """

        byte_array = bytearray()
        for seek, blk in zip(seeks, blks):
            byte_array += self._byte_array[seek:seek+blk]
        return byte_array

    def write_portion(self, seeks, blks, byte_array):
        """
        Write a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read
        byte_array - Data to write out

        """

        old = 0
        for seek, blk in zip(seeks, blks):
            new = old + blk
            self._byte_array[seek:seek+blk] = byte_array[old:new]
            old = new


class data_base(ABC):
    """Base class for dealing with binay data"""

    def __init__(self):
        """Base class init"""
        pass

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
    def check_valid(path, intent, **kw):
        """
            Check to make sure that the paramteres passed
            in are valid for the given data type
        """

    @abstractmethod
    def remove(self):
        """Remove the dataset """


class dataset(data_base):
    """"Base class for dataset"""

    def __init__(self, portion: portion_base):
        """
        A dataset that is in a single file
        """
        self._portion = portion

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
    """"Base class for dataset"""

    def __init__(self, portions: list, beg_locs, parallel=False):
        """
        A dataset that is in a single file

        portions - The list of portions that make up the file
        beg_locs - The begining location for each block
        parallel - Whether or not to do the IO in parallel

        """
        self._portions = portions
        self._beg_locs = beg_locs

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
        portions, seeks_out, reads_out, blocks_out = \
            self.seek_io_multi(seeks_in, blk)
        if self._parallel:
            raise Exception("Not implemented yet")
        else:
            old_beg = 0
            for portion, seeks, blks, sz in zip(portions, seeks_out,
                                                reads_out, blocks_out):
                portion(seeks, blks, byte_array[old_beg:old_beg + sz])
                old_beg += sz

    def remove(self):
        """Remove all the portions of a dataset"""
        for port in self._portions:
            port.remove()

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

        if seeks[-1] >= self._beg_loc[-1] or seeks[0] < 0:
            raise Exception("Invalid begining seek location")
        for seek in seeks:
            while icur_file < len(self._beg_locs)-1 and\
             seek > self._beg_locs[icur_file+1]:
                icur_file += 1
            blk_done = 0
            while blk_done < blk_read:
                if icur_file != iold_file:
                    files.append(self._portions[icur_file])
                    seeks_out.append([])
                    reads_out.append([])
                    assigns_out.append([])
                    blocks_out.append(0)
                seeks_out[-1].append(seek-self._beg_loc[icur_file])
                blk = min(seeks_out[icur_file+1], blk_read-blk_done)
                blocks_out[-1] += blk
                reads_out[-1].append(blk)
                blk_done += blk
                if blk_done != blk_read:
                    icur_file += 1
        return files, seeks_out, reads_out, blocks_out


class concat(dataset):
    """Class for when binary follows the description"""

    def __init__(self, path, intent, **kw):

        self._intent = intent
        super.__init__(path)

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
                seek_more = seek-self._cur_pos
                done_seek = 0
                while done_seek < seek_more:
                    bk = min(1024*1024*16, seek_more-done_seek)
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
                from_mem = min(blk, len(byte_array) - seek)
                from_pipe = blk - from_mem
                byte_array += self._binary[seek:seek+from_mem]
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

        beg_pos = 0
        for seek in seeks_in:
            if seek[0] != self._cur_pos:
                raise Exception("Can not seek  on a out pipe")

            self._file_ptr.write(byte_array[beg_pos:beg_pos+blk])
            blk += beg_pos
            self._cur_pos = seek + blk


class inmem_data(inmem_portion):
    """Read write text data"""

    def __init__(self, filename, intent, **kw):
        """

        Initialze a text dataset

        filename - Name of the file to open
        intent      - Intent of file

        data_type - Data type

        Option 1 (reading):
            skip_lines - (optional) Number of lines to skip at the begininig

        Option 2 (writing):
            nbytes - Number of bytes
            header - Optional header to write data

        """

        if "data_type" not in kw:
            raise Exception("Must specify data_type")

        super.__init__(self, filename, kw["data_type"], **kw)
        self._filename = filename

        if "nbytes" in kw:
            super().__init__(sz=kw["nbytes"])
            self._ar = np.frombuffer(self._byte_array, dtype=kw["data_type"])
            self._intent = "OUTPUT"
            self._dtype = kw["data_type"]
            self._header = ""
            if "footer" in kw:
                self._footer = kw["footer"]
        else:
            self._intent = "INPUT"
            if "skip_lines" in kw:
                self._ar = np.loadtxt(self._filename,
                                      dtype=kw["data_type"],
                                      skiprows=kw["skip_lines"])
            else:
                self._ar = np.loadtxt(self._filename, dtype=kw["data_type"])
            super().__init__(byte_array=self._ar.tobytes())

    def close(self):
        if self._intent == "OUTPUT":
            np.savetxt(self._filename, self._ar, footer=self.footer)


class text_data(inmem_portion):
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
            footer - Optional header to write data

        """

        self._filename = filename

        if "data_type" not in kw:
            raise Exception("Must specify data_type")

        if "nbytes" in kw:
            super().__init__(sz=kw["nbytes"])
            self._ar = np.frombuffer(self._byte_array, dtype=kw["data_type"])
            self._intent = "OUTPUT"
            self._dtype = kw["data_type"]
            self._header = ""
            if "header" in kw:
                self._header = kw["header"]
        else:
            self._intent = "INPUT"
            if "skip_lines" in kw:
                self._ar = np.loadtxt(self._filename,
                                      dtype=kw["data_type"],
                                      skiprows=kw["skip_lines"])
            else:
                self._ar = np.loadtxt(self._filename, dtype=kw["data_type"])
            super().__init__(byte_array=self._ar.tobytes())

    def close(self):
        if self._intent == "OUTPUT":
            np.savetxt(self._filename, self._ar, header=self._header)
