import tempfile
import copy
import numpy as np
from sep_python._data import portion_base, concat, data_base
from sep_python._data import dataset, datasets, inmem_portion
from google.cloud import storage
from sep_python._sep_converter import converter
from sep_python._base_helper import calc_blocks
from sep_python._sep_description import sep_description_base
from sep_python._base_helper import base_class_doc_string


def get_datafile(name, nfiles=1):
    """Creatae the name of data file(s) given the path to the description

    name - Description file name
    nfiles - Number od files

    """
    if nfiles == 1:
        return f"{name}@"
    name = []
    for i in range(nfiles):
        name.append(f"{name}/i")
    return name


class gcs_portion(portion_base):
    """How to do IO on a dataset storead on GCS"""

    def __init__(self, path):
        if path[0:5] != "gs://":
            raise Exception("GCS path must begin with gs://")
        parts = path.split["/"]
        if len(parts) > 4:
            raise Exception("Malformed gcs path must have  bucket and object")
        self._bucket = parts[2]
        self._object = "/".join(parts[3:])

    def read_portion(self, seeks, blks):
        """
        Read a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read


        Returns a byte array of the given portion of the data

        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.get_blob(self._object)

        byte_array = bytearray()
        with blob.open("rb") as file_pointer:
            for seek, blk in zip(seeks, blks):
                file_pointer.seek(seek + self._head)
                byte_array += file_pointer.read(blk)
        return byte_array

    def write_portion(self, seeks, blks, byte_array):
        """
        Write a portion of a dataset from storage source

        seeks - Locations to seek to
        blks - Blocks to read
        byte_array - Data to write out

        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.blob(f"{self._object}")
        with blob.open("wb") as file_pointer:
            old = 0
            for seek, blk in zip(seeks, blks):
                new = old + blk
                file_pointer.write(byte_array[old:new])
                old = new


class gcs_dataset(dataset):
    def __init__(self, path, intent, **kw):
        """

        path - Path to the binary
        intent - Intent of the data

        """
        self._intent = intent
        super.__init__(gcs_portion(path), **kw)

    @base_class_doc_string(data_base.__init__)
    def get_data_path(path, intent, **kw):
        """Get binary for dataset writen to binary file"""
        if "description" not in kw:
            raise Exception("Expecting description")
        if intent == "INPUT":
            if "in" not in kw["description"]:
                raise Exception("Expecting in to be specified")
            inb = kw["description"]["in"]
            if len(inb.split(";")) > 1:
                raise Exception("Expecting a single file")
            if "replace_path" in kw:
                inb = "/".join([kw["replace_path"], inb.split("/")[-1]])
            return inb
        else:
            if kw["description"]._meta:
                return path
            return kw["description"].get_binary_path_func(path, nfiles=1)

    def get_size(self):
        """
        Get size

        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.blob(f"{self._object}")
        return blob.size


class gcs_datasets(datasets):
    def __init__(self, paths, intent, **kw):
        """

        path - Path to the binary
        intent - Intent of the data
        Required if output:
            blk    -  Approximate blocksize
            esize  -  Element size
            ns  -   Number of elements in each dimension

        """
        self._intent = intent
        portions = []
        beg_locs = [0]
        if intent == "INPUT":
            for pth in paths.split(";"):
                portions.append(gcs_portion[pth])
                beg_locs.append(beg_locs[-1] + portions[-1].get_size())

        elif intent == "OUTPUT":
            if "blk" not in kw or "esize" not in kw or "ns" not in kw:
                raise Exception("When specifying output" + " must supply esize,ns,blk")
            if "description" not in kw:
                raise Exception("description not specified")
            blocks = calc_blocks(
                kw["description"].get_hyper().get_ns(),
                converter.get_esize(kw["description"].get_data_type()),
                kw["blk"],
                **kw,
            )
            for pth, element in zip(paths, blocks):
                portions.append(pth)
                beg_locs.append(beg_locs[-1] + element)

        super.__init__(portions, beg_locs, **kw)

    @base_class_doc_string(data_base.__init__)
    def get_data_path(path, intent, **kw):
        """Get binary for dataset writen to binary files"""
        if "description" not in kw:
            raise Exception("Expecting description")
        if intent == "INPUT":
            if "in" not in kw["description"]:
                raise Exception("Expecting in to be specified")
            inb = kw["description"]["in"]
            if len(inb.split(";")) == 1:
                raise Exception("Expecting multiple files")
            outb = []
            for fl in inb.split(";"):
                if "replace_path" in kw:
                    outb.append("/".join([kw["replace_path"], inb.split("/")[-1]]))
            return outb
        else:
            if "blk" not in kw:
                raise Exception("Must specify blk")
            ns = kw["description"].get_hyper().get_ns()
            esize = converter.get_esize(kw["description"].get_data_type())
            blocks = calc_blocks(ns, esize, kw["blk"], **kw)
            return kw["description"].get_binary_path_func(path, nfiles=len(blocks))


class gcs_text_data(inmem_portion):
    """Read write text data"""

    def __init__(self, path, intent, **kw):
        """

        Initialze a text dataset

        path - Name of the file to open
        intent - Intent for file
        data_type      - Data type to read in

        Option 1 (reading):
            skip_lines - (optional) Number of lines to skip at the begininig

        Option 2 (writing):
            nbytes - Number of bytes
            footer - Optional header to write data

        """

        self._tempfile = tempfile.NamedTemporaryFile()
        if path[0:5] != "gs://":
            raise Exception("GCS path must begin with gs://")
        parts = path.split["/"]
        if len(parts) > 4:
            raise Exception("Malformed gcs path must have  bucket and object")
        self._bucket = parts[2]
        self._object = "/".join(parts[3:])

        if "data_type" not in kw:
            raise Exception("Must provide data_elem")

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
            storage_client = storage.Client()
            bucket = storage_client.bucket(self._bucket)
            blob = bucket.get_blob(self._object)
            blob.download_to_filename(self._tempfile)

            if "skip_lines" in kw:
                self._ar = np.loadtxt(
                    self._tempfile, dtype=kw["data_type"], skiprows=kw["skip_lines"]
                )
            else:
                self._ar = np.loadtxt(self._filename, dtype=kw["data_type"])
            super().__init__(byte_array=self._ar.tobytes(), **kw)

    def get_data_path(path, intent, **kw):
        """Check to maske we have a valid path"""
        return path

    def close(self):
        if self._intent == "OUTPUT":
            np.savetxt(self._tempfile, self._ar, header=self._header)
            storage_client = storage.Client()
            bucket = storage_client.bucket(self._bucket)
            blob = bucket.blob(self._object)
            blob.upload_from_filename(self._tempfile)


class gcs_follow_hdr(concat):
    def __init__(self, path, intent, **kw):
        """

        file_ptr - File pointer to stdin or stdout
        intent - Intent of the data

        """
        self._intent = intent

        super.__init__(path, intent, **kw)

    def get_data_path(path, intent, **kw):
        """Check to maske we have a valid path"""
        return "stdin"


class gcs_sep_description_object(sep_description_base):
    @base_class_doc_string(sep_description_base.__init__)
    def __init__(self, path, **kw):
        """Initialie a description using gcs object"""
        super.__init__(path)
        parts = path.split["/"]
        if len(parts) > 4:
            raise Exception("Malformed gcs path must have  bucket and object")
        self._bucket = parts[2]
        self._object = "/".join(parts[3:])
        self._meta = False
        self._binary_path_func = get_datafile

    def read_description(path):
        bucket = path.split("/")[2]
        object = "/".joint(path.split("/"))[3:]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(f"{object}")
        return sep_description_base.read_from_file_descriptor(blob)

    def write_description(self):
        my_str = self.to_string()
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.blob(self._object)
        blob.write(my_str)

    def check_valid(path):
        if path != "gs://":
            raise Exception("Must specify gcs path")

    def remove(self):
        """Remove GCS object assoicated with description"""
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.get_blob(self._object)
        if blob.exists():
            blob.delete()


class gcs_sep_description_metadata(sep_description_base):
    @base_class_doc_string(sep_description_base.__init__)
    def __init__(self, path, intent, **kw):
        """Initialie a description using metadata"""
        super.__init__(path, intent, **kw)
        self._meta = True
        self._binary_path_func = get_datafile

    def read_description(path):
        bucket = path.split("/")[2]
        object = "/".joint(path.split("/"))[3:]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(f"{object}")
        meta = blob.metadata
        pars = {}
        if "history" in pars:
            pars["history"] = meta["history"]
        iax = 1
        if not "data_format" not in meta or "esize" not in meta:
            raise Exception("Must specify data_format and esize")

        pars["data_format"] = meta["data_format"]
        pars["esize"] = meta["esize"]

        while f"n{iax}" in meta:
            pars[f"n{iax}"] = meta[f"n{iax}"]
            pars[f"o{iax}"] = meta[f"o{iax}"] if f"o{iax}" in meta else 0
            pars[f"d{iax}"] = meta[f"d{iax}"] if f"d{iax}" in meta else 1
            pars["label"] = ""
            pars["unit"] = ""
            if f"label{iax}" in meta:
                pars[f"label{iax}"] = meta[f"label{iax}"]
            if f"unit{iax}" in meta:
                pars[f"unit{iax}"] = meta[f"unit{iax}"]
            iax += 1
        return True, pars, pars["data_format"], None, None, None

    def write_description(self):
        tmp = copy.deepcopy(self._dict)
        tmp["esize"] = self._esize
        tmp["data_format"] = converter.get_SEP_name(self.get_data_type())

        self.hyper_to_dict(tmp)
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)
        blob = bucket.blob(self._object)
        blob.metadata = tmp
        blob.patch()

    def check_valid(path):
        if path != "gs://":
            raise Exception("Must specify gcs path")

    def remove(self):
        """Removal, unnessary because of metadata"""
        pass
