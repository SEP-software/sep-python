import os
import re
import logging
import sep_python._sep_converter


converter = sep_python._sep_converter.converter


def get_datapath(host=None, all_paths=None):
    """Return the datapath

    If host is not specified  defaults to the local machine
    If all_paths is specifed returns the list of a ; list of directories

    """

    if host is None:
        hst = os.uname()[1]
    else:
        hst = host

    path = os.environ.get("DATAPATH")
    if not path:
        try:
            file = open(".datapath", "r")
        except IOError:
            try:
                file = open(os.path.join(
                    os.environ.get("HOME"), ".datapath"), "r")
            except IOError:
                file = None
    if file:
        for line in file.readlines():
            check = re.match(r"(?:%s\s+)?datapath=(\S+)" % hst, line)
            if check:
                path = check.group(1)
            else:
                check = re.match(r"datapath=(\S+)", line)
                if check:
                    path = check.group(1)
        file.close()
    if not path:
        path = "/tmp/"
    if all_paths:
        return path.split(":")
    return path


def get_datafile(name, host=None, all_files=None, nfiles=1):
    """Returns the datafile name(s) using SEP datafile conventions

    if host is not specified defaults to local machine
    if all_files is specified and datapath is a ; seperated
        list returns list of paths
    if nfiles is specified returns multi-file names

    """

    filepaths = get_datapath(host, all_files)
    if all_files:
        files_out = []
        for i in range(nfiles):
            for directory in filepaths:
                if i == 0:
                    end = "@"
                else:
                    end = "@" + str(i)
                files_out.append(directory + os.path.basename(name) + end)
        return files_out
    else:
        return filepaths + os.path.basename(name) + "@"
    return filepaths


def database_from_str(string_in: str, data_base: dict):
    """Create a database from string

    string_in - The string we want to parse
    data_base - The output databse (this is a recursive funciton)

    return

    data_base - Output dtabase
    """
    lines = string_in.split("\n")
    parq1 = re.compile(r'([^\s]+)="(.+)"')
    parq2 = re.compile(r"(\S+)='(.+)'")
    par_string = re.compile(r"(\S+)=(\S+)")
    comma_string = re.compile(",")
    for line in lines:
        args = line.split()
        comment = 0
        for arg in args:
            if arg[0] == "#":
                comment = 1
            res = None
            if comment != 1:
                res = parq1.search(arg)
                if res:
                    pass
                else:
                    res = parq2.search(arg)
                    if res:
                        pass
                    else:
                        res = par_string.search(arg)
            if res:
                if res.group(1) == "par":
                    try:
                        file2_open = open(res.group(2), encoding="utf-8")
                    except IOError:
                        logging.getLogger(None).fatal(
                            "Trouble opening %s", res.group(2)
                        )
                        raise Exception("")
                    database_from_str(file2_open.read(), data_base)
                    file2_open.close()
                else:
                    val = res.group(2)
                    if isinstance(val, str):
                        if comma_string.search(val):
                            val = val.split(",")
                    data_base[f"{str(res.group(1))}"] = val
    return data_base