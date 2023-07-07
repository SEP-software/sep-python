import pytest
import os
from sep_python._sep_helpers import (get_datapath, database_from_str,
                                     get_datafile)
import tempfile


def test_get_datapath():
    # Let's use a temporary directory for this test
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["DATAPATH"] = tmpdir

        assert get_datapath() == tmpdir
        assert get_datapath(all_paths=True) == [tmpdir]


def test_get_datafile():
    # Let's use a temporary directory for this test
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["DATAPATH"] = tmpdir

        filename = "testfile"
        expected_output = tmpdir + filename + "@"
        assert get_datafile(filename) == expected_output

        all_files_output = get_datafile(filename)
        assert all_files_output == expected_output


def test_database_from_str():
    string_in = 'par1="value1"\npar2="value2"'
    expected_output = {"par1": "value1", "par2": "value2"}

    assert database_from_str(string_in, {}) == expected_output

    # Test recursive functionality
    string_in = 'par1="value1"\npar="path_to_file"'
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as f:
        f.write('par2="value2"')
        f.close()

    expected_output = {"par1": "value1", "par2": "value2"}

    with pytest.raises(Exception):  # because the file path is incorrect
        assert database_from_str(string_in, {}) == expected_output
