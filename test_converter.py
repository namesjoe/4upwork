import pytest
import numpy as np
import pandas as pd
from converter import Converter
conv = Converter()

class TestCase:

    @pytest.mark.parametrize("expected, x, y", [((3, 3), '/home/sedov/repos/4upwork/test_files/file1.csv',
                             '/home/sedov/repos/4upwork/test_files/file1_csv2json.json')])
    def test_csv_json(self, expected, x, y):
        conv.convert_csv_to_json(x, y)
        assert pd.read_json(y).shape == expected

    @pytest.mark.parametrize("expected, x, y", [(87, '/home/sedov/repos/4upwork/test_files/file2.json',
                             '/home/sedov/repos/4upwork/test_files/file2_json2csv.csv')])
    def test_json_csv(self, expected, x, y):
        conv.convert_json_to_csv(x, y)
        assert pd.read_csv(y, sep=';').age.sum() == expected

    @pytest.mark.parametrize("expected, x, y", [(179, '/home/sedov/repos/4upwork/test_files/file2.json',
                                                '/home/sedov/repos/4upwork/test_files/file2_json2json.json')])
    def test_file_move(self, expected, x, y):
        conv.move_file_wo_convertion(x, y)
        X = open(x, 'r').readlines()
        Y = open(y, 'r').readlines()
        assert X == Y
        assert len(str(Y)) == expected

    @pytest.mark.parametrize("expectation, x, typeX, y, typeY",
                             [(pytest.raises(NotImplementedError), '/home/sedov/repos/4upwork/test_files/file2.json', 'json',
                             '/home/sedov/repos/4upwork/test_files/file2_json2pdf.pdf', 'pdf'),
                              (pytest.raises(NotImplementedError), '/home/sedov/repos/4upwork/test_files/file2.csv','csv',
                             '/home/sedov/repos/4upwork/test_files/file2_csv2pdf.pdf', 'pdf')
                                                                 ]
                             )
    def test_convert_err(self, expectation, x, typeX, y, typeY):
        with expectation as e:
            conv.convert(x, typeX, y, typeY)


