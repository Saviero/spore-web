import os
from decimal import Decimal

from django.test import TestCase
from django.db import connection

from .parser import parse, check_type, make_insert

class CheckTypeTest(TestCase):
    def test_with_integer(self):
        str = '160'
        res = check_type(str)
        self.assertEqual(res, 'INTEGER')

    def test_with_decimal(self):
        str = '16.0'
        res = check_type(str)
        self.assertEqual(res, 'DECIMAL')

    def test_with_float(self):
        str = '1.58722e+000'
        res = check_type(str)
        self.assertEqual(res, 'FLOAT')

    def test_with_string(self):
        str = 'R_0.17_N_1024_K_171.xpec'
        res = check_type(str)
        self.assertEqual(res, 'VARCHAR(max)')

    def test_with_empty_string(self):
        str = ''
        res = check_type(str)
        self.assertEqual(res, 'VARCHAR(max)')


class MakeInsertTest(TestCase):
    def test_with_string(self):
        with connection.cursor() as cursor:
            template = [('spec', 'VARCHAR(max)'), ('snr', 'DECIMAL(4,4)'), ('schedule', 'INTEGER'), ('fer', 'FLOAT')]
            str = 'R_0.17_N_1024_K_171.xpec 0.0 2 1.31062e-001'
            cursor.execute('CREATE TABLE test (spec VARCHAR(max), snr DECIMAL(4,4), schedule INTEGER, fer FLOAT)')
            cursor.execute(make_insert(str, template, 'test'))
            cursor.execute('SELECT spec, snr, schedule, fer FROM test')
            res = cursor.fetchall()
            expected_res = ('R_0.17_N_1024_K_171.xpec', Decimal(0.0), 2, 1.31062e-001)
            self.assertEqual(res, expected_res)


class ParserTest(TestCase):
    def test_with_existing_file(self):
        filename = 'test_logs_parser.txt'
        jobname = 'test'
        file = open(filename, mode='w')
        file.write('#[spec][snr][schedule][N][K][R][stddev][fer]\n')
        file.write('R_0.17_N_1024_K_171.xpec 0.0 2 1024 171 0.17 1.73036e+000 1.31062e-001\n')
        file.write('R_0.17_N_1024_K_171.xpec 0.0 3 1024 171 0.17 1.73036e+000 1.31062e-001\n')
        file.close()
        answer = parse(filename, jobname)
        self.assertEqual(type(answer), 'NoneType')

        with connection.cursor as cursor:
            cursor.execute('SELECT spec, snr, schedule, N, K, R, stddev, fer FROM test WHERE schedule = 2')
            res1 = cursor.fetchall()
            expected_res1 = ('R_0.17_N_1024_K_171.xpec', Decimal(0.0), 2, 1024, 171, Decimal(0.17), 1.73036e+000, 1.31062e-001)
            self.assertEqual(res1, expected_res1)
            cursor.execute('SELECT spec, snr, schedule, N, K, R, stddev, fer FROM test WHERE schedule = 3')
            res1 = cursor.fetchall()
            expected_res1 = (
            'R_0.17_N_1024_K_171.xpec', Decimal(0.0), 3, 1024, 171, Decimal(0.17), 1.73036e+000, 1.31062e-001)
            self.assertEqual(res1, expected_res1)
        os.remove(filename)
