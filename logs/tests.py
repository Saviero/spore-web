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
        self.assertEqual(res, 'VARCHAR')

    def test_with_empty_string(self):
        str = ''
        res = check_type(str)
        self.assertEqual(res, 'VARCHAR')


class MakeInsertTest(TestCase):
    def test_with_string(self):
        with connection.cursor() as cursor:
            template = [('spec', 'VARCHAR(25)'), ('snr', 'DECIMAL(1,2)'), ('schedule', 'INTEGER'), ('fer', 'FLOAT')]
            jobname = 'test'
            str = 'R_0.17_N_1024_K_171.xpec 0.25 2 1.31062e-001'
            res = make_insert(str, template, jobname)
            expected_res = "INSERT INTO test(spec, snr, schedule, fer) " \
                           "VALUES ('R_0.17_N_1024_K_171.xpec', 0.25, 2, 1.31062e-001)"
            self.assertEqual(res, expected_res)


class ParserTest(TestCase):
    def test_with_existing_file(self):
        filename = 'test_logs_parser.txt'
        jobname = 'test'
        file = open(filename, mode='w')
        file.write('#[spec][snr][schedule][N][K][R][stddev][fer]\n')
        file.write('R_0.17_N_1024_K_171 0.0 2 1024 171 56.17 1.73036e+000 1.31062e-001\n')
        file.write('R_0.17_N_1024_K_171.xpec 0.25 3 1024 171 4.17 1.73036e+000 1.31062e-001\n')
        file.close()
        parse(filename, jobname)

        with connection.cursor() as cursor:
            cursor.execute('SELECT spec, snr, schedule, N, K, R, stddev, fer FROM test WHERE schedule = 2')
            res1 = cursor.fetchall()
            expected_res1 = [(u'R_0.17_N_1024_K_171', Decimal('0'), 2,
                              1024, 171, Decimal('56.17'), 1.73036, 0.131062)]
            self.assertEqual(res1, expected_res1)
            cursor.execute('SELECT spec, snr, schedule, N, K, R, stddev, fer FROM test WHERE schedule = 3')
            res1 = cursor.fetchall()
            expected_res1 = [(u'R_0.17_N_1024_K_171.xpec', Decimal('0.25'), 3,
                              1024, 171, Decimal('4.17'), 1.73036, 0.131062)]
            self.assertEqual(res1, expected_res1)
        os.remove(filename)
