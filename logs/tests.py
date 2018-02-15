import os
from decimal import Decimal
from time import sleep

from htcondor import Schedd
from classad import ClassAd

from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase
from django.db import connection

from sim.models import JobIdModel
from .models import FinishedJobs
from .parser import parse, check_type, make_insert
from .daemon import check_history
from sporeweb.settings import WORKING_DIRECTORY

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


class DaemonTest(TestCase):
    def test_with_processed_jobs(self):
        if not os.path.exists(WORKING_DIRECTORY + '/helloworld'):
            os.mkdir(WORKING_DIRECTORY + '/helloworld')
            file = open(WORKING_DIRECTORY + '/helloworld/out', mode='w')
            file.close()

        ad = ClassAd({'Cmd': (WORKING_DIRECTORY + '/spore-web/logs/static/test_files/helloworld').encode('utf-8'),
            'Out': 'out',
            'Err': 'err',
            'UserLog': (WORKING_DIRECTORY + '/helloworld/log').encode('utf-8'),
            'TransferInput': ' ',
            'Iwd': (WORKING_DIRECTORY+ '/helloworld').encode('utf-8')
        })

        schedd = Schedd()
        job_entry = JobIdModel(job_name=u'helloworld', cluster_id=schedd.submit(ad, spool=False))
        job_entry.save()

        sleep(10)
        check_history()
        try:
            job = FinishedJobs.objects.get(cluster_id=job_entry.cluster_id)
        except ObjectDoesNotExist:
            job = None
        self.assertTrue(job != None)

        os.remove(WORKING_DIRECTORY + '/helloworld/out')
        os.remove(WORKING_DIRECTORY + '/helloworld/log')
        os.remove(WORKING_DIRECTORY + '/helloworld/err')
        os.rmdir(WORKING_DIRECTORY + '/helloworld')
