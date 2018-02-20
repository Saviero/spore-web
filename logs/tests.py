import os
from decimal import Decimal
from time import sleep

from htcondor import Schedd
from classad import ClassAd

from django.core.exceptions import ObjectDoesNotExist
from django.test import TestCase
from django.db import connection
from django.urls import reverse

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

        sleep(15)
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


class LogsTest(TestCase):

    def add_finished_job(self, name, cluster_id):
        job = FinishedJobs(job_name=name, cluster_id=cluster_id)
        job.save()

    def add_unfinished_job(self, name, cluster_id):
        job = JobIdModel(job_name=name, cluster_id=cluster_id)
        job.save()

    def add_dir(self, name):
        if not os.path.exists('{0}/{1}'.format(WORKING_DIRECTORY, name)):
            os.mkdir('{0}/{1}'.format(WORKING_DIRECTORY, name))
            file = open('{0}/{1}/out'.format(WORKING_DIRECTORY, name), mode='w')
            file.close()

    def rm_dir(self, name):
        os.remove('{0}/{1}/out'.format(WORKING_DIRECTORY, name))
        os.rmdir('{0}/{1}'.format(WORKING_DIRECTORY, name))

    def test_logs_with_no_jobs(self):
        response = self.client.get(reverse('logs:index'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'There are no finished jobs.')
        self.assertContains(response, 'There are no unfinished jobs.')
        self.assertQuerysetEqual(response.context['finished_jobs'], [])
        self.assertQuerysetEqual(response.context['unfinished_jobs'], [])

    def test_logs_with_finished_job(self):
        self.add_finished_job('test', 1)
        self.add_dir('test')
        response = self.client.get(reverse('logs:index'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'test')
        self.assertContains(response, 'There are no unfinished jobs.')
        self.assertQuerysetEqual(response.context['finished_jobs'], ["u'test'"])
        self.assertQuerysetEqual(response.context['unfinished_jobs'], [])
        self.rm_dir('test')

    def test_logs_with_unfinished_job(self):
        self.add_unfinished_job('test', 1)
        self.add_dir('test')
        response = self.client.get(reverse('logs:index'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'There are no finished jobs.')
        self.assertContains(response, 'test')
        self.assertQuerysetEqual(response.context['unfinished_jobs'], ["u'test'"])
        self.assertQuerysetEqual(response.context['finished_jobs'], [])
        self.rm_dir('test')

    def test_logs_with_finished_and_unfinished_job(self):
        self.add_unfinished_job('test_unfinished_job', 1)
        self.add_finished_job('test_finished_job', 2)
        self.add_dir('test_finished_job')
        self.add_dir('test_unfinished_job')
        response = self.client.get(reverse('logs:index'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'test_unfinished_job')
        self.assertContains(response, 'test_finished_job')
        self.assertQuerysetEqual(response.context['unfinished_jobs'], ["u'test_unfinished_job'"])
        self.assertQuerysetEqual(response.context['finished_jobs'], ["u'test_finished_job'"])
        self.assertNotContains(response, 'There are no finished jobs.')
        self.assertNotContains(response, 'There are no unfinished jobs.')
        self.rm_dir('test_finished_job')
        self.rm_dir('test_unfinished_job')


class LogsDetailTest(TestCase):

    test_string = 'Hi! I\'m a test string!'

    def add_finished_job(self, name, cluster_id):
        job = FinishedJobs(job_name=name, cluster_id=cluster_id)
        job.save()

    def add_unfinished_job(self, name, cluster_id):
        job = JobIdModel(job_name=name, cluster_id=cluster_id)
        job.save()

    def add_dir(self, name):
        if not os.path.exists('{0}/{1}'.format(WORKING_DIRECTORY, name)):
            os.mkdir('{0}/{1}'.format(WORKING_DIRECTORY, name))
            f = open('{0}/{1}/out'.format(WORKING_DIRECTORY, name), mode='w')
            f.close()
            ff = open('{0}/{1}/log'.format(WORKING_DIRECTORY, name), mode='w')
            ff.write(self.test_string)
            ff.write('\n')
            ff.close()

    def rm_dir(self, name):
        os.remove('{0}/{1}/out'.format(WORKING_DIRECTORY, name))
        os.remove('{0}/{1}/log'.format(WORKING_DIRECTORY, name))
        os.rmdir('{0}/{1}'.format(WORKING_DIRECTORY, name))

    def test_with_unexisting_job(self):
        response = self.client.get(reverse('logs:detail', args=('test',)))
        self.assertEqual(response.status_code, 404)

    def test_with_existing_job(self):
        name = 'test_job'
        self.add_finished_job(name, 15)
        self.add_dir(name)
        response = self.client.get(reverse('logs:detail', args=(name,)))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, name)
        self.assertContains(response, self.test_string)
        self.assertQuerysetEqual(response.context['filename'], "u'{0}/{1}/log".format(WORKING_DIRECTORY, name))
        self.assertQuerysetEqual(response.context['job_name'], "u'{0}".format(name))
        self.rm_dir(name)