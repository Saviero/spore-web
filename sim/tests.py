import htcondor
import os

from time import sleep
from django.test import TestCase, Client
from sim.spec_factory import SpecFactory
from sim.forms import ValueForm
from django.core.files.uploadedfile import UploadedFile
from sim.models import JobIdModel


class SpecFactoryParserTest(TestCase):
    def test_parser_can_parse_template(self):
        template = '{asdf}-c {first} -{second} {123third}'

        factory = SpecFactory(template)
        names = factory.get_names()
        buff = factory.get_buff()

        self.assertEqual(len(names), 4, msg='Incorrect number of names')
        correct_names = ['asdf', 'first', 'second', '123third']
        i = 0
        for name in names:
            self.assertEqual(name.name,
                             correct_names[i],
                             msg='Incorrect name: ' + name.name)
            i += 1
        self.assertEqual(len(buff), 5, msg='Incorrect number of buff elems')
        self.assertEqual(buff,
                         ['', '-c ', ' -', ' ', ''],
                         msg='Incorrect buffer, it is: ' + ' '.join(buff))

    def test_can_eval_data(self):
        template = '{asdf}-c {first} -{second} {123third}'
        factory = SpecFactory(template)
        value_data = [
            {
                'type': 'r',
                'name': 'asdf',
                'args': '1, 5, 1',
            },
            {
                'type': 'v',
                'name': 'first',
                'args': 'hello, world',
            },
            {
                'type': 'r',
                'name': 'second',
                'args': '2, 10, 2',
            }
        ]
        file_data = [
            {
                'name': '123third',
                'file': UploadedFile(name='cats.txt'),
            }
        ]

        factory.eval_values(value_data, file_data)

        names = factory.get_names()
        correct_names = {
            'asdf': ['1', '2', '3', '4'],
            'first': ['hello', 'world'],
            'second': ['2', '4', '6', '8'],
            '123third': ['cats.txt'],
        }
        for name in names:
            self.assertEqual(name.values, correct_names[name.name],
                             msg=('Values does not match for name ' + name.name +
                                  ', expected ' + str(correct_names[name.name]) +
                                  ', got ' + str(name.values) + '\n'
                                  )
                             )

    def test_can_get_combinations(self):
        template = '{asdf}-c {first} -{second} {123third}'
        factory = SpecFactory(template)
        value_data = [
            {
                'type': 'r',
                'name': 'asdf',
                'args': '1, 3, 1',
            },
            {
                'type': 'v',
                'name': 'first',
                'args': 'hello, world',
            },
            {
                'type': 'r',
                'name': 'second',
                'args': '2, 6, 2',
            }
        ]
        file_data = [
            {
                'name': '123third',
                'file': UploadedFile(name='cats.txt'),
            }
        ]
        correct_combs = [
            '1-c hello -2 cats.txt',
            '2-c hello -2 cats.txt',
            '1-c world -2 cats.txt',
            '2-c world -2 cats.txt',
            '1-c hello -4 cats.txt',
            '2-c hello -4 cats.txt',
            '1-c world -4 cats.txt',
            '2-c world -4 cats.txt',
        ]

        factory.eval_values(value_data, file_data)
        combs = factory.get_combinations()
        self.assertEqual(len(combs), 8, msg='Invalid number of combinations; expected 8, got ' + str(len(combs)) +
                                            ' combs:' + str(combs))
        for comb in combs:
            self.assertTrue(comb in correct_combs, msg='Incorrect comb: ' + comb)

    def test_can_submit_job(self):
        template = '{asdf}-c{first}-{second} {123third}'
        factory = SpecFactory(template)
        exec_file = open('sim/static/sim/test_files_spec/helloworld')
        input_file = open('sim/static/sim/test_files_spec/cats.txt')
        value_data = [
            {
                'type': 'r',
                'name': 'asdf',
                'args': '1, 3, 1',
            },
            {
                'type': 'v',
                'name': 'first',
                'args': 'hello, world',
            },
            {
                'type': 'r',
                'name': 'second',
                'args': '2, 6, 2',
            }
        ]
        file_data = [
            {
                'name': '123third',
                'file': UploadedFile(file=input_file, name='cats.txt'),
            }
        ]
        exec_data = {
            'job_name': 'testfactory',
            'exec_file': UploadedFile(file=exec_file, name='helloworld'),
            'arg_template': '{asdf}-c{first}-{second} {123third}'
        }
        correct_combs = [
            '1-chello-2 cats.txt',
            '2-chello-2 cats.txt',
            '1-cworld-2 cats.txt',
            '2-cworld-2 cats.txt',
            '1-chello-4 cats.txt',
            '2-chello-4 cats.txt',
            '1-cworld-4 cats.txt',
            '2-cworld-4 cats.txt',
        ]

        cluster_id = factory.run_specs(exec_data, value_data, file_data)

        exec_file.close()
        input_file.close()
        schedd = htcondor.Schedd()
        # Waiting for jobs to end
        sleep(30)
        # Looking for local job id in cache
        job_name = JobIdModel.objects.get(cluster_id=cluster_id).job_name

        self.assertFalse(job_name is None, msg='Job with cluster_id= '+str(cluster_id) + ' is not in cache')
        # Checking through history for our job
        history = list(schedd.history('ClusterId == ' + str(cluster_id), ['Cmd', 'Arguments', 'TransferInput'], -1))
        self.assertEqual(len(history), 8, msg='Number of jobs is not 8, got jobs: ' + str(history))
        # Expected paths for exec and input files
        exec_path = os.path.abspath(os.path.dirname(__file__)) + '/' + job_name + '/helloworld'
        input_path = 'cats.txt, '
        for job in history:
            self.assertEqual(job['Cmd'],
                             exec_path,
                             msg='Exec file has wrong path; expected ' + exec_path + ', got ' + job['Cmd'])
            self.assertTrue(job['Arguments'] in correct_combs,
                            msg='Argument is not in accepted combs: ' + job['Arguments'])
            self.assertEqual(job['TransferInput'],
                             input_path,
                             msg='Input files have wrong path; expected (' +
                                 input_path + '), got (' + job['TransferInput'] + ')')

    def test_view_can_submit_job(self):
        exec_file = open(os.path.abspath(os.path.dirname(__file__)) + '/static/sim/test_files_view/helloworld')
        input_file = open(os.path.abspath(os.path.dirname(__file__)) + '/static/sim/test_files_view/cats.txt')
        c = Client()
        post_data = {
            'values-1-name': u'two',
            'values-MIN_NUM_FORMS': u'0',
            'values-0-type': u'r',
            'values-0-args': u'1, 5, 1',
            'files-0-name': u'three',
            'values-INITIAL_FORMS': u'0',
            'values-1-args': u'cats, dogs, hamsters',
            'files-MAX_NUM_FORMS': u'1000',
            'submit': u'Start Simulation',
            'values-TOTAL_FORMS': u'2',
            'files-MIN_NUM_FORMS': u'0',
            'files-INITIAL_FORMS': u'0',
            'values-0-name': u'one',
            'values-1-type': u'v',
            'job-arg_template': u'{one}{two} {three}',
            'job-job_name': u'testview',
            'values-MAX_NUM_FORMS': u'1000',
            'files-TOTAL_FORMS': u'1',
            'job-exec_file': UploadedFile(file=exec_file, name='helloworld'),
            'files-0-file': UploadedFile(file=input_file, name='cats.txt')
        }
        correct_combs = [
            '1cats cats.txt',
            '2cats cats.txt',
            '3cats cats.txt',
            '4cats cats.txt',
            '1dogs cats.txt',
            '2dogs cats.txt',
            '3dogs cats.txt',
            '4dogs cats.txt',
            '1hamsters cats.txt',
            '2hamsters cats.txt',
            '3hamsters cats.txt',
            '4hamsters cats.txt',
        ]

        response = c.post('/sim/', post_data)

        exec_file.close()
        input_file.close()
        sleep(30)
        job_id = JobIdModel.objects.get(job_name='testview')
        self.assertEqual(job_id.job_name, 'testview',
                         msg='Name of the job is invalid; expected \'testview\', got \'' + job_id.job_name + '\'')
        schedd = htcondor.Schedd()
        history = list(schedd.history('ClusterId == ' +
                                      str(job_id.cluster_id), ['Cmd', 'Arguments', 'TransferInput'], -1))
        self.assertEqual(len(history), 12, msg='Number of jobs is not 12, got jobs: ' + str(history))
        exec_path = os.path.abspath(os.path.dirname(__file__)) + '/' + job_id.job_name + '/helloworld'
        input_path = 'cats.txt, '
        for job in history:
            self.assertEqual(job['Cmd'],
                             exec_path,
                             msg='Exec file has wrong path; expected ' + exec_path + ', got ' + job['Cmd'])
            self.assertTrue(job['Arguments'] in correct_combs,
                            msg='Argument is not in accepted combs: ' + job['Arguments'])
            self.assertEqual(job['TransferInput'],
                             input_path,
                             msg='Input files have wrong path; expected (' +
                                 input_path + '), got (' + job['TransferInput'] + ')')


class FormTest(TestCase):
    def test_valueform_can_be_validated(self):
        # Invalid form data
        form = ValueForm({
            'type': 'r',
            'name': 'test',
            'args': '1, 2'
        })
        self.assertFalse(form.is_valid())

        # Correct form data
        form = ValueForm({
            'type': 'r',
            'name': 'test',
            'args': '1, 2, 1'
        })
        self.assertTrue(form.is_valid())

