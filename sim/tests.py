import htcondor
import os

from time import sleep
from django.test import TestCase
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
                'file': UploadedFile(file=open('sim/static/sim/test_files/cats.txt'), name='cats.txt'),
            }
        ]
        exec_data = {
            'execfile': UploadedFile(file=open('sim/static/sim/test_files/helloworld'), name='helloworld')
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

        schedd = htcondor.Schedd()
        # Waiting for jobs to end
        sleep(30)
        # Looking for local job id in cache
        job_id = JobIdModel.objects.get(cluster_id=cluster_id).id

        self.assertFalse(job_id is None, msg='Job with cluster_id= '+str(cluster_id) + ' is not in cache')
        # Checking through history for our job
        history = list(schedd.history('ClusterId == ' + str(cluster_id), ['Cmd', 'Arguments', 'TransferInput'], -1))
        self.assertEqual(len(history), 8, msg='Number of jobs is not 8, got jobs: ' + str(history))
        # Expected paths for exec and input files
        exec_path = os.path.abspath(os.path.dirname(__file__)) + '/' + str(job_id) + '/helloworld'
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
