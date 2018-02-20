import htcondor
import classad
import re
import os

from itertools import product
from sim.models import JobIdModel
from django.db import IntegrityError


def save_file(file, dest):
    """ Saving Django file in specified destination """
    with open(os.path.join(dest, file.name), 'wb+') as destination:
        for chunk in file.chunks():
            destination.write(chunk)


class JobNameDuplicateError(Exception):
    pass

class Name:
    """ A class for pairs <name : values>, where 'name' - name of a argument template argument,
    'values' - list of all possible string values for this name."""
    def __init__(self, name, values):
        self.name = name
        self.values = values


class SpecFactory:
    """
    This class parses argument template, generates all possible arguments with
    this template (i.e. cartesian product of all argument elements),
    and launches HTCondor jobs for some executable with said arguments.
    """

    def __init__(self, template):
        """
        Creates an instance of SpecFactory class by parsing an argument template.
        :param template: string of argument template, with names matching r'{\w+}
        """
        regex_name = r'{\w+}'
        self.__template_buff = re.split(regex_name, template)
        names = re.findall(regex_name, template)
        for i in range(len(names)):
            names[i] = names[i].replace('{', '')
            names[i] = names[i].replace('}', '')
        self.__names = [Name(name, []) for name in names]
        self.__specs = []

    def get_names(self):
        return self.__names

    def get_buff(self):
        return self.__template_buff

    def check_names(self, names):
        """
        Checking if all parsed names are in input names.
        :param names: input names
        :return: A list of names which was not in the template
        """
        missed_names = []
        for name in self.__names:
            if not (name.name in names):
                missed_names.append(name.name)
        return missed_names

    def unused_names(self, names):
        """
        Checking if any names were not used in the template.
        :param names: input names
        :return: names unused in the template
        """
        unused_names = []
        for name in names:
            is_in = False
            for saved_name in self.__names:
                if name == saved_name.name:
                    is_in = True
                    break
            if not is_in:
                unused_names.append(name)
        return unused_names

    def eval_values(self, value_data, file_data):
        """
        Evaluating input values into actual strings and assigning them to names.
        As input can be range function arguments or arbitrary values
        separated by comma, this function produces lists of strings with actual values,
        (i.e. range function args '1, 5, 2' into ['1', '3'], and arbitrary values 'abc, def' into ['abc', 'def'])
        and then assigning them to respective names.
        :param value_data: a dictionary representation of sim.forms.ValueForm
        :param file_data: a dictionary representation of sim.forms.FileForm
        """
        name_vals = {}
        for value in value_data:
            if not value['name'] in name_vals.keys():
                name_vals[value['name']] = []
            if value['type'] == 'r':
                args = value['args']
                start, end, inc = map(int, args.split(','))
                for i in range(start, end, inc):
                    name_vals[value['name']].append(str(i))
            if value['type'] == 'v':
                for val in value['args'].replace(' ', '').split(','):
                    name_vals[value['name']].append(val)
        for file in file_data:
            if not file['name'] in name_vals.keys():
                name_vals[file['name']] = []
            name_vals[file['name']].append(file['file'].name)
        for i in range(0, len(self.__names)):
            if self.__names[i].name in name_vals.keys():
                self.__names[i].values = name_vals[self.__names[i].name]

    def get_combinations(self):
        """
        Getting all possible combination of argument strings, using stored names' values.
        :return: A list of all possible argument strings
        """
        if len(self.__names) == 0:
            combos = [[]]
        else:
            combos = [list(x) for x in self.__names[0].values]
            for i in range(1, len(self.__names)):
                combos = list(product(combos, self.__names[i].values))
                for j in range(0, len(combos)):
                    combos[j] = combos[j][0] + [combos[j][1]]
        for i in range(0, len(combos)):
            combos[i] = self.join_comb(combos[i])
        return combos

    def join_comb(self, name_vals):
        """
        Joining provided values for names with the rest of a template, producing final argument string.
        :param name_vals: A list of name values in appearance order
        :return:
        """
        ans = ''
        for i in range(0, len(name_vals)):
            ans = ans + self.__template_buff[i] + name_vals[i]
        ans += self.__template_buff[-1]
        return ans

    def run_specs(self, exec_data, value_data, file_data):
        """
        Launching HTCondor jobs with provided executable, name values and files.
        Storing local ID for a launch and created Cluster ID in sim.models.JobIdModel.
        :param exec_data: a dictionary representation of sim.forms.JobForm
        :param value_data: a dictionary representation of sim.forms.ValueForm
        :param file_data: a dictionary representation of sim.forms.FileForm
        :return: Cluster ID of a newly created job
        """
        # Getting new ID entry
        try:
            job_entry = JobIdModel(exec_data['job_name'], cluster_id=-1)
        except IntegrityError:
            raise JobNameDuplicateError('This job name already exist!')
        job_entry.save()

        # Making working directory and changing location to it
        os.makedirs(os.path.join(os.path.abspath(os.path.dirname(__file__)), exec_data['job_name']))
        os.chdir(os.path.join(os.path.abspath(os.path.dirname(__file__)), exec_data['job_name']))

        # Storing files in directory
        save_file(exec_data['exec_file'], os.getcwd())
        input_files_names = ''
        for file in file_data:
            save_file(file['file'], os.getcwd())
            input_files_names = input_files_names + file['file'].name + ', '

        # Preparing specs

        exec_name = exec_data['exec_file'].name

        # Creating a base ClassAd for all jobs
        base_ad = classad.ClassAd({
            'Cmd': (os.getcwd() + '/' + exec_name).encode('utf-8'),
            'Out': 'out',
            'Err': 'err',
            'UserLog': (os.getcwd() + '/log').encode('utf-8'),
            'TransferInput': input_files_names.encode('utf-8'),
            'Iwd': os.getcwd().encode('utf-8')
        })

        # Process data and getting all possible args
        self.eval_values(value_data, file_data)
        args = self.get_combinations()

        # Making ads for each arg
        proc_ads = []
        for arg in args:
            proc_ads.append((classad.ClassAd({'Arguments': arg.encode('utf-8')}), 1))

        if len(proc_ads) == 0:
            proc_ads = [(classad.ClassAd({'Arguments': ''}), 1)]

        # Sending jobs to local condor_schedd
        schedd = htcondor.Schedd()
        cluster_id = schedd.submitMany(base_ad, proc_ads, spool=False)

        job_entry.cluster_id = cluster_id
        job_entry.save()

        return cluster_id
