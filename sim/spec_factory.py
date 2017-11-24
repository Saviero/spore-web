import htcondor
import classad
import re
import os

from django.core.files.uploadedfile import UploadedFile
from itertools import product


def save_file(file, dest):
    with open(os.path.join(dest, file.name), 'wb+') as destination:
        for chunk in file.chunks():
            destination.write(chunk)


class Name:

    def __init__(self, name, values):
        self.name = name
        self.values = values


class SpecFactory:

    def __init__(self, template):
        regex_name = r'{\w*}'
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
        missed_names = []
        for name in self.__names:
            if not (name in names):
                missed_names.append(name)
        return missed_names

    def unused_names(self, names):
        unused_names = []
        for name in names:
            if not (name in self.__names):
                unused_names.append(name)
        return unused_names

    def eval_values(self, value_data, file_data):
        # Evaluating values into literal strings
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
        # Get all posible combination of name values and store it as a list of strings
        combos = [list(x) for x in self.__names[0].values]
        for i in range(1, len(self.__names)):
            combos = list(product(combos, self.__names[i].values))
            for j in range(0, len(combos)):
                combos[j] = combos[j][0] + [combos[j][1]]
        for i in range(0, len(combos)):
            combos[i] = self.join_comb(combos[i])
        return combos

    def join_comb(self, name_vals):
        # Concatenate name values and the rest of template
        ans = ''
        for i in range(0, len(name_vals)):
            ans = ans + self.__template_buff[i] + name_vals[i]
        ans += self.__template_buff[-1]
        return ans

    def run_specs(self, exec_data, value_data, file_data):
        # Making working directory TODO: which name is the ClusterId of a job or at least distinct internal ID
        os.makedirs(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'temp'))
        os.chdir(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'temp'))

        # Storing files in directory
        save_file(exec_data['execfile'], os.path.abspath(os.path.dirname(__file__)))
        input_files_names = ''
        for file in file_data:
            save_file(file['file'], os.path.abspath(os.path.dirname(__file__)))
            input_files_names = input_files_names + file['file'].name + ', '

        # Preparing specs

        exec_name = exec_data['execfile'].name

        # Creating a base ClassAd for all jobs
        base_ad = classad.ClassAd({
            'Cmd': os.getcwd() + '/' + exec_name,
            'Out': 'out',
            'Err': 'err',
            'UserLog': os.getcwd() + '/log',
            'TransferInput': input_files_names,
            'Iwd': os.getcwd()
        })

        # Process data and getting all possible args
        self.eval_values(value_data, file_data)
        args = self.get_combinations()

        # Making ads for each arg
        proc_ads = []
        for arg in args:
            proc_ads.append((classad.ClassAd({'Arguments': arg}), 1))

        # Sending jobs to local condor_schedd
        schedd = htcondor.Schedd()
        return schedd.submitMany(base_ad, proc_ads, spool=False)
