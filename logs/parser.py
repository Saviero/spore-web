from decimal import Decimal
import math

from django.db import connection

from .models import Result

def isdecimal(str):
    parts = str.split('.', maxsplit=1)
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return True
    return False

def isfloat(str):
    parts = str.split('e+', maxsplit=1)
    if len(parts) == 1:
        parts = str.split('e-', maxsplit=1)
    if len(parts) == 2 and isdecimal(parts[0]) and parts[1].isdigit():
        return True
    return False

def check_type(str):
    if str.isdigit():
        return 'INTEGER'
    if isdecimal(str):
        precision1 = len(str.split('.')[0]) + 3
        precision2 = len(str.split('.')[1]) + 3
        return 'DECIMAL({},{})'.format(precision1, precision2)
    if isfloat(str):
        return 'FLOAT'
    return 'VARCHAR(max)'

def make_insert(str, template, jobname):
    insert_string = "INSERT INTO " + jobname + "("
    for i in range(len(template)):
        insert_string += template[i][0] + ", "
    insert_string += ") VALUES ("
    data = str.split()
    for i, d in enumerate(data):
        if template[i][1] == "VARCHAR(max)":
            insert_string += "'" + d + "', "
        else:
            insert_string += d + ", "
    insert_string += ")"
    return insert_string

def parse(filename, jobname):
    with open(filename, mode='r') as file:
        lines = file.readlines()

        if lines[0][0] == '#':
            columns = lines[0][1:].strip('[]').split('][')
        else:
            raise SyntaxError

        types = []
        values = lines[1].split()
        for v in values:
            types.append(check_type(v))

        template = zip(columns, types)

        with connection.cursor() as cursor:
            init_string = 'CREATE TABLE ' + jobname + ' ('
            for pair in template:
                init_string += pair[0] + ' ' + pair[2] + ', '
            init_string += ')'
            cursor.execute(init_string)
            for i in range(1, len(lines)):
                cursor.execute(make_insert(lines[i], template, jobname))