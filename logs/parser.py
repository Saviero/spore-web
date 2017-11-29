import re

from django.db import connection


def isdecimal(str):
    parts = str.split('.')
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return True
    return False


def isfloat(str):
    parts = str.split('e+')
    if len(parts) == 1:
        parts = str.split('e-')
    if len(parts) == 2 and isdecimal(parts[0]) and parts[1].isdigit():
        return True
    return False


def check_type(str):
    if str.isdigit():
        return 'INTEGER'
    if isdecimal(str):
        return 'DECIMAL'
    if isfloat(str):
        return 'FLOAT'
    return 'VARCHAR'


def make_insert(str, template, jobname):
    #Creates an SQL query, which inserts row of values into the table

    insert_string = "INSERT INTO " + jobname + "("
    for t in template[:-1]:
        insert_string += t[0] + ", "    #adds columns names
    insert_string += template[-1][0] + ") VALUES ("
    data = str.split()

    #if VARCHAR value is added, surrounds it with quotes
    #else lives it unchanged
    for i, d in enumerate(data[:-1]):
        if re.match(r'VARCHAR', template[i][1]):
            insert_string += "'" + d + "', "
        else:
            insert_string += d + ", "
    if re.match(r'VARCHAR', template[-1][1]):
        insert_string += "'" + data[-1] + "')"
    else:
        insert_string += data[-1] + ")"
    return insert_string


def parse(filename, jobname):
    with open(filename, mode='r') as file:
        lines = file.readlines()

        #checks if the syntax is correct
        if lines[0][0] == '#':
            columns = lines[0][1:].strip('[]\n').split('][')
        else:
            raise SyntaxError

        types = []
        values = lines[1].split()
        for v in values:
            types.append(check_type(v))

        #creates a list, containing max length for VARCHAR values and max precisions for DECIMAL ones
        max_length = []
        for t in types:
            if t == 'VARCHAR':
                max_length.append(0)
            elif t == 'DECIMAL':
                max_length.append([0,0])
            else:
                max_length.append(None)

        #finds max length and precisions
        for line in lines[1:]:
            values = line.split()
            for i, v in enumerate(values):
                if type(max_length[i]) is int and max_length[i] < len(v):
                    max_length[i] = len(v)
                elif type(max_length[i]) is list and max_length[i][0] < len(v.split('.')[0]):
                    max_length[i][0] = len(v.split('.')[0])
                elif type(max_length[i]) is list and max_length[i][1] < len(v.split('.')[1]):
                    max_length[i][1] = len(v.split('.')[1])

        #adds obtained max length and precisions to type declarations
        for i, m in enumerate(max_length):
            if type(m) is int:
                types[i] += '(' + str(m) + ')'
            elif type(m) is list:
                types[i] += '(' + str(m[0]) + ',' + str(m[1]) + ')'

        template = zip(columns, types)

        with connection.cursor() as cursor:
            #concatenates an SQL query for table creation
            init_string = 'CREATE TABLE ' + jobname + ' ('
            for pair in template[:-1]:
                init_string += pair[0] + ' ' + pair[1] + ', '
            init_string += template[-1][0] + ' ' + template[-1][1] + ')'
            cursor.execute(init_string)
            # adds rows to the table
            for line in lines[1:]:
                cursor.execute(make_insert(line, template, jobname))