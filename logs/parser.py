from decimal import Decimal

from .models import Result

def parse(filename):
    try:
        with open(filename, mode='r') as file:
            for line in file:
                if line != '#[spec][snr][schedule][N][K][R][stddev][fer]':
                    parameters = line.split(' ')
                    try:
                        res = Result(spec=parameters[0],
                                     snr=Decimal(parameters[1]),
                                     schedule=int(parameters[2]),
                                     n=int(parameters[3]),
                                     k=int(parameters[4]),
                                     r=Decimal(parameters[5]),
                                     stddev=float(parameters[6]),
                                     fer=float(parameters[7]))
                        res.save()
                    except IndexError:
                        print 'An unexpected format in line'
    except IOError:
        print 'Could not read file {0}'.format(filename)