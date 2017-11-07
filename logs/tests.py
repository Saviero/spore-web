import os
from decimal import Decimal

from django.test import TestCase

from .models import Result
from .parser import parse

class ParserTest(TestCase):
    def test_parsing_from_unexistent_file(self):
        filename = 'test_logs_parser.txt'
        file = open(filename, mode='w')
        file.write('#[spec][snr][schedule][N][K][R][stddev][fer]\n')
        file.write('R_0.17_N_1024_K_171.xpec 0.0 2 1024 171 0.17 1.73036e+000 1.31062e-001\n')
        file.write('R_0.17_N_1024_K_171.xpec 0.0 3 1024 171 0.17 1.73036e+000 1.31062e-001\n')
        file.close()
        parse(filename)
        res1 = Result.objects.get(id=1)
        res2 = Result.objects.get(id=2)

        self.assertEqual(res1.spec, 'R_0.17_N_1024_K_171.xpec')
        self.assertEqual(res1.snr, Decimal('0.0'))
        self.assertEqual(res1.schedule, 2)
        self.assertEqual(res1.n, 1024)
        self.assertEqual(res1.k, 171)
        self.assertEqual(res1.r, Decimal('0.17'))
        self.assertEqual(res1.stddev, 1.73036)
        self.assertEqual(res1.fer, 1.31062e-001)

        self.assertEqual(res2.spec, 'R_0.17_N_1024_K_171.xpec')
        self.assertEqual(res2.snr, Decimal('0.0'))
        self.assertEqual(res2.schedule, 3)
        self.assertEqual(res2.n, 1024)
        self.assertEqual(res2.k, 171)
        self.assertEqual(res2.r, Decimal('0.17'))
        self.assertEqual(res2.stddev, 1.73036)
        self.assertEqual(res2.fer, 1.31062e-001)

        os.remove(filename)