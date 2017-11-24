from django.db import models

class Result(models.Model):
    spec = models.CharField(max_length=40)
    snr = models.DecimalField(max_digits=5, decimal_places=3)
    schedule = models.IntegerField()
    n = models.IntegerField()
    k = models.IntegerField()
    r = models.DecimalField(max_digits=5, decimal_places=3)
    stddev = models.FloatField()
    fer = models.FloatField()

    def __str__(self):
        return self.spec