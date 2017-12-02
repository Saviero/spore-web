from django.db import models

class FinishedJobs(models.Model):
    job_name = models.CharField(max_length=300, primary_key=True, default='null')
    cluster_id = models.IntegerField()