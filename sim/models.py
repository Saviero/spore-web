from django.db import models

class Configuration(models.Model):
    """ Model of a saved configuration """
    name = models.CharField(max_length=200)
    arg_template = models.CharField(max_length=300)

    def __str__(self):
        return self.name

class Argument(models.Model):
    """ Model of a defined argument in a saved configuration """
    ARG_TYPE_CHOICE = (
        ('r', 'Range'),
        ('v', 'List of values'),
        ('f', 'List of files'),
    )
    name = models.CharField(max_length=50)
    arg_type = models.CharField(max_length=1, choices=ARG_TYPE_CHOICE)
    arg_value = models.CharField(max_length=300)
    configuration = models.ForeignKey(Configuration, on_delete=models.CASCADE)

    def __str__(self):
        return self.name


class JobIdModel(models.Model):
    """ This model stores created cluster_ids for further use. Id of a model is a local id of a job. """
    job_name = models.CharField(max_length=300, primary_key=True, default='null')
    cluster_id = models.IntegerField()
