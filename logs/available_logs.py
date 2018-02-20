import os

from models import FinishedJobs
from sim.models import JobIdModel
from sporeweb.settings import WORKING_DIRECTORY

def get_available_finished_jobs():
    dirnames = []
    jobs = list(FinishedJobs.objects.order_by('job_name').all())
    for job in jobs:
        if os.path.exists('{0}/{1}/out'.format(WORKING_DIRECTORY, job.job_name)):
            dirnames.append(job.job_name)
    return dirnames


def get_available_unfinished_jobs():
    dirnames = []
    jobs = list(JobIdModel.objects.order_by('job_name').all())
    for job in jobs:
        if os.path.exists('{0}/{1}/out'.format(WORKING_DIRECTORY, job.job_name)):
            dirnames.append(job.job_name)
    return dirnames