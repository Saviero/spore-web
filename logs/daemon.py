from htcondor import Schedd
from classad import ExprTree
from django.core.exceptions import ObjectDoesNotExist

from sim.models import JobIdModel
from sporeweb.settings import WORKING_DIRECTORY
from models import FinishedJobs
from parser import parse

def check_history():
    schedd = Schedd()
    jobs = list(schedd.history(ExprTree('true'), ["ClusterId"], -1))

    for job in jobs:
        try:
            job_by_id = JobIdModel.objects.get(cluster_id=job["ClusterId"])
        except ObjectDoesNotExist:
            job_by_id = None

        if job_by_id != None:
            filename = WORKING_DIRECTORY + '/{0}/out'.format(job_by_id.job_name)
            parse(filename, job_by_id.job_name)
            finished_job = FinishedJobs(job_name=job_by_id.job_name, cluster_id=job_by_id.cluster_id)
            finished_job.save()
            job_by_id.delete()