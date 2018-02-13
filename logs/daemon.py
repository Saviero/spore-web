from htcondor import Schedd
from classad import ExprTree

from sim.models import JobIdModel
from models import FinishedJobs
from parser import parse

def check_history():
    schedd = Schedd()
    jobs = list(schedd.history(ExprTree(''), ["ClusterId"], -1))

    for job in jobs:
        job_by_id = JobIdModel.objects.all(cluster_id=job["ClusterId"])
        if job_by_id.exists():
            filename = '/{0}/out'.format(job_by_id.job_name)
            parse(filename, job_by_id.job_name)
            finished_job = FinishedJobs(job_name=job_by_id.job_name, cluster_id=job_by_id.cluster_id)
            finished_job.save()
            job_by_id.delete()