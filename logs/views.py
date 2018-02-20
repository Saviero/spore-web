# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os

from django.shortcuts import render
from django.template import loader
from django.http import Http404

from sporeweb.settings import WORKING_DIRECTORY
from sim.models import JobIdModel
from models import FinishedJobs
from available_logs import get_available_finished_jobs
from available_logs import get_available_unfinished_jobs

def contains(model, job_name):
    if model.objects.filter(job_name=job_name).exists():
        return True
    return False

def index(request):
    finished_jobs = get_available_finished_jobs()
    unfinished_jobs = get_available_unfinished_jobs()
    context = {
        'finished_jobs': finished_jobs,
        'unfinished_jobs': unfinished_jobs,
    }
    return render(request, 'logs/index.html', context)


def detail(request, job_name):
    if not os.path.exists('{0}/{1}/log'.format(WORKING_DIRECTORY, job_name))\
            and not (contains(JobIdModel, job_name) or contains(FinishedJobs, job_name)):
        raise Http404('Job does not exist')
    #filename = 'file://{0}/{1}/helloworld'.format(WORKING_DIRECTORY, job_name)
    filename = 'file:///users/liza_moskovskaya/elle.html'
    context = {'filename' : filename,
               'job_name' : job_name,}
    return render(request, 'logs/detail.html', context)