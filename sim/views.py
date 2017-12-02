import gettext

from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.forms import formset_factory
from sim.forms import ValueForm, AddFileForm, JobForm
from django.core.exceptions import ValidationError
from spec_factory import SpecFactory, JobNameDuplicateError
from django.core.urlresolvers import reverse


def index(request):
    ValueFormSet = formset_factory(ValueForm, extra=1)
    FileFormSet = formset_factory(AddFileForm, extra=1)
    missed_names = []
    if request.method == 'POST':
        value_formset = ValueFormSet(request.POST, request.FILES, prefix='values')
        file_formset = FileFormSet(request.POST, request.FILES, prefix='files')
        job_form = JobForm(request.POST, request.FILES, prefix='job')
        # Checking forms' data
        if value_formset.is_valid() and file_formset.is_valid() and job_form.is_valid():
            # Parsing argument template
            factory = SpecFactory(job_form.cleaned_data['arg_template'])
            # Gathering all elements' names from valueforms and fileforms and checking with parsed ones
            names = []
            valid = True
            for i in range(0, len(value_formset)):
                if value_formset[i].cleaned_data['name'] in names:
                    value_formset[i].add_error('name', ValidationError(
                        gettext.gettext('Name already used'),
                        code='duplicate_value_name'))
                    valid = False
                names.append(value_formset[i].cleaned_data['name'])
            for i in range(0, len(file_formset)):
                if file_formset[i].cleaned_data['name'] in names:
                    file_formset[i].add_error('name', ValidationError(
                        gettext.gettext('Name already used'),
                        code='duplicate_value_name'))
                    valid = False
                names.append(file_formset[i].cleaned_data['name'])
            if valid:
                missed_names = factory.check_names(names)
                if len(missed_names) == 0:
                    # All names in template are covered
                    unused_names = factory.unused_names(names)
                    value_data = []
                    file_data = []
                    for form in value_formset:
                        # Not using unused data
                        if not form.cleaned_data['name'] in unused_names:
                            value_data.append(form.cleaned_data)
                    for form in file_formset:
                        file_data.append(form.cleaned_data)
                    try:
                        factory.run_specs(job_form.cleaned_data, value_data, file_data)
                    except JobNameDuplicateError:
                        pass # TODO Figure out what to do with such exception, though it should never be raised
                    return HttpResponseRedirect(
                        reverse('success', kwargs={'job_name':job_form.cleaned_data['job_name']})
                    )
    else:
        value_formset = ValueFormSet(prefix='values')
        file_formset = FileFormSet(prefix='files')
        job_form = JobForm(prefix='job')
    context = {'arg_forms': value_formset, 'file_forms': file_formset,
               'job_form': job_form, 'missed_names': missed_names}
    return render(request, 'sim/index.html', context)


def success(request, job_name):
    return render(request, 'sim/success.html', {'job_name': job_name})