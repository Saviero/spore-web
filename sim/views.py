from django.shortcuts import render, redirect
from django.forms import formset_factory
from sim.forms import ValueForm, AddFileForm, ExecutableFileForm, ArgTemplateForm
from spec_factory import SpecFactory


def index(request):
    ValueFormSet = formset_factory(ValueForm, extra=1)
    FileFormSet = formset_factory(AddFileForm, extra=1)
    if request.method == 'POST':
        value_formset = ValueFormSet(request.POST, request.FILES, prefix='values')
        file_formset = FileFormSet(request.POST, request.FILES, prefix='files')
        exec_form = ExecutableFileForm(request.POST, request.FILES)
        argtemp_form = ArgTemplateForm(request.POST)
        # Checking forms' data
        if value_formset.is_valid() and file_formset.is_valid() and exec_form.is_valid() and argtemp_form.is_valid():
            # Parsing argument template
            parser = SpecFactory(argtemp_form.cleaned_data['argtemplate'])
            # Gathering all elements' names from valueforms and fileforms and checking with parsed ones
            names = []
            for form in value_formset:
                names.append(form.cleaned_data['name'])
            for form in file_formset:
                names.append(form.cleaned_data['name'])
            missed_names = parser.check_names(names)
            if len(missed_names) == 0:
                # All names in template are covered
                unused_names = parser.unused_names(names)
                value_data = []
                file_data = []
                for form in value_formset:
                    # Not using unused data
                    if not form.cleaned_data['name'] in unused_names:
                        value_data.append(form.cleaned_data)
                for form in file_formset:
                    file_data.append(form.cleaned_data)
                parser.run_specs(exec_form.cleaned_data, value_data, file_data)

            else:
                # TODO Name not specified, adding error to form
                pass


    else:
        value_formset = ValueFormSet(prefix='values')
        file_formset = FileFormSet(prefix='files')
        exec_form = ExecutableFileForm()
        argtemp_form = ArgTemplateForm()
    context = {'arg_forms': value_formset, 'file_forms': file_formset,
               'exec_form': exec_form, 'argtemp_form': argtemp_form}
    return render(request, 'sim/index.html', context)
