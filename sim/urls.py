from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^success/(?P<job_name>\w+)', views.success, name='success')
]
