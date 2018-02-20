from django.conf.urls import url
from . import views

app_name = 'logs'
urlpatterns = [
    url(r'^$', views.index, name = 'index'),
    url(r'^detail/(?P<job_name>\w+)', views.detail, name='detail'),
]