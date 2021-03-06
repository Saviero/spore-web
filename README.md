# SPORe Web App
SPORe Web App is designed as a tool for distributed parallelization of computationally intensive tasks and processing output data in a web application form.
This includes parallelization using HTCondor, parsing output data and storing it in a database, constructing plots using output data, and exporting data.
## Getting started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.
### Prerequisites
- Python 2.7
- Django 1.11.5
- HTCondor 8.6.6
- Selenium 2.21.2_0 of greater, preferably the last version, for functional testing
- Firefox browser and Gecko driver for functional testing

You can install Django and Selenium via ```pip```:
```
pip install django==1.11.5
pip install --upgrade selenium
```
You need HTCondor up and running. Check the [official website](http://research.cs.wisc.edu/htcondor/) for downloads and user manual.
### Running
After fulfilling prerequisites and downloading a source, you should be able to launch the application using Django commands:
```
python manage.py runserver
```
You can check ```127.0.0.1:8000``` and see if the website is running.