"""
WSGI config for sporeweb project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/howto/deployment/wsgi/
"""

import os
import threading
import time

from django.core.wsgi import get_wsgi_application

from logs.daemon import check_history

def daemon():
    time.sleep(300)
    check_history()


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sporeweb.settings")

application = get_wsgi_application()

_thread = threading.Thread(target=daemon)
_thread.setDaemon(True)
_thread.start()