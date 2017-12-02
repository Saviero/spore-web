import threading
import time

def daemon():
    time.sleep(60)



_thread = threading.Thread(target=daemon)
_thread.setDaemon(True)
_thread.start()
