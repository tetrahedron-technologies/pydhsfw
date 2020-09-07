from .context import pydhsfw
from pydhsfw.threads import AbortableThread
import time

class TestThread(AbortableThread):

    def __init__(self, name=None):
        super().__init__(group=None, name=name)

    def run(self):
  
        # target function of the thread class 
        try: 
            while True: 
                print('running ' + self.name)
                time.sleep(0.25)
        except SystemExit:
            print('exception')
        finally:
            self.cleanup()
            print('ended') 

    def cleanup(self):
        print('cleanup')

       
t1 = TestThread('Thread 1') 
t1.start() 
time.sleep(1) 
t1.abort()
t1.join()