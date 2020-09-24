import logging
import threading
import ctypes

_logger = logging.getLogger(__name__)

class AbortableThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), config:dict={}, kwargs=None, *, daemon=None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self._thread_blocking_timeout = config.get('thread_blocking_timeout', 5.0)

    def _get_blocking_timeout(self):
        return self._thread_blocking_timeout

    def _get_id(self): 

        # returns id of the respective thread 
        for id, thread in threading._active.items():
            if thread is self: 
                return id

    def abort(self): 
        thread_id = self._get_id()
        thread = threading._active.get(thread_id)
        _logger.info(f'Sending SystemExit exceptions to: {thread.name}')
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread_id), 
              ctypes.py_object(SystemExit)) 
        if res > 1: 
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread_id), None) 
            print('Exception raise failure') 