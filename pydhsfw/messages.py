import threading 
import traceback
from collections import deque
from pydhsfw.threads import AbortableThread

class MessageOut():
    def __init__(self):
        pass

    def write(self)->bytes:
        pass

class MessageIn():
    @staticmethod
    def parse(buffer:bytes):
        pass

class MessageReader():
    def read(self, bufsize):
        pass

class MessageFactory():

    def __init__(self):
        pass
    
    def readRawMessage(self, reader: MessageReader)->bytes:
        """Read the raw bytes of an entire message based on known message length or delimeter and return it."""
        pass

    def createMessage(self, raw_message:bytes)->MessageIn:
        """Convert a raw message to a MessageIn subclass"""
        pass

class MessageProcessor():

    def __init__(self):
        pass
    
    def start(self):
        pass

    def process_message(self, message:MessageIn):
        pass

    def get_connection(self):
        pass 

    def stop(self):
        pass

    def _queque_message(self, message:MessageIn):
        pass

    def _set_connection(self, connection):
        pass

class MessageProcessorWorker(AbortableThread, MessageProcessor):

    _deque_message = deque()
    _deque_event = threading.Event()
    _connection = None

    def __init__(self, name):
        AbortableThread.__init__(self, name=name)

    def start(self):
        AbortableThread.start(self)

    def process_message(self, message:MessageIn):
        pass

    def get_connection(self):
        return self._connection

    def stop(self):
        super().abort()
        super().join()

    def run(self):
        try:
            while True:
                try:
                    #Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                    msg = self._get_message(5)
                    if msg:
                        print(f'Processing message: {msg}')
                        self.process_message(msg)
                except TimeoutError:
                    #This is normal when there are no more mesages in the queue and wait time has ben statisfied. Just ignore it.
                    pass
                except Exception:
                    traceback.print_exc()
                    raise

        except SystemExit:
            self._notify_status('Connection message processor shutdown')
            
        finally:
            pass


    def _queque_message(self, message:MessageIn):
        #Append message and unblock
        self._deque_message.append(message)
        self._deque_event.set()

    def _get_message(self, timeout=None):
        
        msg = None

        #Block until items are available
        if not self._deque_event.wait(timeout):
            raise TimeoutError
        
        elif self._deque_message: 
            msg = self._deque_message.popleft()

        #If there are no more items, start blocking again
        if not self._deque_message:
            self._deque_event.clear()
        return msg

    def _set_connection(self, connection):
        self._connection = connection

    def _notify_status(self, msg:str):
        print(msg)
