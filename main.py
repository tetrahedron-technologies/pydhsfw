from pydhsfw.connectionmanager import ConnectionManager
from pydhsfw.messages import MessageIn, MessageProcessorWorker
from pydhsfw.connectionmanager import ConnectionManager
from pydhsfw.dcss import DcssStoCSendClientType, DcssCtoSClientIsHardware
import time

conn_mgr = ConnectionManager()
class LoopDcssMessageProcessor(MessageProcessorWorker):

    def __init__(self):
        super().__init__(name = 'loop dcss message processor')

    def process_message(self, message:MessageIn):
        print(f'LoopDHS process message received {message}')
        
        conn = conn_mgr.get_connection('dcss')
        if isinstance(message, DcssStoCSendClientType):
            conn.send(DcssCtoSClientIsHardware('loop'))

url = 'dcss://localhost:14242'
msg_processor = LoopDcssMessageProcessor()
msg_processor.start()

conn_mgr.create_connection('dcss', url, msg_processor)
conn_mgr.start_connections()

conn_mgr.get_connection('dcss').connect()

time.sleep(30)

msg_processor.abort()
msg_processor.join()
conn_mgr.stop_connections()
