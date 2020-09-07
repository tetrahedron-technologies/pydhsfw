from pydhsfw.connectionfactory import ConnectionFactory
from pydhsfw.messages import MessageIn, MessageProcessorWorker
from pydhsfw.dcss import DcssMessageFactory, DcssStoCSendClientType, DcssCtoSClientIsHardware
import time

class LoopDcssMessageProcessor(MessageProcessorWorker):

    def __init__(self):
        super().__init__(name = 'loop dcss message processor')

    def process_message(self, message:MessageIn):
        conn = self.get_connection()
        print(f'LoopDHS process message received {message}')
        
        if isinstance(message, DcssStoCSendClientType):
            conn.send(DcssCtoSClientIsHardware('loop'))

url = 'dcss://localhost:14242'
connection = ConnectionFactory.getConnection(url, LoopDcssMessageProcessor())
connection.start()
connection.connect(url)
time.sleep(45)
connection.exit()
