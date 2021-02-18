import socket
import os

hostname = socket.gethostname()
stage = os.environ.get('STAGE', 'develop')
