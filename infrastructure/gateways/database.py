from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

class DatabaseGateway():
    def __init__(self):
        load_dotenv()
        self.user = os.getenv('postgresuser')
        self.password = os.getenv('postgrespassword')
        self.host = os.getenv('postgreshost')
        self.port = os.getenv('postgresport')
        self.db = os.getenv('postgresdb')
        self.engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.db))