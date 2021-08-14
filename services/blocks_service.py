import sys
import os
from distutils.util import strtobool

from dotenv import load_dotenv
from blockchainetl.streaming.streaming_utils import configure_signals, configure_logging
from ethereumetl.enumeration.entity_type import EntityType

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

from sqlalchemy import Table
from sqlalchemy.sql.schema import MetaData

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from infrastructure.gateways.database import DatabaseGateway
from sqlalchemy import insert

def parse_entity_types():
    entity_types = ["block","transaction", "token_transfer"]
    return entity_types
    pass

def validate_entity_types(entity_types, output):
    from ethereumetl.streaming.item_exporter_creator import determine_item_exporter_type, ItemExporterType
    item_exporter_type = determine_item_exporter_type(output)
    

def build_postgres_connectionstring(self):
    return "postgresql+pg8000://{}:{}@{}:{}/{}".format(
        os.getenv('postgresuser'), os.getenv('postgrespassword'), os.getenv('postgreshost'), os.getenv('postgresport'), os.getenv('postgresdb'))

class ETLService():
    def __init__(self):
        load_dotenv()
        self.last_synced_block_file = 'last_synced_block.txt'
        self.lag = 0
        self.provider_uri = os.getenv('rpcUri')
        self.output = build_postgres_connectionstring(self)
        self.start_block = None
        self.entity_types = parse_entity_types()
        self.period_seconds = 10
        self.batch_size = 10
        self.block_batch_size = 1
        self.max_workers = 5
        self.log_file = None
        self.pid_file = None


        def start(self):
            #Stream Handler
            stream(self.last_synced_block_file, self.lag, self.provider_uri, self.output, self.start_block, self.entity_types,
                self.period_seconds, self.batch_size, self.block_batch_size, self.max_workers, self.log_file, self.pid_file)
            

def stream(last_synced_block_file, lag, provider_uri, output, start_block, entity_types,
           period_seconds=10, batch_size=2, block_batch_size=10, max_workers=5, log_file=None, pid_file=None):

    configure_logging(log_file)
    configure_signals()
    validate_entity_types(entity_types, output)

    from ethereumetl.streaming.item_exporter_creator import create_item_exporter
    from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
    from blockchainetl.streaming.streamer import Streamer

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_exporter=create_item_exporter(output),
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file
    )
    streamer.stream()
