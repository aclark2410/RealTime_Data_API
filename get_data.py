# Importing relevant IG service modules
from trading_ig import IGService
from trading_ig.config import config
import logging
import json
from initialisation import Initialisation

#fFramework for recording log messages
logger = logging.getLogger(__name__)

# Useful for debugging and tracing the execution flow of a program.
logger.setLevel(logging.DEBUG)

class Get_Data:
    
    def __init__(self):
        self.initialise = Initialisation()
        self.initialise_connection = Initialisation().initialise_connection()

    def initialise_connecton(self):
        """
        Sets up the connection to the trading service
        by creating an instance of the IGService class and establishing
        a session.
        """
        self.ig_service = self.initialise.initialise_connection()
        self.ig_service.create_session()

    def get_node_by_node_data(self, node):
        """
        Fetches data about sub-categories of financial instruments 
        within a specified node on. Sub-nodes provide 
        finer classifications of instruments.
        """
        while True:
            try:
                map_dataframe = self.ig_service.fetch_sub_nodes_by_node(node=node)
                return map_dataframe
            except Exception as e:
                print(e)
                self.initialise_connection

    def get_epic_details(self, epic):
        """ 
        Retrieves information about a specific financial instrument 
        identified by its EPIC (Epic Identifier Code).
        """
        try:
            map_data = self.ig_service.fetch_market_by_epic(epic)
            data_string = json.dumps(map_data)
            data = json.loads(data_string)
            return data
        except Exception as e:
            print(e)
            self.initialise_connection

