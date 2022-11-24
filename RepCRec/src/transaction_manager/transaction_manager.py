from src.deadlock_manager import DeadlockManager
from src.io_manager import IOManager
from src.site import Site


class TransactionManager:
    def __init__(self, total_sites: int):
        self.DeadlockManager = DeadlockManager()
        self.IOManager = IOManager()
        self.last_failed_timestamp = {}
        self.sites = []
        self.transactions = []              # store details about transactions
        self.transaction_start_timestamp = {}
        self.timestamp = 0
        self.total_sites = int(total_sites)

    def detect_deadlock(self):
        """

        Returns:

        """
        # TODO: call self.DeadlockManager.detect_deadlock_in_graph()
        deadlocks = self.DeadlockManager.detect_deadlock_in_graph()
        if len(deadlocks):
            latest_transaction_id = self.fetch_latest_transaction(transactions=deadlocks)
            self.DeadlockManager.delete_edges_of_source(latest_transaction_id)
            # TODO: pick up from here
            print(f'There exists deadlcock with number of transactions={len(deadlocks)}')

        return False

    def dump_all_sites(self):
        """

        Returns:

        """
        pass

    def fetch_latest_transaction(self, transactions: set) -> int:
        """ fetches latest/youngest transaction

        Args:
            transactions (set): set of transaction ids to search from

        Returns:
            latest_transaction_id (int): youngest transaction id

        """
        latest_transaction_id = None
        max_time = float('-inf')
        for transaction_id, timestamp in self.transaction_start_timestamp.items():
            if transaction_id in transactions and timestamp > max_time:
                latest_transaction_id = transaction_id; max_time = timestamp
        return latest_transaction_id



    def prepare_input(self, filename: str):
        """ creates sites and pushes it to self.sites

        Args:
            filename (str): file containing input

        Returns:
            None
        """
        for id_ in range(self.total_sites):
            self.sites.append(Site(id_=id_ + 1))
            self.sites[id_].initialize()

        self.transactions = self.IOManager.input_file(filename)

        # TODO: execute()
        self.start_execution()

    def start_execution(self):
        """ starts the execution on all transactions

        Returns:

        """
        for i in range(len(self.transactions)):
            self.timestamp += 1
            while True:
                if not self.detect_deadlock():
                    break
