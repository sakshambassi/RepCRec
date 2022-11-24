from src.deadlock_manager import DeadlockManager
from src.io_manager import IOManager
from src.site import Site


class TransactionManager:
    def __init__(self, total_sites: int):
        self.DeadlockManager = DeadlockManager()
        self.IOManager = IOManager()
        self.sites = []
        self.timestamp = 0
        self.total_sites = int(total_sites)
        self.transactions = []  # store details about transactions

    def detect_deadlock(self):
        """

        Returns:

        """
        # TODO: call self.DeadlockManager.detect_deadlock_in_graph()
        return False

    def dump_all_sites(self):
        """

        Returns:

        """
        pass

    def fetch_latest_transaction(self):
        """

        Returns:

        """
        pass

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
