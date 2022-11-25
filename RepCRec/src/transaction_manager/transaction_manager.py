from src.deadlock_manager import DeadlockManager
from src.io_manager import IOManager
from src.site import Site
from src.enums import TransactionType, InstructionType


class TransactionManager:
    def __init__(self, total_sites: int):
        self.DeadlockManager = DeadlockManager()
        self.IOManager = IOManager()
        self.last_failed_timestamp = {}     # {site, time}
        self.sites = []
        self.transactions = []              # store details about transactions
        self.transaction_start_timestamp = {}   # { transaction #, time }
        self.timestamp = 0
        self.total_sites = int(total_sites)
        self.waitForLockQueue = []        # something list of transactions
        self.aborted_transactions = set()
        self.variables_used_in_write_transactions = dict()  # {transaction_id: int, set of variables}


    # TODO: canCommit or ref
    def is_commit_allowed(self, transaction_id: int):
        return transaction_id not in self.aborted_transactions


    def commit(self, transaction_id: int) -> None:
        """
        When a transaction is committed, all the variables that it changed (made a write
        to), should be saved/committed on all the sites.

        Args:
            transaction_id (int)
        """
        for variable in self.variables_used_in_write_transactions.get(transaction_id, {}):
            for site in self.sites:
                if site.is_active() and site.is_variable_present(variable):
                    site.commit_cache(variable)


    # TODO: checkWaitQueue of ref
    def fetch_transaction_from_wait_queue(self):

        for index, transaction in enumerate(self.waitForLockQueue):

            if transaction.transaction_type == TransactionType.READONLY:
                if self.can_fetch_readonly_transaction_from_wait_queue(
                    index, transaction):
                    return transaction

            elif transaction.transaction_type == TransactionType.READ:
                if self.can_fetch_read_transaction_from_wait_queue(
                    index, transaction):
                    return transaction

            elif transaction.transaction_type == TransactionType.WRITE:
                if self.can_fetch_write_transaction_from_wait_queue(
                    index, transaction):
                    return transaction


    def can_fetch_readonly_transaction_from_wait_queue(self, index, transaction) -> bool:
        start_time = self.transaction_start_timestamp[transaction]
        for site in self.sites:
            variable = transaction.variable
            if not site.is_active() or not site.is_variable_present(variable):
                continue

            if site.is_variable_unique(variable) or site not in self.last_failed_timestamp:
                self.waitForLockQueue.pop(index)
                return True

            if not site.is_variable_unique(variable) and site.is_stale(variable):
                continue
            
            # TODO: Refer this condition again: seems weird in ref
            last_fail_time = self.last_failed_timestamp[site]
            last_commit_time = site.get_last_committed_time(variable, start_time)
            if last_commit_time < last_fail_time and last_fail_time < start_time:
                self.waitForLockQueue.pop(index)
                return True
        return False

    def can_fetch_read_transaction_from_wait_queue(self, index, transaction) -> bool:
        pass

    def can_fetch_write_transaction_from_wait_queue(self, index, transaction) -> bool:
        for site in self.sites:
            variable = transaction.variable
            if not site.is_active() or not site.is_variable_present(variable):
                continue
        pass


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
        for transaction in self.transactions:
            self.timestamp += 1
            while True:
                if not self.detect_deadlock():
                    break
            while True:
                if not self.fetch_transaction_from_wait_queue():
                    break

            transaction_handler = {
                InstructionType.FAIL: self.handle_transaction_fail,
                InstructionType.RECOVER: self.handle_transaction_recover,
                InstructionType.DUMP: self.handle_transaction_dump,
                InstructionType.BEGINRO: self.handle_transaction_begin_readonly,
                InstructionType.BEGIN: self.handle_transaction_begin,
                InstructionType.END: self.handle_transaction_end,
                InstructionType.NO: self.handle_transaction_none
            }
            transaction_handler[transaction.instruction_type]()


    def handle_transaction_fail(self):
        pass

    def handle_transaction_recover(self):
        pass

    def handle_transaction_dump(self):
        pass

    def handle_transaction_begin(self):
        pass

    def handle_transaction_begin_readonly(self):
        pass

    def handle_transaction_end(self):
        pass

    def handle_transaction_none(self):
        pass
