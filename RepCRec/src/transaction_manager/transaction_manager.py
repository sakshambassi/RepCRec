from src.deadlock_manager import DeadlockManager
from src.enums import TransactionType, InstructionType
from src.io_manager import IOManager
from src.site import Site
from src.transaction_manager import Transaction


class TransactionManager:
    def __init__(self, total_sites: int):
        self.aborted_transactions = set()
        self.DeadlockManager = DeadlockManager()
        self.IOManager = IOManager()
        self.last_failed_timestamp = {}  # {site, time}
        self.sites = []  # object of sites
        self.site_to_transactions = {}  # {site, List(Transaction)}
        self.transactions = []  # store details about transactions
        self.transaction_start_timestamp = {}  # { transaction #, time }
        self.timestamp = 0
        self.total_sites = int(total_sites)
        self.wait_for_lock_queue = []  # something list of transactions
        self.variables_used_in_write_transactions = (
            dict()
        )  # {transaction_id: int, set of variables}

    # TODO: implement the below function
    def abort_transaction(self, transaction_id: int):
        """ aborts the provided transaction

        Args:
            transaction_id (int): id of transaction
        """
        pass

    def commit(self, transaction_id: int) -> None:
        """ When a transaction is committed, all the variables that it changed (made a write
        to), should be saved/committed on all the sites.

        Args:
            transaction_id (int)
        """
        for variable in self.variables_used_in_write_transactions.get(
            transaction_id, {}
        ):
            for site in self.sites:
                if site.is_active() and site.is_variable_present(variable):
                    site.commit_cache(variable)

    def check_wait_queue(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        for index, transaction in enumerate(self.wait_for_lock_queue):

            if transaction.transaction_type == TransactionType.READONLY:
                if self.check_readonly_transaction_from_wait_queue(transaction):
                    self.wait_for_lock_queue.pop(index)
                    return transaction

            elif transaction.transaction_type == TransactionType.READ:
                if self.check_read_transaction_from_wait_queue(transaction):
                    self.wait_for_lock_queue.pop(index)
                    return transaction

            elif transaction.transaction_type == TransactionType.WRITE:
                if self.check_write_transaction_from_wait_queue(transaction):
                    self.wait_for_lock_queue.pop(index)
                    return transaction

    def check_readonly_transaction_from_wait_queue(self, transaction) -> bool:
        """_summary_

        Args:
            transaction (_type_): _description_

        Returns:
            bool: _description_
        """
        start_time = self.transaction_start_timestamp[transaction]

        for site in self.sites:
            variable = transaction.variable

            if not site.is_active() or not site.is_variable_present(variable):
                continue

            if (
                site.is_variable_unique(variable)
                or site not in self.last_failed_timestamp
            ):
                return True

            if not site.is_variable_unique(variable) and site.is_stale(variable):
                continue

            # site failed before the start of transaction
            last_fail_time = self.last_failed_timestamp[site]
            last_commit_time = site.get_last_committed_time(
                variable, start_time)
            if last_commit_time < last_fail_time and last_fail_time < start_time:
                continue
            return True
        return False

    def check_read_transaction_from_wait_queue(self, transaction) -> bool:
        """_summary_

        Args:
            transaction (_type_): _description_

        Returns:
            bool: _description_
        """
        pass

    def check_write_transaction_from_wait_queue(self, transaction) -> bool:
        """ checks write Transaction in wait queue

        Args:
            transaction (_type_): _description_

        Returns:
            bool: _description_
        """
        for site in self.sites:
            variable = transaction.variable
            if not site.is_active() or not site.is_variable_present(variable):
                continue
        pass

    def detect_deadlock(self):
        """ detects deadlocks in graph, uses DeadlockManager module

        Returns:
            true / false: true if detects a deadlock else false
        """
        deadlocks = self.DeadlockManager.detect_deadlock_in_graph()
        if len(deadlocks):
            latest_transaction_id = self.fetch_latest_transaction(
                transactions=deadlocks
            )
            self.DeadlockManager.delete_edges_of_source(latest_transaction_id)
            print(
                f"There exists deadlock with number of transactions={len(deadlocks)}"
            )
            self.aborted_transactions.add(latest_transaction_id)
            for site_id in range(self.total_sites):
                self.sites[site_id].release_all_locks(latest_transaction_id)
            print(
                f'Latest transaction {latest_transaction_id} is aborted.'
            )
            self.abort_transaction(latest_transaction_id)
            self.pop_waitq_transaction()
            return True
        return False

    def fetch_latest_transaction(self, transactions: set) -> int:
        """ fetches latest/youngest transaction

        Args:
            transactions (set): set of transaction ids to search from

        Returns:
            latest_transaction_id (int): youngest transaction id

        """
        latest_transaction_id = None
        max_time = float("-inf")
        for transaction_id, timestamp in self.transaction_start_timestamp.items():
            if transaction_id in transactions and timestamp > max_time:
                latest_transaction_id = transaction_id
                max_time = timestamp
        return latest_transaction_id

    def handle_transaction_fail(self, transaction: Transaction):
        """ handles transaction when it is FAIL

        Args:
            transaction (Transaction)
        """
        print(f'Handling FAIL transaction, site {transaction.site_id} down.')
        self.sites[transaction.site_id - 1].release_all_locks()
        self.sites[transaction.site_id - 1].shutdown()
        if transaction.site_id in self.site_to_transactions:
            site_transactions = self.site_to_transactions[transaction.site_id]
            for site_transaction in site_transactions:
                self.aborted_transactions.add(site_transaction)
            del self.site_to_transactions[transaction.site_id]
        self.last_failed_timestamp[
            self.sites[
                transaction.site_id-1
            ]
        ] = self.timestamp

    def handle_transaction_recover(self, transaction: Transaction):
        """ handles transaction when it is RECOVER

        Args:
            transaction (Transaction)
        """
        print(
            f'Handling RECOVER transaction, site {transaction.site_id} recovered succesfully.')
        self.sites[transaction.site_id - 1].activate()

    def handle_transaction_dump(self, transaction: Transaction):
        """ handles transaction when it is DUMP

        Args:
            transaction (Transaction)
        """
        for site in self.sites:
            site.dump(self.timestamp)

    def handle_transaction_begin(self, transaction: Transaction):
        """ handles transaction when it is BEGIN

        Args:
            transaction (Transaction)
        """
        print(
            f'Handling BEGIN transaction, transaction {transaction.id} begun succesfully.')
        self.transaction_start_timestamp[transaction.id] = self.timestamp

    # TODO: May differ from `handle_transaction_begin` when print statements are added
    def handle_transaction_begin_readonly(self, transaction: Transaction):
        """ handles transaction when it is BEGINRO

        Args:
            transaction (Transaction)
        """
        print(
            f'Handling BEGINRO transaction, transaction {transaction.id} of type read-only begun.')
        self.transaction_start_timestamp[transaction.id] = self.timestamp

    def handle_transaction_end(self, transaction: Transaction):
        """ handles transaction when it is END

        Args:
            transaction (Transaction)
        """
        if self.is_commit_allowed(transaction_id=transaction.id):
            self.commit(transaction_id=transaction.id)
        else:
            self.abort_transaction(transaction_id=transaction.id)
        del self.transaction_start_timestamp[transaction.id]
        self.pop_waitq_transaction(transaction.id)
        for site_id in range(self.total_sites):
            self.sites[site_id].release_all_transaction_locks(transaction.id)
        self.DeadlockManager.delete_edges_of_source(transaction_id=transaction.id)


    def handle_transaction_none(self, transaction: Transaction):
        if transaction.transaction_type == TransactionType.READONLY:
            if not self.check_read_transaction_from_wait_queue(transaction):
                self.wait_for_lock_queue.append(transaction)

        if transaction.transaction_type == TransactionType.READ:
            pass

        if transaction.transaction_type == TransactionType.WRITE:
            pass

    # TODO: canCommit or ref
    def is_commit_allowed(self, transaction_id: int):
        return transaction_id not in self.aborted_transactions

    def pop_waitq_transaction(self, pop_transaction_id: int):
        """ pops transaction from the wait queue

        Args:
            transaction_id (int): transaction id
        """
        for index, transaction in enumerate(self.wait_for_lock_queue):
            if transaction.id_ == pop_transaction_id:
                self.wait_for_lock_queue.pop(index)

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
                if not self.check_wait_queue():
                    break

            transaction_handler = {
                InstructionType.FAIL: self.handle_transaction_fail,
                InstructionType.RECOVER: self.handle_transaction_recover,
                InstructionType.DUMP: self.handle_transaction_dump,
                InstructionType.BEGINRO: self.handle_transaction_begin_readonly,
                InstructionType.BEGIN: self.handle_transaction_begin,
                InstructionType.END: self.handle_transaction_end,
                InstructionType.NO: self.handle_transaction_none,
            }
            transaction_handler[transaction.instruction_type](transaction)
