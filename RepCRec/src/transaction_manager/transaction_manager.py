"""
Authors: 
Saksham Bassi
Aayush Agrawal
"""
from src.deadlock_manager import DeadlockManager
from src.enums import TransactionType, InstructionType, AcquireLockPermission, LockType
from src.io_manager import IOManager
from src.site import Site
from src.transaction_manager import Transaction
from src.utils import log
from typing import Set, Tuple

class TransactionManager:
    def __init__(self, total_sites: int):
        self.aborted_transactions = set()
        self.DeadlockManager = DeadlockManager()
        self.IOManager = IOManager()
        self.last_failed_timestamp = {}  # {site, time}
        self.sites = []  # object of sites
        self.site_to_transactions = {}  # {site, Set(Transaction #)}
        self.transactions = []  # store details about transactions
        self.transaction_start_timestamp = {}  # { transaction #, time }
        self.timestamp = 0
        self.total_sites = total_sites
        self.wait_for_lock_queue = []   # something list of transactions
                                        # {transaction_id: set (variables)}
        self.write_transactions_to_variables = {}

    def abort_transaction(self, transaction_id: int):
        """
        Aborts the provided transaction

        Args:
            transaction_id (int): id of transaction
        """
        log(f"Abort transaction T{transaction_id}")
        self.aborted_transactions.remove(transaction_id)

    def add_transaction_to_site(self, site: Site, transaction: Transaction):
        """
        Helper function to map a transaction's id to a site.

        Args:
            site (Site)
            transaction (Transaction)
        """
        existing_transactions = self.site_to_transactions.get(site.id, set())
        existing_transactions.add(transaction.id)
        self.site_to_transactions[site.id] = existing_transactions

    def add_variable_to_write_transaction(
        self, transaction: Transaction, variable: int
    ):
        """
        Helper function to map a variable to a transaction that has issued a write
        request. 

        Args:
            transaction (Transaction):
            variable (int):
        """
        existing_variables = self.write_transactions_to_variables.get(
            transaction.id, set()
        )
        existing_variables.add(variable)
        self.write_transactions_to_variables[transaction.id] = existing_variables

    def any_dependent_in_wait_queue(
        self, transaction: Transaction, locktype: LockType
    ) -> Set[int]:
        """
        Check if the given transaction is contending to get a lock on any other
        transaction that is ahead of the given transcation in the wait queue.

        Args:
            transaction (Transaction)
            locktype (LockType)

        Returns:
            Set[int]: Set of transactions with which the passed transction contends
            with/ are dependents.
        """
        dependents = set()
        for dep_transaction in self.wait_for_lock_queue:
            if dep_transaction == transaction:
                break

            if transaction.variable == dep_transaction.variable:
                if (
                    locktype == LockType.READ
                    and dep_transaction.transaction_type == TransactionType.WRITE
                ):
                    dependents.add(dep_transaction.id)
                if (
                    locktype == LockType.WRITE
                    and dep_transaction.transaction_type != TransactionType.NONE
                ):
                    dependents.add(dep_transaction.id)
        return dependents

    def commit(self, transaction_id: int) -> None:
        """ When a transaction is committed, all the variables that it changed (made a write
        to), should be saved/committed on all the sites.

        Args:
            transaction_id (int)
        """
        for variable in self.write_transactions_to_variables.get(transaction_id, set()):
            for site in self.sites:
                if site.is_active() and site.is_variable_present(variable):
                    site.commit_cache(variable)
        log(f"Transaction {transaction_id} is commited")

        # clear transaction metadata in memory
        if transaction_id in self.write_transactions_to_variables:
            self.write_transactions_to_variables.pop(transaction_id)

        for site in self.sites:
            if site.id in self.site_to_transactions:
                if transaction_id in self.site_to_transactions[site.id]:
                    self.site_to_transactions[site.id].remove(transaction_id)

    def check_wait_queue(self):
        """
        Before a new transaction is picked up, we check if there is a transaction in the
        wait queue that has the permission to acquire the desired locks on the variables
        across sites.

        Therefore, this function iterates over all the transactions in the wait queue.
        Helper methods are then called to check the condition based on which type of
        lock the transaction desires.

        Returns:
            bool: Whether a transaction in the wait queue is ready to be executed.
        """
        for index, transaction in enumerate(self.wait_for_lock_queue):
            if transaction.transaction_type == TransactionType.READONLY:
                can_wait, site = self.check_readonly_transaction_from_wait_queue(transaction)
                if not can_wait:
                    self.wait_for_lock_queue.pop(index)
                    self.log_read(
                        transaction, site, self.transaction_start_timestamp[transaction.id])
                    return True
            
            if transaction.transaction_type == TransactionType.READ:
                can_wait, _, site = self.check_read_transaction_from_wait_queue(
                    transaction)
                if not can_wait:
                    self.wait_for_lock_queue.pop(index)
                    self.log_read(
                        transaction, site, self.timestamp)
                    return True

            if transaction.transaction_type == TransactionType.WRITE:
                can_wait, _ = self.check_write_transaction_from_wait_queue(
                    transaction)
                if not can_wait:
                    self.wait_for_lock_queue.pop(index)
                    return True

    def check_readonly_transaction_from_wait_queue(self, transaction) -> bool:
        """
        We iterate over all the sites to find the one onto which the transaction can be
        applied.

        - The site should be up.
        - The variable requested by the transaction should be present on the site.
        - Variable in the site is as less stale as possible (even variables are present
          on all sites), so if it is stale in some site_i we try to find it elsewhere.
        - Site has not failed.
        - Site made a commit before failing, and it failed after the transaction started.

        Args:
            transaction (Transaction)

        Returns:
            bool: Whether the given transaction can can wait in the wait queue or not.
        """
        start_time = self.transaction_start_timestamp[transaction.id]
        variable = transaction.variable
        for site in self.sites:
            if not site.is_active() or not site.is_variable_present(variable):
                continue
            if not site.is_variable_unique(variable) and site.is_stale(variable):
                continue
            if (
                site.is_variable_unique(variable)
                or site not in self.last_failed_timestamp
            ):
                return False, site
            # site failed before the start of transaction
            last_fail_time = self.last_failed_timestamp[site]
            last_commit_time = site.get_last_committed_time(variable, start_time)
            if last_commit_time < last_fail_time and last_fail_time < start_time:
                continue
            return False, site
        return True, None

    def check_read_transaction_from_wait_queue(self, transaction) -> Tuple[bool, Set[int]]:
        """
        We iterate over all the sites to find the one onto which the transaction can be
        applied.

        - The site should be up.
        - The variable requested by the transaction should be present on the site.
        - Variable in the site is as less stale as possible (even variables are present
          on all sites), so if it is stale in some site_i we try to find it elsewhere.
        - Read lock can be acquired on the requested variable in the site.

        Args:
            transaction (Transaction)

        Returns:
            bool: Whether the given transaction can wait or not.
        """
        dependents = self.any_dependent_in_wait_queue(transaction, LockType.READ)

        variable = transaction.variable
        for site in self.sites:
            if not site.is_active() or not site.is_variable_present(variable):
                continue
            if not site.is_variable_unique(variable) and site.is_stale(variable):
                continue
            if (
                site.can_acquire_read_lock(variable, transaction.id)
                == AcquireLockPermission.NOT_ALLOWED
            ):
                dependents.update(site.get_all_transaction_locks(variable))
                continue

            dependents = set()
            log(f"Transaction T{transaction.id} acquires READ lock on variable {variable} at site {site.id} at time {self.timestamp}")
            site.acquire_lock(transaction.id, variable, LockType.READ)
            self.add_transaction_to_site(site, transaction)
            return False, dependents, site

        return True, dependents, None

    def check_write_transaction_from_wait_queue(self, transaction) -> Tuple[bool, Set[int]]:
        """
        When a transaction wants to write, it has to acquire a lock for reqested
        variable on all the sites which are active and contains the requested variable.

        - The site should be up.
        - The variable requested by the transaction should be present on the site.
        - Transaction should be able to acquire a lock for the variable on all the site.

        Args:
            transaction (Transaction)

        Returns:
            bool: Whether the given transaction can be applied onto all the site.
        """

        dependents = self.any_dependent_in_wait_queue(transaction, LockType.WRITE)

        for site in self.sites:
            variable = transaction.variable
            if not site.is_active() or not site.is_variable_present(variable):
                continue
            permit = site.can_acquire_write_lock(variable, transaction.id)
            if permit == AcquireLockPermission.NOT_ALLOWED:
                dependents.update(site.get_all_transaction_locks(variable))
                return True, dependents
            if permit == AcquireLockPermission.ALLOWED_IF_EMPTY_WAIT_QUEUE:
                if len(dependents) > 0:
                    return True, dependents
        
        if len(dependents) > 0:
            return True, dependents

        can_wait = True

        for site in self.sites:
            variable = transaction.variable
            if not site.is_active() or not site.is_variable_present(variable):
                continue
            
            can_wait = False
            log(f"Transaction T{transaction.id} acquires WRITE lock on variable {variable} at site {site.id} at time {self.timestamp}")
            site.acquire_lock(transaction.id, variable, LockType.WRITE)
            site.set_cache(variable, transaction.value, self.timestamp)
            self.add_transaction_to_site(site, transaction)
            self.add_variable_to_write_transaction(transaction, variable)
        return can_wait, dependents

    def detect_deadlock(self):
        """
        Detects deadlocks in graph, uses DeadlockManager module. In case of a dead lock,
        it calls on the latest transaction in the cycle of transactions and aborts it. 

        Returns:
            bool: Whether a deadlock is detected.
        """
        deadlocks = self.DeadlockManager.detect_deadlock_in_graph()
        if len(deadlocks):
            latest_transaction_id = self.fetch_latest_transaction(
                transactions=deadlocks
            )
            self.DeadlockManager.delete_edges_of_source(latest_transaction_id)
            log(
                f"Deadlock found: {len(deadlocks)} transactions in cycle")
            self.aborted_transactions.add(latest_transaction_id)
            for site_id in range(self.total_sites):
                self.sites[site_id].release_all_transaction_locks(
                    latest_transaction_id)
            log(f"Latest transaction T{latest_transaction_id} aborted to avoid deadlock.")
            self.abort_transaction(latest_transaction_id)
            self.pop_waitq_transaction(latest_transaction_id)
            return True
        return False

    def fetch_latest_transaction(self, transactions: set) -> int:
        """
        Fetches latest/youngest transaction

        Args:
            transactions (set): Set of transactions that form a cycle/deadlock in the
            dependency graph formed by the transactions.

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
        """
        Handles transaction when it fails.

        - All locks on the site of the transaction are released.
        - Transaction is ready to abort. 

        Args:
            transaction (Transaction)
        """
        self.sites[transaction.site_id - 1].release_all_locks()
        self.sites[transaction.site_id - 1].shutdown()
        if transaction.site_id in self.site_to_transactions:
            site_transactions = self.site_to_transactions[transaction.site_id]
            for site_transaction in site_transactions:
                self.aborted_transactions.add(site_transaction)
            del self.site_to_transactions[transaction.site_id]
        self.last_failed_timestamp[self.sites[transaction.site_id - 1]
                                   ] = self.timestamp
        log(f"Site {transaction.site_id} failed at time {self.timestamp}")

    def handle_transaction_recover(self, transaction: Transaction):
        """ Handles transaction when it is RECOVER

        Args:
            transaction (Transaction)
        """
        self.sites[transaction.site_id - 1].activate()
        log(f"Site {transaction.site_id} recovered at time {self.timestamp}")

    def handle_transaction_dump(self, transaction: Transaction):
        """ Handles transaction when it is DUMP

        Args:
            transaction (Transaction)
        """
        for site in self.sites:
            log(f"DUMP data for site {site.id}")
            site.dump(self.timestamp)

    def handle_transaction_begin(self, transaction: Transaction):
        """ Handles transaction when it is BEGIN

        Args:
            transaction (Transaction)
        """
        log(f"Transaction T{transaction.id} BEGIN at time {self.timestamp}")
        self.transaction_start_timestamp[transaction.id] = self.timestamp

    def handle_transaction_begin_readonly(self, transaction: Transaction):
        """ Handles transaction when it is BEGINRO

        Args:
            transaction (Transaction)
        """
        log(f"Transaction T{transaction.id} BEGINRO at time {self.timestamp}")
        self.transaction_start_timestamp[transaction.id] = self.timestamp

    def handle_transaction_end(self, transaction: Transaction):
        """ Handles transaction when it is END

        - Attempt to commit the transaction, if failed, abort it.
        - If transaction is present in the wait queue, remove it.
        - Release all the locks that the transaction held.
        - Remove the edges that had source has given transactions.

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
        self.DeadlockManager.delete_edges_of_source(
            transaction_id=transaction.id)

    def handle_transaction_none(self, transaction: Transaction):
        """
        Transaction either wants to READONLY, READ or WRITE.

        Helpers are invoked depending on the transaction type of the given transction.
        It is checked if the given transaction can wait, or needs to be executed right
        away. If it can wait, it is added to the wait queue.

        In case of a READ and WRITE transaction type, if a transaction can wait, edges
        are added to a dependency graph such that the given transaction is the source
        vertex and the transactions on which it depends assume the destination vertices.
        
        Args:
            transaction (Transaction)
        """
        
        if transaction.transaction_type == TransactionType.READONLY:
            can_wait, site = self.check_readonly_transaction_from_wait_queue(
                transaction)
            if can_wait:
                self.wait_for_lock_queue.append(transaction)
                log(
                    """Transaction T{0} wants to READONLY variable {1} at time {2}: pushed to wait queue""".format(
                        transaction.id, transaction.variable, self.timestamp)
                )
            else:
                self.log_read(transaction, site,
                              self.transaction_start_timestamp[transaction.id])

        elif transaction.transaction_type == TransactionType.READ:
            can_wait, dependents, site = self.check_read_transaction_from_wait_queue(
                transaction)
            if can_wait:
                self.DeadlockManager.insert_transactions_to_source(
                    transaction.id, dependents)
                self.wait_for_lock_queue.append(transaction)
                log(
                    """Transaction T{0} wants to READ variable x{1} at time {2}: pushed to wait queue""".format(
                        transaction.id, transaction.variable, self.timestamp)
                )
            else:
                self.log_read(transaction, site, self.timestamp)

        elif transaction.transaction_type == TransactionType.WRITE:
            can_wait, dependents = self.check_write_transaction_from_wait_queue(transaction)
            if can_wait:
                self.DeadlockManager.insert_transactions_to_source(transaction.id, dependents)
                self.wait_for_lock_queue.append(transaction)
                log(
                    """Transaction T{0} wants to WRITE value {1} for variable {2} at time {3}: pushed to wait queue""".format(
                        transaction.id, transaction.value, transaction.variable, self.timestamp)
                )

    def is_commit_allowed(self, transaction_id: int):
        """

        If a transaction is not marked for abortion, it can commit.

        Args:
            transaction_id (int)

        Returns:
            bool: Whether a transaction can commit or not.
        """
        return transaction_id not in self.aborted_transactions

    def log_read(self, transaction: Transaction, site: Site, timestamp: int):
        variable = transaction.variable
        begin_time = self.transaction_start_timestamp[transaction.id]
        value = site.get_value(variable, timestamp)
        log(
            f"Variable {variable}: {value}")

    def pop_waitq_transaction(self, pop_transaction_id: int):
        """
        Pops transaction from the wait queue

        Args:
            transaction_id (int): transaction id
        """
        for index, transaction in enumerate(self.wait_for_lock_queue):
            if transaction.id == pop_transaction_id:
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
        """
        Starts the execution on all transactions
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
