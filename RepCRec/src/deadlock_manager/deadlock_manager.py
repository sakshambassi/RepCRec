from src.utils import config


class DeadlockManager:
    def __init__(self) -> None:
        self.nodes = int(config['CONSTANTS']['num_transactions'])
        self.adjacency_list = [list() for _ in range(self.nodes)]
        self.deadlocked_transactions = set()

    def detect_cycle_in_graph(self, visited: list, recursion_stack: list, node: int):
        """ detects cycle in graph
        
        Args:
            vistied (list): list of bool visited
            recursion_stack (list): recursion stack 
            node (int): current node being processed

        Returns:
            cycle or not (bool)
        """
        if not visited[node]:
            recursion_stack[node] = visited[
                node
            ] = True  # setting visited true for current node
            for i in range(len(self.adjacency_list[node])):
                if (
                    not visited[i]
                    and self.detect_cycle_in_graph(visited, recursion_stack, i)
                ) or (recursion_stack[i]):
                    self.deadlocked_transactions.add(i)
                    return True

        recursion_stack[node] = False  # resetting
        return False

    def delete_edges_of_source(self, transaction_id: int):
        """ deletes all edges of the source transaction

        Args:
            transaction_id (int): source transaction id
        """
        self.adjacency_list[transaction_id] = list()    # remove outgoing transactions from source
        for node in range(self.nodes):
            # remove all incoming edges to source transaction
            if transaction_id in self.adjacency_list[node]:
                self.adjacency_list[node].remove(transaction_id)

    def detect_deadlock_in_graph(self):
        """ detects deadlock in graph

        Returns:
            self.deadlocked_transactions (set)
        """
        self.deadlocked_transactions = set()
        recursion_sack = visited = [False] * self.nodes
        for i in range(self.nodes):
            if self.detect_cycle_in_graph(
                visited=visited, recursion_stack=recursion_sack, node=i
            ):
                return self.deadlocked_transactions
        return self.deadlocked_transactions

    def insert_transactions_to_source(self, source_transaction_id: int, transactions: set):
        """ insert transactions to given source transaction_id

        Args:
            source_transaction_id (int): source transaction_id
            transactions (set): set of transaction_ids to be added
        """
        transactions.remove(source_transaction_id)     # remove source transaction from set
        for transaction_id in transactions:
            self.adjacency_list[source_transaction_id].append(transaction_id)

