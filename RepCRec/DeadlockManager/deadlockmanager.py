import os

from read_config import read_config

path = os.path.dirname(os.path.realpath(__file__))
configdir = '/'.join([path, '..', 'config.ini'])
config = read_config(configdir)

class DeadlockManager:
    def __init__(self) -> None:
        self.nodes = int(config['CONSTANTS']['num_transactions'])
        self.adjacency_list = [None]*self.nodes
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
            recursion_stack[node] = visited[node] = True    # setting visited true for current node
            for i in range(len(self.adjacency_list[node])):
                if (not visited[i] \
                    and self.detect_cycle_in_graph(visited, recursion_stack, i)) \
                    or (recursion_stack[i]):
                    self.deadlocked_transactions.add(i)
                    return True
        
        recursion_stack[node] = False                       # resetting
        return False
    
    def detect_deadlock_in_graph(self):
        """ detects deadlock in graph

        Returns:
            self.deadlocked_transactions (set)
        """
        self.deadlocked_transactions = set()
        recursion_sack = visited = [False] * self.nodes
        for i in range(self.nodes):
            if self.detect_cycle_in_graph(
                visited=visited,
                recursion_stack=recursion_sack,
                node=i):
                return self.deadlocked_transactions
        return self.deadlocked_transactions
