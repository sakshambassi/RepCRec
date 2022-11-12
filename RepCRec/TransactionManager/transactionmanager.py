from IOManager import iomanager
from Site import site


class TransactionManager:
    def __init__(self, total_sites: int):
        self.total_sites = int(total_sites)
        self.sites = []
        self.transactions = []
        self.IOManager = iomanager.IOManager()

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
        """ Creates sites and pushes it to self.sites

        Args:
            filename (str): file containing input

        Returns:
            None
        """
        for id_ in range(self.total_sites):
            self.sites.append(site.Site(id_=id_ + 1))
            self.sites[id_].initialize()
        print(self.sites[1].id)
        print(self.sites[1].data)
        self.transactions = self.IOManager.input_file(filename)

    def start_execution(self):
        """

        Returns:

        """
        pass
