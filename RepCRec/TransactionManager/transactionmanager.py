from Site import site


class TransactionManager:
    def __init__(self, total_sites: int):
        self.total_sites = int(total_sites)
        self.sites = []

    def prepare_input(self, filename: str):
        """ Creates sites and pushes it to self.sites

        Args:
            filename (str): file containing input

        Returns:
            None
        """
        for id in range(self.total_sites):
            self.sites.append(site.Site(id=id+1))
            self.sites[id].initialize()

        # TODO: implement sites.initialize