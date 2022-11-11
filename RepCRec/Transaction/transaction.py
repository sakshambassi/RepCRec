class Transaction():
    def __init__(self,
                 id_: int,
                 instruction: int,
                 site_id: int,
                 variable: int,
                 value: float):
        """

        Args:
            id_ ():
            instruction ():
            site_id ():
            variable ():
            value ():
        """
        self.id = id_
        self.instruction = instruction
        self.site_id = site_id
        self.variable = variable
        self.value = value