from src.enums import TransactionType, InstructionType

class Transaction:
    def __init__(
        self,
        id_: int,
        transaction_type: TransactionType,
        instruction_type: InstructionType,
        site_id: int,
        variable: int,
        value: float,
    ):
        """

        Args:
            id_ ():
            transaction_type ():
            instruction_type ():
            site_id ():
            variable ():
            value ():
        """
        self.id = id_
        self.transaction_type = transaction_type
        self.instruction_type = instruction_type
        self.site_id = site_id
        self.variable = variable
        self.value = value
