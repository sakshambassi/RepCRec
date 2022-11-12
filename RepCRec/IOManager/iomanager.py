import re
from Enums import enums
from Transaction import transaction


class IOManager:
    def __init__(self):
        pass

    @staticmethod
    def get_transaction_or_site_id(input_: str):
        """

        Args:
            input_ ():

        Returns:

        """
        begin, end = input_.find("("), input_.find(")")
        return int(input_[begin+2:end])

    def input_file(self, filename: str) -> list[transaction.Transaction]:
        """

        Args:
            filename ():

        Returns:

        """
        transactions = []
        ro_transactions = []
        with open(filename, 'r') as f:
            for line in f:
                if line[:2] != '//' and line.strip():  # check if line is not a comment
                    instruction = None
                    transaction_type = enums.TransactionType.NONE
                    transaction_id, site_id, variable, value = 0, 0, 0, 0
                    if line.find("beginRO") == 0:
                        instruction = enums.InstructionType.BEGINRO
                        transaction_id = IOManager.get_transaction_or_site_id(line)
                        ro_transactions.append(transaction_id)
                    elif line.find("begin") == 0:
                        instruction = enums.InstructionType.BEGIN
                        transaction_id = IOManager.get_transaction_or_site_id(line)
                    elif line.find("end") == 0:
                        instruction = enums.InstructionType.END
                        transaction_id = IOManager.get_transaction_or_site_id(line)
                    elif line.find("fail") == 0:
                        instruction = enums.InstructionType.FAIL
                        site_id = IOManager.get_transaction_or_site_id(line)
                    elif line.find("dump") == 0:
                        instruction = enums.InstructionType.DUMP
                    elif line.find("recover") == 0:
                        instruction = enums.InstructionType.RECOVER
                        site_id = IOManager.get_transaction_or_site_id(line)
                    else:
                        instruction = enums.InstructionType.NO
                        if line[0] == 'W':
                            transaction_type = enums.TransactionType.WRITE
                            transaction_id, variable, value = self.process_write(line)
                        elif line[0] == 'R':
                            transaction_id, variable = self.process_read(line)
                            # TODO: assign transaction
                        else:
                            raise Exception("Unknown transaction type")

                    transactions.append(transaction.Transaction(
                        id_=transaction_id,
                        transaction_type=transaction_type,
                        instruction_type=instruction,
                        site_id=site_id,
                        variable=variable,
                        value=value
                    ))
        return transactions

    def process_read(self, input_: str):
        """

        Args:
            input_ ():

        Returns:

        """
        begin, end = input_.find("("), input_.find(")")
        raw_details = input_[begin + 2:end].split(',')
        transaction_id = int(raw_details[0])
        variable = int(re.findall(r'\d+', raw_details[1])[0])
        return transaction_id, variable

    def process_write(self, input_: str):
        """

        Args:
            input_ ():

        Returns:

        """
        begin, end = input_.find("("), input_.find(")")
        raw_details = input_[begin+2:end].split(',')
        transaction_id = int(raw_details[0])
        variable = int(re.findall(r'\d+', raw_details[1])[0])
        value = int(raw_details[2])
        return transaction_id, variable, value




