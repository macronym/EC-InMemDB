from typing import Dict

class Database:
    _db: Dict[str, int] = {}
    _current_transaction: Dict[str, int] = {}   # The data of the current transaction used to update _db
    _transaction = False        


    def begin_transaction(self) -> int:
        """
        Starts a new transaction.
        """

        # Only a single transaction allowed at a time.
        if self._transaction is True:
            raise Exception("Transaction already active")
        else:
            self._transaction = True
            return 0
    
    def put(self, key: str, value: int) -> int:
        """
        Creates or updates a key-value pair in the database.
        """ 

        # If called when a transaction is not in progress, throw exception
        if self._transaction is False:
            raise Exception("Transaction not currently active")
        
        # Otherwise, updates the database.
        else:
            self._current_transaction[key] = value
            return 0

    def get(self, key: str):
        """
        Return value associated with the key, or None if the key doesn't exist.
        """

        # This functionality is built-in to python dictionaries already.
        return self._db.get(key, None)

    def commit(self) -> int:
        """
        Commits an open transaction
        """
        if self._transaction is False:
            raise Exception("No current transaction")
        else:
            # Updates the main database with the transaction data
            self._db.update(self._current_transaction)
            self._current_transaction = {}
            self._transaction = False
            return 0

    def rollback(self):
        """
        Ends the current transaction without making changes.
        """
        if self._transaction is False:
            raise Exception("No current transaction")
        else:
            self._current_transaction = {}
            self._transaction = False
            return 0
        