from typing import Dict
import unittest

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
    

class TestDatabase(unittest.TestCase):

    def testFromFig2Example(self):
        """
        This test closely follows the figure 2 example in the 
        assignment documentation.
        """
        db = Database()

        # Doesn't exist in db yet.
        self.assertEqual(db.get("A"), None)
        
        # Should return an error because a transaction is not active
        with self.assertRaises(Exception) as cm:
            db.put("A", 5)

        # Start a new transaction
        self.assertEqual(db.begin_transaction(), 0)

        # Should return an error because a transaction is already active
        with self.assertRaises(Exception) as cm:
            db.begin_transaction()

        # Add a new key
        self.assertEqual(db.put("A", 5), 0)

        # Should return None, as A hasn't yet been committed/added.
        self.assertEqual(db.get("A"), None)

        # Updates A's value to 6 within the transaction
        self.assertEqual(db.put("A", 6), 0)

        # Successfully commits open transaction
        self.assertEqual(db.commit(), 0)

        # Check if A's value is now 6.
        self.assertEqual(db.get("A"), 6)

        # No active transaction to commit error
        with self.assertRaises(Exception):
            db.commit()

        # No active transaction to commit error
        with self.assertRaises(Exception):
            db.rollback()

        # B does not exist in the database
        self.assertEqual(db.get("B"), None)

        # Starts a new transaction
        self.assertEqual(db.begin_transaction(), 0)

        # Set B's value to 10 within the transaction
        self.assertEqual(db.put("B", 10), 0)

        # Rollback the transaction
        self.assertEqual(db.rollback(), 0)

        # B was rolled back so should not exist in the database
        self.assertEqual(db.get("B"), None)   

if __name__ == "__main__":
    unittest.main()