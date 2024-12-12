from in_mem_db import Database
import unittest

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