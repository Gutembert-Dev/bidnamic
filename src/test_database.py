import unittest
import database as dbs


class TestDatabase(unittest.TestCase):


    """
    Test if the database does not exist
    """
    def test_exist(self):
        db = dbs.Database('localhost', 'postgres', 'camer', 'camer')
        db_name = 'bidnamic'
        self.assertEqual(db.exist(db_name), False)

    
    """
    Test if the database is created
    """
    def test_create(self):
        db = dbs.Database('localhost', 'postgres', 'camer', 'camer')
        db_name = 'bidnamic'
        db.create(db_name)
        self.assertEqual(db.exist(db_name), True)


    """
    Test if the database exists
    """
    def test_exist(self):
        db = dbs.Database('localhost', 'postgres', 'camer', 'camer')
        db_name = 'bidnamic'
        self.assertEqual(db.exist(db_name), True)

        
    """
    Test if the database tables are initialized
    """
    def test_initalize_tables(self):
        db = dbs.Database('localhost', 'postgres', 'camer', 'camer')
        db_name = 'bidnamic'
        db.initalize_tables(db_name)
        table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        res = db.run_query(db_name, table_query)
        for table_name in res:
            self.assertEqual(table_name in ['campaigns', 'adgroups', 'search_terms'], True)


    """
    Test if the csv data are loaded
    """
    def test_load_csv(self):
        db = dbs.Database('localhost', 'postgres', 'camer', 'camer')
        db_name = 'bidnamic'
        db.load_csv(db_name)
        data_query = "select count(*) from search_terms"
        res = db.run_query(db_name, data_query)
        for data_rows in res:
            self.assertEqual(data_rows, 221855)

        
    """
    Test if the run query util works
    """
    def test_run_query(self):
        db = dbs.Database('localhost', 'postgres', 'camer', 'camer')
        query = "SELECT datname FROM pg_database"
        db_name = 'bidnamic'
        res = db.run_query(db_name, query)
        self.assertEqual('template0' in res, True)


if __name__ == "__main__":
    unittest.main()
