from cql_queries import create_keyspace, create_table_queries, drop_table_queries

class CqlHelper(object):
    def __init__(self, session, keyspace):
        self.session = session
        self.keyspace = keyspace

    def create_keyspace(self):
        """Create keyspace
        """
        self.session.execute(create_keyspace)
        self.session.set_keyspace(self.keyspace)
        print("Keyspace created: {}".format(self.keyspace))

    def create_tables(self):
        """Create tables
        """
        self.session.set_keyspace(self.keyspace)
        for query in create_table_queries:
            self.session.execute(query)
        print("Tables created")
    
    def insert_data(self, query, data):
        """Insert data
        """
        self.session.set_keyspace(self.keyspace)
        self.session.execute(query,data)
 
    def drop_tables(self):
        """Drop tables
        """
        self.session.set_keyspace(self.keyspace)
        for query in drop_table_queries:
            self.session.execute(query)
        print("Tables deleted")
 