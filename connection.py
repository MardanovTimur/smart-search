import psycopg2

database = 'postgres'
user = 'postgres'
password = 'postgres'
host = '0.0.0.0'
port = 4321


class PGConnection:

    def __init__(self, database=database,
                 user=user,
                 password=password,
                 host=host,
                 port=port):
        self.database = database,
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def __enter__(self, ):
        self.connection = psycopg2.connect(database='postgres',
                                           user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           port=self.port)
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        if exc_type:
            print(exc_val)
            raise

