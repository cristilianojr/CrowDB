import mysql
import mysql.connector.pooling
from mysql.connector import MySQLConnection
from mysql.connector.pooling import MySQLConnectionPool


class MySQLManager:
    def __init__(self, host: str, user: str, password: str, database: str = None) -> None:
        self.configs: dict = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
        }
        self.__connection__: MySQLConnection | None = None

    def open(self) -> bool:
        try:
            self.__connection__ = mysql.connector.connect(**self.configs)
            return True
        except Exception as ConnectionError:
            print(f'Connection Error | {ConnectionError}')
            return False

    def close(self) -> None:
        try:
            self.__connection__.close()
            return True
        except Exception as ConnectionError:
            print(f'Close Connection Error | {ConnectionError}')
            return False

    def action(self, query: str, params: tuple | tuple[tuple]) -> None:
        status: bool = False
        error: Exception = None

        try:
            self.open()
            cursor = self.__connection__.cursor()
            cursor.execute(query, params) if len(params) <= 1 else cursor.executemany(query, params)
            self.__connection__.commit()
            status = True
            cursor.close()

        except Exception as ActionError: 
            print('ActionError:', ActionError)
            self.__connection__.rollback()
            error = ActionError

        self.close()

        return status, error

    def request(self, query: str, params: tuple | None = None, fetch: str = 'all') -> tuple | None:
        try:
            self.open()
            cursor = self.__connection__.cursor()

            cursor.execute(query) if params == None else cursor.execute(query, params)
            data: tuple | None = cursor.fetchall() if fetch == 'all' else cursor.fetchone() if fetch == 'one' else None
            cursor.close()

        except Exception as ActionError: 
            print('RequestError:', ActionError)
            data = None

        self.close()

        return data

    def request_batches(self, query: str, params: tuple | None = None, batches_size: int = 10) -> tuple | None:
        batches: list = []

        try:
            self.open()
            cursor = self.__connection__.cursor()

            cursor.execute(query) if params == None else cursor.execute(query, params)

            while True:
                rows: tuple | None = cursor.fetchmany(batches_size)
                
                if not rows:
                    break

                batches.append([row for row in rows])
                cursor.close()

        except Exception as ActionError: 
            print('RequestError:', ActionError)
            batches = None
        
        self.close()

        return batches

    def ping(self) -> bool:
        try:
            self.open()
            self.__connection__.ping(reconnect=True)
            print("Connection Status: \033[32mTrue\033[m")
            return True
        except mysql.connector.Error as e:
            print("Connection Status: \033[33mFalse\033[m")
            print(e)

        self.close()
        return False


class MySQLPoollingManager:
    def __init__(self, host: str, user: str, password: str, database: str, pool_name: str) -> None:
        self.configs: dict = {
            "pool_name": pool_name,
            "pool_size": 20,
            'host': host,
            'user': user,
            'password': password,
            'database': database,
        }
        self.pool: MySQLConnectionPool | None = self.create_pool()

    def create_pool(self) -> MySQLConnectionPool | None:
        try:
            return MySQLConnectionPool(**self.configs)
        except Exception as e:
            print('ExceptionError', e)
            return None

    def kill_pool(self) -> None:
        if self.pool != None:
            self.pool._remove_connections()
            self.pool = None

    def action(self, query: str, params: tuple | None = None) -> None:
        try:
            cnx: MySQLConnection = self.pool.get_connection()
            cursor = cnx.cursor()
            cursor.execute(query, params) if len(params) <= 1 else cursor.executemany(query, params)
            cnx.commit()
        except Exception as e:
            print('ActionError |', e)

        cnx.close()

    def request(self, query: str, params: tuple | None = None, fetch: str = 'all') -> tuple | None:
        try:
            cnx: MySQLConnection = self.pool.get_connection()
            cursor = cnx.cursor()
            cursor.execute(query) if params == None else cursor.execute(query, params)
            data: tuple | None = cursor.fetchall() if fetch == 'all' else cursor.fetchone() if fetch == 'one' else None
            cursor.close()

        except Exception as ActionError: 
            print('RequestError:', ActionError)
            data = None

        cnx.close()

        return data
