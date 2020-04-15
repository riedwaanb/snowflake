import snowflake.connector
import pandas as pd

class snowQ(object):
    def __init__(self, user: str, password: str, account: str):
        self.user = user
        self.account = account
        self.ctx = snowflake.connector.connect(user=user,
                                               password=password,
                                               account=account)
        cs = self.ctx.cursor()
        try:
            cs.execute("SELECT current_version()")
            one_row = cs.fetchone()
            self.version = one_row[0]
        finally:
            cs.close()
    
    def __del__(self):
        self.ctx.close()
    
    # Execute Query
    def sql(self, query: str, timeout=10):
        cs = self.ctx.cursor()
        try:
            cs.execute(query, timeout=timeout)
        except Exception as e:
            if e.errno == 604:
                print("Query Timeout, rolling back")
                cs.execute("rollback")
            else:
                raise e
        return cs
    
    # Execute Query providing a SQL file that holds the Query
    def sqlf(self, filename: str, timeout=10):
        cs = self.ctx.cursor()

        if len(filename) > 0:
            with open(filename, 'r') as file:
                query = file.read().replace('\n', '')
                file.close()
        return self.sql(query, timeout)
