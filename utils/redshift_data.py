import psycopg2
import pandas.io.sql as psql

from django.http import JsonResponse

class RedshiftData:
    def __init__(self, connect, query):
        self.connect = connect
        self.query = query

    def get_data(self):
        try:
            data = psql.read_sql_query(self.query, self.connect)
            return data

        except:
            return None
