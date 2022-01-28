import psycopg2

from django.http import JsonResponse

from my_settings import DBNAME, HOST, PORT, USER, PASSWORD

def connect_redshift(func):
    def wrapper(self, request, *args, **kwargs):
        try:
            connect = psycopg2.connect(
                dbname = DBNAME,
                host = HOST,
                port = PORT,
                user = USER,
                password = PASSWORD,
            )
            cursor = connect.cursor()
            request.cursor = cursor
            request.connect = connect
            return func(self, request, *args, **kwargs)

        except KeyError as e:
            return JsonResponse({"message":getattr(e, 'message', str(e))}, status=400)
        
        finally:
            if connect:
                connect.close()

    return wrapper
