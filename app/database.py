import psycopg2
import uuid
from datetime import date, time
import os
from datetime import date, datetime

user = os.getenv('POSTGRES_USER', 'bank_user')
password = os.getenv('POSTGRES_PASSWORD', 'bank_password')
#host = os.getenv('POSTGRES_HOST', 'localhost')
host = os.getenv('POSTGRES_HOST', '109.196.164.87')
port = os.getenv('POSTGRES_PORT_NUMBER', "5432")
database = os.getenv('POSTGRES_DB', 'bank')

conn = {
    'user': user,
    'password': password,
    'host': host,
    'port': port,
    'database': database
}

# def insert_scheduled_arrival(user_id, date_time_obj):
#     try:
#         connection = psycopg2.connect(**conn)
#         cursor = connection.cursor()
#         operation_uuid = str(uuid.uuid4())
#         current_date = datetime.now().date()
#         insert_query = "INSERT INTO reports (id, user_id, date, scheduled_arrival) VALUES (%s, %s, %s, %s)"
#         cursor.execute(insert_query, (operation_uuid, user_id, current_date, date_time_obj))
#         connection.commit()
#         cursor.close()
#         connection.close()
#         print("Data inserted successfully into the 'report' table.")
#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to the database or inserting data:", error)


def insert_scheduled_arrival(user_id, date_time_obj):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        operation_uuid = str(uuid.uuid4())
        current_date = datetime.now().date()

        insert_query = """
            INSERT INTO reports (id, user_id, date, scheduled_arrival, car_number, last_name, first_name, patronymic)
            SELECT %s, %s, %s, %s, d.car_number, d.last_name, d.first_name, d.patronymic
            FROM drivers AS d
            WHERE d.user_id = %s
            ON CONFLICT DO NOTHING
        """
        cursor.execute(insert_query, (operation_uuid, user_id, current_date, date_time_obj, user_id))

        connection.commit()
        cursor.close()
        connection.close()
        print("Data inserted successfully into the 'reports' table.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or inserting data:", error)

# def insert_actual_arrival(user_id, date_time_obj):
#     try:
#         connection = psycopg2.connect(**conn)
#         cursor = connection.cursor()
#         operation_uuid = str(uuid.uuid4())
#         current_date = datetime.now().date()
#         insert_query = "INSERT INTO reports (id, user_id, date, actual_arrival) VALUES (%s, %s, %s, %s)"
#         cursor.execute(insert_query, (operation_uuid, user_id, current_date, date_time_obj))
#         connection.commit()
#         cursor.close()
#         connection.close()
#         print("Data inserted successfully into the 'report' table.")
#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to the database or inserting data:", error)

def insert_actual_arrival(user_id, date_time_obj):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()

        current_date = datetime.now().date()
        select_query = "SELECT * FROM reports WHERE user_id = %s AND date = %s"
        cursor.execute(select_query, (user_id, current_date))
        existing_record = cursor.fetchone()

        if existing_record:
            update_query = "UPDATE reports SET actual_arrival = %s WHERE user_id = %s AND date = %s"
            cursor.execute(update_query, (date_time_obj, user_id, current_date))
        else:
            operation_uuid = str(uuid.uuid4())
            insert_query = "INSERT INTO reports (id, user_id, date, actual_arrival) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (operation_uuid, user_id, current_date, date_time_obj))

        connection.commit()
        cursor.close()
        connection.close()
        print("Data inserted successfully into the 'reports' table.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or inserting data:", error)


def insert_shipment(user_id, date_time_obj):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()

        current_date = datetime.now().date()
        select_query = "SELECT * FROM reports WHERE user_id = %s AND date = %s AND scheduled_arrival IS NOT NULL"
        cursor.execute(select_query, (user_id, current_date))
        existing_record = cursor.fetchone()

        if existing_record:
            update_query = "UPDATE reports SET shipment = %s WHERE user_id = %s AND date = %s"
            cursor.execute(update_query, (date_time_obj, user_id, current_date))
        else:
            operation_uuid = str(uuid.uuid4())
            insert_query = "INSERT INTO reports (id, user_id, date, shipment) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (operation_uuid, user_id, current_date, date_time_obj))

        connection.commit()
        cursor.close()
        connection.close()
        print("Data inserted successfully into the 'report' table.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or inserting data:", error)

def insert_departure(user_id, date_time_obj):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()

        current_date = datetime.now().date()
        select_query = "SELECT * FROM reports WHERE user_id = %s AND date = %s AND scheduled_arrival IS NOT NULL"
        cursor.execute(select_query, (user_id, current_date))
        existing_record = cursor.fetchone()

        if existing_record:
            update_query = "UPDATE reports SET departure = %s WHERE user_id = %s AND date = %s"
            cursor.execute(update_query, (date_time_obj, user_id, current_date))
        else:
            operation_uuid = str(uuid.uuid4())
            insert_query = "INSERT INTO reports (id, user_id, date, departure) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (operation_uuid, user_id, current_date, date_time_obj))

        connection.commit()
        cursor.close()
        connection.close()
        print("Data inserted successfully into the 'report' table.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or inserting data:", error)

def get_daily_report(report_date):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()

        select_query = "SELECT car_number, last_name, first_name, patronymic, scheduled_arrival, actual_arrival, shipment, departure FROM public.reports WHERE date = %s"
        cursor.execute(select_query, (report_date,))
        daily_report = cursor.fetchall()

        cursor.close()
        connection.close()

        return daily_report
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or executing query:", error)


def insert_alert(user_id, token, amount):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        alert_uuid = str(uuid.uuid4())
        current_date = date.today().strftime("%Y-%m-%d")
        insert_query = "INSERT INTO reports (signal_id, user_id, date, token, amount, flag) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, (alert_uuid, user_id, current_date, token, amount, False))
        connection.commit()
        cursor.close()
        connection.close()
        print("Data inserted successfully into the 'alerts' table.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or inserting data:", error)

def get_user_operations(user_id):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        select_query = "SELECT date, token, amount FROM operations WHERE user_id = %s"
        cursor.execute(select_query, (user_id,))
        operations = cursor.fetchall()
        cursor.close()
        connection.close()
        return operations
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the database or fetching data:", error)
        return []

