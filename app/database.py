import psycopg2
import uuid
import os
from datetime import date, datetime
import openpyxl
import requests


user = os.getenv('POSTGRES_USER', 'bank_user')
password = os.getenv('POSTGRES_PASSWORD', 'bank_password')
#host = os.getenv('POSTGRES_HOST', 'localhost')
host = os.getenv('POSTGRES_HOST', '194.87.92.15')
port = os.getenv('POSTGRES_PORT_NUMBER', "5432")
database = os.getenv('POSTGRES_DB', 'bank')

conn = {
    'user': user,
    'password': password,
    'host': host,
    'port': port,
    'database': database
}


def get_tomorrow_routes(report_date):
    connection = psycopg2.connect(**conn)
    try:
        cursor = connection.cursor()
        tomorrow = report_date.strftime("%Y-%m-%d")
        cursor.execute(f"SELECT num_th FROM public.reestr_table WHERE date_th = '{tomorrow}'")
        routes = cursor.fetchall()
        return routes
    except (Exception, psycopg2.Error) as error:
        print("Error fetching routes from the database:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_driver_last_name(driver_id):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        cursor.execute(f"SELECT last_name FROM public.drivers WHERE car_number = '{driver_id}'")
        last_name = cursor.fetchone()
        if last_name:
            return last_name[0]
        else:
            return None
    except (Exception, psycopg2.Error) as error:
        print("Error fetching driver's last name from the database:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_driver_for_route(selected_route, selected_driver):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        cursor.execute(
            f"UPDATE public.reestr_table SET lastname = '{selected_driver}' WHERE num_th = '{selected_route}'")
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error updating lastname in reestr_table in the database:", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()


def update_driver_assignment(driver_id, selected_route):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        cursor.execute(f"UPDATE public.drivers SET user_id = {selected_route} WHERE car_number = '{driver_id}'")
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error updating driver assignment in the database:", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_drivers_for_route():
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        cursor.execute("SELECT last_name FROM public.drivers WHERE user_id IS NOT NULL")
        drivers = cursor.fetchall()
        return drivers
    except (Exception, psycopg2.Error) as error:
        print("Error fetching available drivers from the database:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_excel_to_db(excel_file_path, db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return

    try:
        ths = []
        wb = openpyxl.load_workbook(excel_file_path, data_only=True)
        sheet = wb.active
        th = {}

        for row in sheet.iter_rows(min_row=5, values_only=True):
            if row[2] is not None:
                if len(th.keys()) != 0:
                    ths.append(th)
                th = {}
                th["num_th"] = row[2]
                th["date_th"] = row[3].strftime('%Y-%m-%d')
                th["total_count_boxes"] = int(row[9])
                th["total_weight"] = float(row[10])
                th["addresses"] = []
            else:
                addr = {}
                addr["num_th"] = th["num_th"]
                addr["num_route"] = row[4]
                addr["num_shop"] = row[6]
                addr["code_tt"] = row[7]
                addr["address_delivery"] = row[8]
                addr["count_boxes"] = int(row[9])
                addr["weight"] = float(row[10])
                th["addresses"].append(addr)
        ths.append(th)

        with conn:
            with conn.cursor() as cursor:
                for th in ths:
                    cursor.execute("""
                        INSERT INTO reestr_table (num_th, date_th, total_count_boxes, total_weight)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id;
                    """, (th["num_th"], th["date_th"], th["total_count_boxes"], th["total_weight"]))
                    th_id = cursor.fetchone()[0]

                    for addr in th["addresses"]:
                        coordinates = revers_geocoding_yandex(addr["address_delivery"])
                        if coordinates:
                            addr["latitude"], addr["longitude"] = coordinates
                            cursor.execute("""
                                INSERT INTO address_table (
                                num_th, num_route, num_shop, code_tt, address_delivery, count_boxes, weight, th_id, latitude, longitude)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (addr["num_th"], addr["num_route"], addr["num_shop"], addr["code_tt"], addr["address_delivery"],
                                  addr["count_boxes"],
                                  addr["weight"], th_id, addr["latitude"], addr["longitude"]))
                        else:
                            cursor.execute("""
                                INSERT INTO address_table (
                                num_th, num_route, num_shop, code_tt, address_delivery, count_boxes, weight, th_id, latitude, longitude)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (addr["num_th"], addr["num_route"], addr["num_shop"], addr["code_tt"], addr["address_delivery"],
                                  addr["count_boxes"],
                                  addr["weight"], th_id, None, None))
        print("Data inserted into the database successfully.")
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
    finally:
        conn.close()


def revers_geocoding_yandex(address):
    token = '5d9f623c-b003-4632-956f-464db92caaae'
    headers = {"Accept-Language": "ru"}
    response = requests.get(
        f'https://geocode-maps.yandex.ru/1.x/?apikey={token}&geocode={address}&format=json',
        headers=headers).json()
    if (
            "response" in response
            and "GeoObjectCollection" in response["response"]
            and "featureMember" in response["response"]["GeoObjectCollection"]
    ):
        geo_objects = response["response"]["GeoObjectCollection"]["featureMember"]
        if geo_objects:
            first_result = geo_objects[0]["GeoObject"]
            latitude, longitude = map(float, first_result["Point"]["pos"].split())
            return latitude, longitude
    return None


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




