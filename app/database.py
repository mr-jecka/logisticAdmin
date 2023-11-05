import psycopg2
import os
import openpyxl
import requests
import logging
import uuid
from sshtunnel import SSHTunnelForwarder
from contextlib import contextmanager
from datetime import datetime

# Параметры подключения к БД
user = os.getenv('POSTGRES_USER', 'postgres')
password = os.getenv('POSTGRES_PASSWORD', 'postgres')
database = os.getenv('POSTGRES_DB', 'postgres')
db_port = os.getenv('POSTGRES_PORT_NUMBER', "5432")

# Параметры SSH туннеля
REMOTE_HOST = os.getenv('SSH_REMOTE_HOST')
REMOTE_SSH_PORT = os.getenv('SSH_REMOTE_PORT', 22)
REMOTE_USERNAME = os.getenv('SSH_USER')
REMOTE_PASSWORD = os.getenv('SSH_PASSWORD')
LOCAL_BIND_HOST = os.getenv('SSH_LOCAL_BIND_HOST', 'localhost')
LOCAL_BIND_PORT = os.getenv('SSH_LOCAL_BIND_PORT', 5432)

tunnel = SSHTunnelForwarder(  # Инициализация SSH туннеля
    (REMOTE_HOST, int(REMOTE_SSH_PORT)),
    ssh_username=REMOTE_USERNAME,
    ssh_password=REMOTE_PASSWORD,
    remote_bind_address=('localhost', int(db_port)),
    local_bind_address=(LOCAL_BIND_HOST, int(LOCAL_BIND_PORT)))


@contextmanager
def get_db_connection():  # Функция-генератор для управления соединениями
    if not tunnel.is_active:
        tunnel.start()
    conn_params = {
        'database': database,
        'user': user,
        'password': password,
        'host': LOCAL_BIND_HOST,
        'port': tunnel.local_bind_port
    }
    conn = psycopg2.connect(**conn_params)
    try:
        yield conn
    finally:
        conn.close()
        tunnel.stop()


def get_info_for_report():
    with get_db_connection() as connection:
        today = datetime.now().date()
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT num_th, lastname, num_car, arrival_time_fact, shipment_time, departure_time, arrival_time FROM public.reestr_table WHERE date_th = %s",
                (today,))
            routes = cursor.fetchall()
            return routes
        except (Exception, psycopg2.Error) as error:
            print("Error fetching routes from the database:", error)
        finally:
            cursor.close()


def get_routes():
    with get_db_connection() as connection:
        try:
            cursor = connection.cursor()
            # tomorrow = report_date.strftime("%Y-%m-%d")
            cursor.execute(f"SELECT num_th FROM public.reestr_table WHERE lastname IS NULL")
            # cursor.execute(f"SELECT num_th FROM public.reestr_table WHERE date_th = '{tomorrow}'")
            routes = cursor.fetchall()
            return routes
        except (Exception, psycopg2.Error) as error:
            print("Error fetching routes from the database:", error)
        finally:
            cursor.close()


def get_routes():
    with get_db_connection() as connection:
        try:
            cursor = connection.cursor()
            #tomorrow = report_date.strftime("%Y-%m-%d")
            cursor.execute(f"SELECT num_th FROM public.reestr_table WHERE lastname IS NULL")
            #cursor.execute(f"SELECT num_th FROM public.reestr_table WHERE date_th = '{tomorrow}'")
            routes = cursor.fetchall()
            return routes
        except (Exception, psycopg2.Error) as error:
            print("Error fetching routes from the database:", error)
        finally:
            cursor.close()


#def get_info_for_report():
#    connection = psycopg2.connect(**conn)
#    today = datetime.now().date()
#    try:
#        cursor = connection.cursor()
#        cursor.execute("SELECT num_th, lastname, num_car, arrival_time_fact, shipment_time,# departure_time, arrival_time FROM public.reestr_table WHERE date_th = %s",
#                       (today,))
#        routes = cursor.fetchall()
#        return routes
#    except (Exception, psycopg2.Error) as error:
#        print("Error fetching routes from the database:", error)
#    finally:
#        if connection:
#            cursor.close()
#            connection.close()


def get_driver_last_name(driver_id):
    with get_db_connection() as connection:
        try:
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
            cursor.close()


def insert_driver_for_route(selected_route, selected_driver, selected_driver_car, loading_time):
    with get_db_connection() as connection:
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"UPDATE public.reestr_table SET lastname = %s, num_car = %s, arrival_time = %s WHERE num_th = %s",
                (selected_driver, selected_driver_car, loading_time, selected_route)
            )
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error updating reestr_table in the database:", error)
            connection.rollback()
        finally:
            cursor.close()


def get_main_json():
    with get_db_connection() as connection:
        try:
            cursor = connection.cursor()

            def fetch_addresses(num_th, is_null):
                condition = "IS NULL" if is_null else "IS NOT NULL"
                cursor.execute(
                    f"SELECT * FROM public.address_table WHERE num_th = %s AND arrival_date {condition} ORDER BY arrival_date ASC",
                    (num_th,))
                return cursor.fetchall()

            cursor.execute("SELECT * FROM public.reestr_table")
            reestr_entries = cursor.fetchall()
            result = []

            for entry in reestr_entries:
                num_th = entry[0]
                addresses = fetch_addresses(num_th, False)
                addresses += fetch_addresses(num_th, True)
                address_list = []
                for idx, addr in enumerate(addresses):
                    if addr[0] is not None:
                        address_list.append({
                            "order": idx,
                            "num_route": addr[0],
                            "num_shop": addr[1],
                            "code_tt": addr[2],
                            "address_delivery": addr[3],
                            "count_boxes": addr[4],
                            "weight": addr[5]
                        })

                if address_list:
                    result.append({
                        "num_th": num_th,
                        "date_th": entry[1],
                        "total_count_boxes": entry[2],
                        "total_weight": entry[3],
                        "addreses": address_list
                    })

            return result

        except Exception as e:
            print(f"Error: {e}")
            return []

# def get_main_json():
#     try:
#         connection = psycopg2.connect(**conn)
#         cursor = connection.cursor()
#         cursor.execute("SELECT num_route FROM public.address_table WHERE arrival_date IS NULL")
#         result = cursor.fetchall()
#         logging.info("Fetched num_route values: %s", [row[0] for row in result if row[0]])
#         if result:
#             return [row[0] for row in result if row[0]]
#         return []
#     except (Exception, psycopg2.Error) as error:
#         logging.error("Error getting num_route from address_table:", error)
#         return []
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()


def update_driver_assignment(driver_id, selected_route):
    with get_db_connection() as connection:
        try:
            cursor = connection.cursor()
            cursor.execute(f"UPDATE public.drivers SET user_id = {selected_route} WHERE car_number = '{driver_id}'")
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error updating driver assignment in the database:", error)
            connection.rollback()
        finally:
            cursor.close()


def get_drivers_for_route():
    with get_db_connection() as connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT last_name, num_car FROM public.drivers WHERE user_id IS NOT NULL")
            drivers = cursor.fetchall()
            return drivers
        except (Exception, psycopg2.Error) as error:
            print("Error fetching available drivers from the database:", error)
        finally:
            cursor.close()


def generate_and_assign_uuid(cursor, table_name, id_column):
    new_uuid = uuid.uuid4()
    new_uuid_str = new_uuid.hex  # Преобразование UUID в строку
    cursor.execute(f"""
        UPDATE {table_name}
        SET {id_column} = %s
        WHERE {id_column} IS NULL;
    """, (new_uuid_str,))
    cursor.connection.commit()
    return new_uuid


def insert_excel_to_db(excel_file_path, db_params):
    with get_db_connection() as connection:
        cursor = connection.cursor()
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
                    date_str = row[3]
                    if isinstance(date_str, datetime):
                        date_str = date_str.strftime('%d.%m.%Y')
                    try:
                        date_th = datetime.strptime(date_str, '%d.%m.%Y')
                        th["date_th"] = date_th.strftime('%Y-%m-%d')
                    except ValueError:
                        logging.error(f"Error converting date string: {date_str}")
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

            for th in ths:
                cursor.execute("""
                    INSERT INTO reestr_table (num_th, date_th, total_count_boxes, total_weight)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                """, (th["num_th"], th["date_th"], th["total_count_boxes"], th["total_weight"]))
                th_id = cursor.fetchone()[0]

                for addr in th["addresses"]:
                    #coordinates = revers_geocoding_yandex(addr["address_delivery"])
                    # if coordinates:
                    #     addr["latitude"], addr["longitude"] = coordinates
                    #     cursor.execute("""
                    #         INSERT INTO address_table (
                    #         num_th, num_route, num_shop, code_tt, address_delivery, count_boxes, weight, th_id, latitude, longitude)
                    #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    #     """, (addr["num_th"], addr["num_route"], addr["num_shop"], addr["code_tt"], addr["address_delivery"],
                    #           addr["count_boxes"],
                    #           addr["weight"], th_id, addr["latitude"], addr["longitude"]))
                    # else:
                    cursor.execute("""
                        INSERT INTO address_table (
                        num_th, num_route, num_shop, code_tt, address_delivery, count_boxes, weight, th_id, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (addr["num_th"], addr["num_route"], addr["num_shop"], addr["code_tt"], addr["address_delivery"],
                          addr["count_boxes"],
                          addr["weight"], th_id, None, None))
                    new_uuid_in_reestr = generate_and_assign_uuid(cursor, 'reestr_table', 'id')
                    new_uuid_in_address = generate_and_assign_uuid(cursor, 'address_table', 'id')
                    print("New UUID in reestr_table:", new_uuid_in_reestr)
                    print("New UUID in address_table:", new_uuid_in_address)
                    logging.info("Data inserted into the database successfully.")
                    return True, "Data inserted into the database successfully."
        except Exception as e:
            logging.error(f"Error inserting data into the database: {e}")
            return False, f"Error inserting data into the database: {e}"
        finally:
            cursor.close()


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
    with get_db_connection() as connection:
        cursor = connection.cursor()
        try:
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
            print("Data inserted successfully into the 'reports' table.")
            logging.info("Data inserted successfully into the 'reports' table.")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to the database or inserting data:", error)
            logging.error("Error while connecting to the database or inserting data:", error)
        finally:
            cursor.close()


def insert_actual_arrival(user_id, date_time_obj):
    with get_db_connection() as connection:
        cursor = connection.cursor()
        try:
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
            print("Data inserted successfully into the 'report' table.")
            logging.info("Data inserted successfully into the 'reports' table.")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to the database or inserting data:", error)
            logging.error("Error while connecting to the database or inserting data:", error)
        finally:
            cursor.close()


def insert_shipment(user_id, date_time_obj):
    with get_db_connection() as connection:
        cursor = connection.cursor()
        try:
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
            print("Data inserted successfully into the 'report' table.")
            logging.info("Data inserted successfully into the 'reports' table.")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to the database or inserting data:", error)
            logging.error("Error while connecting to the database or inserting data:", error)
        finally:
            cursor.close()


def insert_departure(user_id, date_time_obj):
    with get_db_connection() as connection:
        cursor = connection.cursor()
        try:
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
            print("Data inserted successfully into the 'report' table.")
            logging.info("Data inserted successfully into the 'reports' table.")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to the database or inserting data:", error)
            logging.error("Error while connecting to the database or inserting data:", error)
        finally:
            cursor.close()


def get_internal_report(report_date):
    with get_db_connection() as connection:
        cursor = connection.cursor()
        try:
            select_query = "SELECT car_number, last_name, first_name, patronymic, scheduled_arrival, actual_arrival, shipment, departure FROM public.reports WHERE date = %s"
            cursor.execute(select_query, (report_date,))
            daily_report = cursor.fetchall()
            return daily_report
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to the database or executing query:", error)
            logging.error("Error while connecting to the database or inserting data:", error)
        finally:
            cursor.close()

