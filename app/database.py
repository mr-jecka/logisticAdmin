import psycopg2
import os
import openpyxl
import requests
import logging
from datetime import datetime
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

user = os.getenv('POSTGRES_USER', 'bank_user')
password = os.getenv('POSTGRES_PASSWORD', 'bank_password')
host = os.getenv('POSTGRES_HOST', 'localhost')
#host = os.getenv('POSTGRES_HOST', '194.87.92.15')
port = os.getenv('POSTGRES_PORT_NUMBER', "5432")
database = os.getenv('POSTGRES_DB', 'bank')

conn = {
    'user': user,
    'password': password,
    'host': host,
    'port': port,
    'database': database
}


def get_routes():
    connection = psycopg2.connect(**conn)
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
        if connection:
            cursor.close()
            connection.close()


def get_info_for_report():
    connection = psycopg2.connect(**conn)
    today = datetime.now().date()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT num_th, lastname, num_car, arrival_time_fact, shipment_time, departure_time, arrival_time FROM public.reestr_table WHERE date_th = %s",
                       (today,))
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


def insert_user_id_for_addresses(selected_route, user_id):
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        update_query = """
        UPDATE public.address_table
        SET user_id = %s
        WHERE num_th = %s AND user_id IS NULL;
        """
        cursor.execute(update_query, (user_id, selected_route))
        updated_rows = cursor.rowcount  # Количество обновленных строк
        connection.commit()
        if updated_rows > 0:
            logger.info(f"User ID {user_id} присвоен для маршрута {selected_route}. Обновлено записей: {updated_rows}")
        else:
            logger.warning(f"Нет записей для обновления user_id для маршрута {selected_route}")
    except Exception as e:
        logger.exception("Ошибка при обновлении user_id в адресной таблице:")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()




def insert_driver_for_route(selected_route, selected_driver, selected_driver_car, loading_time, user_id):
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()
        cursor.execute(
            f"UPDATE public.reestr_table SET lastname = %s, num_car = %s, arrival_time = %s, user_id = %s WHERE num_th = %s",
            (selected_driver, selected_driver_car, loading_time, user_id, selected_route)
        )
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error updating reestr_table in the database:", error)
        connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_main_json():
    try:
        connection = psycopg2.connect(**conn)
        cursor = connection.cursor()

        def fetch_addresses(num_th, is_null):
            condition = "IS NULL" if is_null else "IS NOT NULL"
            cursor.execute(
                f"SELECT * FROM public.address_table WHERE num_th = %s AND arrival_time {condition} ORDER BY arrival_time ASC",
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
        cursor.execute("SELECT last_name, num_car, user_id FROM public.drivers WHERE user_id IS NOT NULL")
        drivers = cursor.fetchall()
        return drivers
    except (Exception, psycopg2.Error) as error:
        print("Error fetching available drivers from the database:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


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
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return
    try:
        wb = openpyxl.load_workbook(excel_file_path, data_only=True)
        sheet = wb.active

        ths = []
        th = {}
        index_number = 0  # Счётчик для порядкового номера

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
                index_number = 0  # Обнуление счётчика при новом "Номере ТН"
            else:
                index_number += 1  # Увеличение порядкового номера для "Номера перемещения"
                addr = {}
                addr["num_th"] = th["num_th"]
                addr["num_route"] = row[4]
                addr["num_shop"] = row[6]
                addr["code_tt"] = row[7]
                addr["address_delivery"] = row[8]
                addr["count_boxes"] = int(row[9])
                addr["weight"] = float(row[10])
                addr["index_number"] = index_number  # Добавление порядкового номера
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
                        cursor.execute("""
                            INSERT INTO address_table (
                            num_th, num_route, num_shop, code_tt, address_delivery,
                             index_number, count_boxes, weight, th_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, (addr["num_th"], addr["num_route"], addr["num_shop"], addr["code_tt"],
                              addr["address_delivery"], addr["index_number"], addr["count_boxes"], addr["weight"], th_id))
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
        conn.close()


# def revers_geocoding_yandex(address):
#     token = '5d9f623c-b003-4632-956f-464db92caaae'
#     headers = {"Accept-Language": "ru"}
#     response = requests.get(
#         f'https://geocode-maps.yandex.ru/1.x/?apikey={token}&geocode={address}&format=json',
#         headers=headers).json()
#     if (
#             "response" in response
#             and "GeoObjectCollection" in response["response"]
#             and "featureMember" in response["response"]["GeoObjectCollection"]
#     ):
#         geo_objects = response["response"]["GeoObjectCollection"]["featureMember"]
#         if geo_objects:
#             first_result = geo_objects[0]["GeoObject"]
#             latitude, longitude = map(float, first_result["Point"]["pos"].split())
#             return latitude, longitude
#     return None


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


def get_internal_report(report_date):
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
