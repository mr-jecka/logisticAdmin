import psycopg2
import openpyxl
import uuid
import os
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func,  Column, Integer, String, Numeric, Boolean, Time, Date, and_
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from datetime import timedelta
import requests


Base = declarative_base()


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

connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()


class DriversTable(Base):
    __tablename__ = 'drivers'

    id = Column(Integer, primary_key=True)
    num_car = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    first_name = Column(String(255), nullable=False)
    patronymic = Column(String(255))
    user_id = Column(Integer)
    like_num_shop = Column(String(255))


class ReestrTable(Base):
    __tablename__ = 'reestr_table'

    num_th = Column(String(20))
    date_th = Column(Date)
    total_count_boxes = Column(Integer)
    total_weight = Column(Numeric(10, 2))
    lastname = Column(String(255))
    notification = Column(Boolean)
    id = Column(UUID, primary_key=True)
    num_car = Column(String(255))
    shipment_time = Column(Time)
    arrival_time = Column(Time)
    departure_time = Column(Time)
    arrival_time_fact = Column(Time)
    user_id = Column(Integer)


class AddressTable(Base):
    __tablename__ = 'address_table'

    num_route = Column(String)
    num_shop = Column(String(20))
    code_tt = Column(String(20))
    address_delivery = Column(String(255))
    count_boxes = Column(Integer)
    weight = Column(Numeric(10, 2))
    th_id = Column(Integer)
    num_th = Column(String(20))
    last_name = Column(String(255))
    arrival_time = Column(Time)
    id = Column(UUID, primary_key=True)
    departure_time = Column(Time)
    index_number = Column(Integer)
    user_id = Column(Integer)


def calculate_delivery_order(departure_place, api_key, destinations):
    finish_road = []
    while any(not v[1] for v in destinations.values()):
        dest = "|".join(k for k, v in destinations.items() if not v[1])
        params = {
            "origins": departure_place,
            "destinations": dest,
            "mode": "truck",
            "apikey": api_key
        }
        res = requests.get("https://api.routing.yandex.net/v2/distancematrix", params=params).json()
        rows = res["rows"][0]['elements']
        result_dest = [{"coord": coord, "distance": row['distance']['value']} for coord, row in zip(dest.split('|'), rows)]
        sorted_rows = sorted(result_dest, key=lambda d: d['distance'])
        closest_coord = sorted_rows[0]['coord']
        finish_road.append(closest_coord)
        destinations[closest_coord][1] = True
        departure_place = closest_coord
    return finish_road


def update_index_numbers(day):
    connection = psycopg2.connect(**conn)
    cursor = connection.cursor()
    cursor.execute("SELECT num_th FROM public.reestr_table WHERE user_id IS NULL and date_th = %s", (day,))
    num_ths = cursor.fetchall()

    for num_th in num_ths:
        cursor.execute("SELECT longitude, latitude, address_delivery FROM public.address_table WHERE num_th = %s", (num_th,))
        destinations_data = cursor.fetchall()
        destinations = {f"{data[0]},{data[1]}": [data[2], False] for data in destinations_data}

        ordered_addresses = calculate_delivery_order("55.716498,36.841024", "a6f0bc4e-a4ed-4839-8a91-25c403ff4b46", destinations)
        for index, coord in enumerate(ordered_addresses, start=1):
            address = destinations[coord][0]
            cursor.execute("UPDATE public.address_table SET index_number = %s WHERE num_th = %s AND address_delivery = %s", (index, num_th, address))
    connection.commit()


def get_routes_and_insert_index(day):
    connection = psycopg2.connect(**conn)
    try:
        cursor = connection.cursor()
        query_update_index = """
        WITH sorted_addresses AS (
            SELECT id, ROW_NUMBER() OVER(PARTITION BY num_th ORDER BY id) AS rn
            FROM public.address_table
            WHERE num_th IN (SELECT num_th FROM public.reestr_table WHERE user_id IS NULL and date_th = %s)
        )
        UPDATE public.address_table
        SET index_number = sorted_addresses.rn
        FROM sorted_addresses
        WHERE public.address_table.id = sorted_addresses.id;
        """
        cursor.execute(query_update_index, (day,))
        connection.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        connection.close()


def get_routes(day):
    connection = psycopg2.connect(**conn)
    try:
        cursor = connection.cursor()
        query = "SELECT num_th FROM public.reestr_table WHERE user_id IS NULL and date_th = %s"
        cursor.execute(query, (day,))

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


from decimal import Decimal


def convert_decimal(value):  # Конвертируем Decimal в float
    if isinstance(value, Decimal):
        return float(value)
    return value


def filter_addresses(session, num_th):
    addresses = (
        session.query(AddressTable)
        .filter_by(num_th=num_th)
        .all()
    )
    return addresses


def get_optimal_json():
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    drivers_shops = get_likes_shop_drivers()

    try:
        reestr_entries = session.query(ReestrTable).filter(func.date(ReestrTable.date_th) == tomorrow).all()

        route_entries = []
        overweight_addresses = []

        for entry in reestr_entries:
            num_th = entry.num_th
            addresses = filter_addresses(session, num_th)

            address_list = []
            total_weight = 0
            driver_assigned = None
            num_car = None

            for addr in addresses:
                weight = convert_decimal(addr.weight)
                total_weight += weight

                address_info = {
                    "num_route": addr.num_route,
                    "num_shop": addr.num_shop,
                    "code_tt": addr.code_tt,
                    "address_delivery": addr.address_delivery,
                    "index_number": addr.index_number,
                    "count_boxes": convert_decimal(addr.count_boxes),
                    "weight": weight
                }

                address_list.append(address_info)

                if total_weight > 3000:
                    overweight_addresses.append(address_info)

                if not driver_assigned and drivers_shops.get(addr.num_shop):
                    driver_assigned, num_car = drivers_shops.get(addr.num_shop)

            route_entries.append({
                "num_th": num_th,
                "date_th": entry.date_th.strftime('%d.%m.%Y'),
                "driver": driver_assigned,
                "num_car": num_car,
                "addresses": address_list
                })

        return route_entries, overweight_addresses

    except Exception as e:
        print("An error occurred:", e)
        return []


def get_likes_shop_drivers():
    drivers = session.query(DriversTable).all()
    like_shop = {}
    for driver in drivers:
        if driver.like_num_shop:
            for num_shop in driver.like_num_shop.split(','):
                like_shop[num_shop.strip()] = (driver.last_name, driver.num_car)

    return like_shop


# def get_optimal_json():
#     today = datetime.now().date()
#     tomorrow = today + timedelta(days=1)
#     try:
#         reestr_entries = session.query(ReestrTable).filter(func.date(ReestrTable.date_th) == tomorrow).all()
#
#         route_entries = []
#         overweight_addresses = []
#
#         for entry in reestr_entries:
#             num_th = entry.num_th
#             addresses = filter_addresses(session, num_th)
#
#             address_list = []
#             total_weight = 0
#
#             for addr in addresses:
#                 weight = convert_decimal(addr.weight)
#                 total_weight += weight
#
#                 if total_weight > 3000:
#                     overweight_addresses.append({
#                         "num_route": addr.num_route,
#                         "num_shop": addr.num_shop,
#                         "code_tt": addr.code_tt,
#                         "address_delivery": addr.address_delivery,
#                         "index_number": addr.index_number,
#                         "count_boxes": convert_decimal(addr.count_boxes),
#                         "weight": weight
#                     })
#                 else:
#                     address_list.append({
#                         "num_route": addr.num_route,
#                         "num_shop": addr.num_shop,
#                         "code_tt": addr.code_tt,
#                         "address_delivery": addr.address_delivery,
#                         "index_number": addr.index_number,
#                         "count_boxes": convert_decimal(addr.count_boxes),
#                         "weight": weight
#                     })
#
#         route_entries.append({
#             "num_th": num_th,
#             "date_th": entry.date_th.strftime('%d.%m.%Y'),
#             "addresses": address_list
#         })
#
#         final_data = route_entries + [{"overweight_addresses": overweight_addresses}]
#
#         return final_data
#
#     except Exception as e:
#         print("An error occurred:", e)
#         return []


# def get_optimal_json():
#     today = datetime.now().date()
#     tomorrow = today + timedelta(days=1)
#     try:
#         reestr_entries = session.query(ReestrTable).filter(func.date(ReestrTable.date_th) == tomorrow).all()
#         result = []
#
#         for entry in reestr_entries:
#             print("Processing num_th:", entry.num_th)
#             num_th = entry.num_th
#             addresses = filter_addresses(session, num_th)
#
#             address_list = []
#             for idx, addr in enumerate(addresses):
#                 address_list.append({
#                     "index_number": addr.index_number,
#                     "num_route": addr.num_route,
#                     "num_shop": addr.num_shop,
#                     "code_tt": addr.code_tt,
#                     "address_delivery": addr.address_delivery,
#                     "count_boxes": convert_decimal(addr.count_boxes),
#                     "weight": convert_decimal(addr.weight)
#                 })
#
#             result.append({
#                 "num_th": num_th,
#                 "date_th": entry.date_th.strftime('%d.%m.%Y'),
#                 "addresses": address_list
#             })
#
#         return result
#     except Exception as e:
#         print("An error occurred:", e)
#         return []


def fetch_addresses(session, num_th, is_null):
    condition = AddressTable.arrival_time.is_(None) if is_null else AddressTable.arrival_time.isnot(None)
    addresses = (
        session.query(AddressTable)
        .filter_by(num_th=num_th)
        .filter(condition)
        .order_by(AddressTable.arrival_time.asc())
        .all()
    )
    return addresses


def get_main_json():
    today = datetime.now().date()

    try:
        reestr_entries = session.query(ReestrTable).filter(func.date(ReestrTable.date_th) == today).all()
        result = []

        for entry in reestr_entries:
            num_th = entry.num_th
            addresses = fetch_addresses(session, num_th, False)
            addresses += fetch_addresses(session, num_th, True)

            for addr in addresses:
                if addr.num_th is not None:
                    result.append({
                        "num_route": addr.num_route,
                        "arrival_time": addr.arrival_time,
                        "departure_time": addr.departure_time
                    })

        return result

    except Exception as e:
        print(f"Error: {e}")
        return []


from datetime import datetime, time


def fetch_addresses_14(session, num_th, is_null):
    condition = AddressTable.arrival_time.is_(None) if is_null else AddressTable.arrival_time.isnot(None)
    time_condition = AddressTable.arrival_time < time(14, 0, 0)
    addresses = (
        session.query(AddressTable)
        .filter_by(num_th=num_th)
        .filter(condition)
        .filter(time_condition)
        .order_by(AddressTable.arrival_time.asc())
        .all()
    )
    return addresses


def get_main_json_14():
    today = datetime.now().date()
    try:
        reestr_entries = session.query(ReestrTable).filter(func.date(ReestrTable.date_th) == today).all()
        result = []

        for entry in reestr_entries:
            num_th = entry.num_th
            addresses = fetch_addresses_14(session, num_th, False)
            addresses += fetch_addresses_14(session, num_th, True)

            for addr in addresses:
                if addr.num_th is not None:
                    result.append(addr.num_route)
        return result

    except Exception as e:
        print(f"Error: {e}")
        return []


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


def find_and_delete_total(sheet, start_row):
    total_row_index = None
    for row_index, row in enumerate(
            sheet.iter_rows(min_row=start_row, max_col=1, max_row=sheet.max_row, values_only=True), start=start_row):
        if row[0] == "Итого":
            total_row_index = row_index
            break

    if total_row_index is not None:
        sheet.delete_rows(total_row_index, 21)  # Удалить строку "Итого" и следующие 20 строк


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

        find_and_delete_total(sheet, start_row=5)  # Найти и удалить строку "Итого" и следующие 20 строк

        ths = []
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
                                num_th, num_route, num_shop, code_tt, address_delivery,
                                 count_boxes, weight, th_id, latitude, longitude)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (addr["num_th"], addr["num_route"], addr["num_shop"], addr["code_tt"],
                                  addr["address_delivery"], addr["count_boxes"], addr["weight"],
                                  th_id, addr["latitude"], addr["longitude"]))
                            new_uuid_in_reestr = generate_and_assign_uuid(cursor, 'reestr_table', 'id')
                            new_uuid_in_address = generate_and_assign_uuid(cursor, 'address_table', 'id')
                            print("New UUID in reestr_table:", new_uuid_in_reestr)
                            print("New UUID in address_table:", new_uuid_in_address)

        logging.info("Data inserted into the database successfully")
        return True, "Data inserted into the database successfully"
    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")
        return False, f"Error inserting data into the database: {e}"
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
        print("Data inserted successfully into the 'reports' table")
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
        print("Data inserted successfully into the 'report' table")
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
        print("Data inserted successfully into the 'report' table")
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
