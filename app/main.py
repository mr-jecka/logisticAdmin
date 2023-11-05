from openpyxl import Workbook
from decimal import Decimal
import json
from aiogram import Bot, Dispatcher, executor, types
import markup as nav
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from database import get_drivers_for_route, get_internal_report, get_routes,\
    insert_driver_for_route, get_main_json, get_info_for_report
from aiogram.types import CallbackQuery, KeyboardButton, ReplyKeyboardMarkup
from aiogram.dispatcher import FSMContext
from logger import logging
from aiogram import types
import os
from openpyxl.styles import Alignment
from aiogram.types import InputFile
from PIL import Image, ImageDraw, ImageFont
import openpyxl
from datetime import datetime, timedelta
import database

logging.basicConfig(level=logging.INFO)
TOKEN = "6441679596:AAEYabzPiA4dg0GOlBISJk0BhAjqn1OPjF0"
#TOKEN = "6489569901:AAHBPmIvgYsxj_M_p6x9FnG_RThYEBthcRc" #@MoveTrafficBot

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


class EnterForm(StatesGroup):
    waiting_for_scheduled_arrival = State()
    waiting_for_actual_arrival = State()
    waiting_for_shipment = State()
    waiting_for_departure = State()
    waiting_for_reestr = State()
    waiting_for_routes = State()
    waiting_for_driver = State()


nav.init(dp)


@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    mess = f'👋  Приветствую, {message.from_user.first_name}!  Я помощник по логистике! '
    await bot.send_message(message.chat.id, mess, reply_markup=nav.mainMenu)
    logging.info(f"User {message.from_user.username} {message.from_user.id} started the bot.")


@dp.callback_query_handler(text="inputBD")
async def address(message: types.Message):
    await bot.send_message(message.from_user.id, "Предоставьте мне файл reestr.xlsx:")
    await EnterForm.waiting_for_reestr.set()


def update_excel_with_route_driver_car(selected_route, selected_driver_name, selected_driver_car, selected_time):
    wb = openpyxl.load_workbook('reestr.xlsx')
    sheet = wb.active

    for row in range(4, sheet.max_row + 1):
        if sheet.cell(row=row, column=3).value == selected_route:
            sheet.cell(row=row, column=12).value = selected_driver_name
            sheet.cell(row=row, column=13).value = selected_time
            sheet.cell(row=row, column=14).value = selected_driver_car
            break

    wb.save('reestr.xlsx')


@dp.callback_query_handler(text="distribute_routes")
async def distribute_route(query: types.CallbackQuery):
    try:
        await bot.send_message(query.from_user.id, "Распределите маршруты между водителями")
        tomorrow_routes = get_routes()
        if tomorrow_routes:
            route_buttons = [KeyboardButton(route[0]) for route in tomorrow_routes]
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(*route_buttons)
            await bot.send_message(query.from_user.id, "Выберете маршрут:", reply_markup=keyboard)
        else:
            await bot.send_message(query.from_user.id, "Не найдены маршруты на завтра")

        @dp.message_handler(lambda message: message.text in [route[0] for route in tomorrow_routes])
        async def handle_route_choice(message: types.Message):
            selected_route = message.text
            await bot.send_message(message.from_user.id, "You selected route: " + selected_route)
            all_drivers = get_drivers_for_route()
            if all_drivers:
                all_drivers_dict = {driver[0]: driver[1] for driver in all_drivers}
                driver_buttons = [KeyboardButton(driver[0]) for driver in all_drivers]

                driver_keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(*driver_buttons)
                await bot.send_message(message.from_user.id, "Select a driver:", reply_markup=driver_keyboard)

                @dp.message_handler(lambda message: message.text in [driver[0] for driver in all_drivers])
                async def handle_driver_choice(driver_message: types.Message):
                    nonlocal selected_route
                    selected_driver_name = driver_message.text
                    selected_driver_car = all_drivers_dict[selected_driver_name]
                    await bot.send_message(
                        driver_message.from_user.id,
                        f"You selected route: {selected_route}, driver: {selected_driver_name}")

                    loading_times = ["3:00", "3:30", "4:00", "4:30", "5:00", "5:30", "6:00", "6:30", "7:00"]
                    time_buttons = [KeyboardButton(time) for time in loading_times]
                    time_keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(*time_buttons)
                    await bot.send_message(driver_message.from_user.id, "Какое время подачи на погрузку?",
                                           reply_markup=time_keyboard)

                    @dp.message_handler(lambda message: message.text in loading_times)
                    async def handle_loading_time_choice(time_message: types.Message):
                        selected_time = time_message.text
                        insert_driver_for_route(
                            selected_route, selected_driver_name, selected_driver_car, selected_time)
                        update_excel_with_route_driver_car(
                            selected_route, selected_driver_name, selected_driver_car, selected_time)
                        await bot.send_message(
                            time_message.from_user.id,
                            f"You selected route: {selected_route}, driver: {selected_driver_name}, time: {selected_time}")
            else:
                await bot.send_message(message.from_user.id, "No drivers found for this route.")
    except Exception as e:
        await bot.send_message(query.from_user.id, f"Произошла ошибка: {e}")


@dp.message_handler(lambda message: message.text.isdigit())
async def distribute_driver(message: types.Message, state: FSMContext):
    try:
        logging.info(f"Start distribute_route")
        route_number = message.text
        await state.update_data(selected_route=route_number)
        drivers = get_available_drivers()
        if drivers:
            driver_buttons = [KeyboardButton(driver[0]) for driver in drivers]
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(*driver_buttons)
            await message.answer("Select a driver:", reply_markup=keyboard)
        else:
            await message.answer("No available drivers.")
    except Exception as e:
        logging.error(f"Error in distribute_route: {str(e)}")


@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=EnterForm.waiting_for_reestr)
async def input_reestr(message: types.Message, state: FSMContext):
    logging.info(f"Start input_reestr in BD")
    if message.document.file_name.endswith('.xlsx'):
        file_id = message.document.file_id
        file_info = await bot.get_file(file_id)
        file_path = file_info.file_path
        downloaded_file = await bot.download_file(file_path)
        with open("reestr.xlsx", "wb") as file:
            file.write(downloaded_file.read())

        await message.answer("Файл принят, начинаю запись в БД")

        ths = await parsed_reestr("reestr.xlsx")

        formatted_message = "Parsed transportation records:\n\n"
        for idx, th in enumerate(ths, start=1):
            formatted_message += f"Transportation {idx}:\n"
            formatted_message += f"Number: {th['num_th']}\n"
            formatted_message += f"Date: {th['date_th']}\n"
            formatted_message += f"Total Count Boxes: {th['total_count_boxes']}\n"
            formatted_message += f"Total Weight: {th['total_weight']}\n\n"

        status, response_message = database.insert_excel_to_db("reestr.xlsx", database.conn)
        await message.answer(response_message)
        await state.finish()
    else:
        await message.answer("Предоставьте excel-файл (reestr.xlsx).")
    await state.finish()


async def parsed_reestr(excel_file_path):
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
            if isinstance(row[3], datetime):
                th["date_th"] = row[3].strftime('%Y-%m-%d')
            else:
                th["date_th"] = datetime.strptime(row[3], '%d.%m.%Y').strftime('%Y-%m-%d')
            th["total_count_boxes"] = int(row[9])
            th["total_weight"] = float(row[10])
            th["addresses"] = []
        else:
            addr = {}
            addr["num_route"] = row[4]
            addr["num_shop"] = row[6]
            addr["code_tt"] = row[7]
            addr["address_delivery"] = row[8]
            addr["count_boxes"] = int(row[9])
            addr["weight"] = float(row[10])
            th["addresses"].append(addr)
    ths.append(th)
    return ths


# @dp.callback_query_handler(text="Report")
# async def show_daily_report(call: CallbackQuery):
#     report_date = datetime.now().date()
#     daily_report = get_daily_report(report_date)
#     report_message = "Daily Report for {}:\n".format(report_date)
#     for row in daily_report:
#         car_number, last_name, scheduled_arrival, actual_arrival, shipment, departure = row
#         report_message += f"Номер машины: {car_number}, Фамилия: {last_name}, Прибытие по регламенту: {scheduled_arrival}, Прибытие фактическое: {actual_arrival}, Shipment: {shipment}, Departure: {departure}\n"
#     await bot.send_message(call.from_user.id, report_message)
#     await call.answer()


@dp.callback_query_handler(text="Picture")
async def show_daily_report(call: CallbackQuery):
    report_date = datetime.now().date()
    daily_report = get_daily_report(report_date)

    img = Image.new("RGB", (704, 350), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)

    font = ImageFont.truetype("arial.ttf", 12)

    column_names = ["Номер машины", "Фамилия", "Имя", "Отчество", "Прибытие по регламенту", "Прибытие фактическое", "Погрузка", "Выезд"]
    header_text = "  ".join(column_names)
    draw.text((20, 20), header_text, font=font, fill=(0, 0, 0))

    y = 40
    for row in daily_report:
        car_number, last_name, first_name, patronymic, scheduled_arrival, actual_arrival, shipment, departure = row
        report_text = f"{car_number}  {last_name}  {first_name}  {patronymic} {scheduled_arrival} {actual_arrival} {shipment} {departure}"
        draw.text((20, y), report_text, font=font, fill=(0, 0, 0))
        y += 20

    img.save("daily_report.png")

    with open("daily_report.png", "rb") as f:
        await bot.send_photo(call.from_user.id, f)

    await call.answer()


def date_handler(obj):
    if hasattr(obj, 'isoformat'):  # для объектов date
        return obj.isoformat()
    elif isinstance(obj, Decimal):  # для объектов decimal.Decimal
        return float(obj)
    else:
        raise TypeError("Object of type %s with value of %s is not JSON serializable" % (type(obj), repr(obj)))


import openpyxl

import openpyxl


# def modify_reestr(json_data):
#     wb = openpyxl.load_workbook('reestr.xlsx')
#     sheet = wb.active
#     current_th_row = 4
#     route_to_row = {}  # словарь для сохранения номеров строк по num_route
#
#     for th in json_data:
#         routes_in_sheet = []
#         for row in sheet.iter_rows(min_row=current_th_row, values_only=True):
#             if row[2] == th['num_th']:
#                 routes_in_sheet.append(row)
#                 # сохраняем номер строки для данного num_route
#                 route_to_row[row[4]] = current_th_row  # row[4] это num_route
#                 current_th_row += 1
#             else:
#                 break
#
#         sorted_routes = sorted(routes_in_sheet, key=lambda x: next((addr['order'] for addr in th['addreses'] if addr['num_route'] == x[4]), 1000))
#         for idx, route in enumerate(sorted_routes):
#             row_to_update = current_th_row + idx
#             for col_num, value in enumerate(route, 1):
#                 cell = sheet.cell(row=row_to_update, column=col_num)
#                 if isinstance(cell, openpyxl.cell.MergedCell):
#                     sheet.unmerge_cells(start_row=cell.row, start_column=cell.column, end_row=cell.row, end_column=cell.column)
#                 cell.value = value
#
#     wb.save('reestr_modified.xlsx')
#     return route_to_row


# def modify_reestr(json_data):
#     wb = openpyxl.load_workbook('reestr.xlsx')
#     sheet = wb.active
#     current_th_row = 4
#
#     for th in json_data:
#         routes_in_sheet = []
#         for row in sheet.iter_rows(min_row=current_th_row, values_only=True):
#             if row[2] == th['num_th']:
#                 routes_in_sheet.append(row)
#             else:
#                 break
#         sorted_routes = sorted(routes_in_sheet, key=lambda x: next((addr['order'] for addr in th['addreses'] if addr['num_route'] == x[4]), 1000))
#         for idx, route in enumerate(sorted_routes):
#             row_to_update = current_th_row + idx
#             for col_num, value in enumerate(route, 1):
#                 sheet.cell(row=row_to_update, column=col_num).value = value
#         current_th_row += len(routes_in_sheet)
#     wb.save('reestr_modified.xlsx')


# def modify_reestr(json_data):
#     wb = openpyxl.load_workbook('reestr.xlsx')
#     sheet = wb.active
#     current_th_row = 4
#     for row in sheet.iter_rows(min_row=current_th_row, values_only=True):
#         if row[2]:
#             th = next((item for item in json_data if item['num_th'] == row[2]), None)
#             if th is not None and 'addreses' in th:
#                 sorted_addresses = sorted(th['addreses'], key=lambda x: x['num_route'])
#                 for addr in sorted_addresses:
#                     insertion_row = current_th_row + 1
#                     while sheet.cell(row=insertion_row, column=5).value and addr['num_route'] > sheet.cell(
#                             row=insertion_row, column=5).value:
#                         insertion_row += 1
#                     if sheet.cell(row=insertion_row, column=5).value:
#                         sheet.insert_rows(insertion_row)
#                     sheet.cell(row=insertion_row, column=5).value = addr['num_route']
#                     sheet.cell(row=insertion_row, column=7).value = addr['num_shop']
#                     sheet.cell(row=insertion_row, column=8).value = addr['code_tt']
#                     sheet.cell(row=insertion_row, column=9).value = addr['address_delivery']
#                     sheet.cell(row=insertion_row, column=10).value = addr['count_boxes']
#                     sheet.cell(row=insertion_row, column=11).value = addr['weight']
#                     current_th_row = insertion_row + 1
#             else:
#                 current_th_row += 1
#     wb.save('reestr_modified.xlsx')

# def reorder_rows_based_on_json(json_data):
#     wb = openpyxl.load_workbook('reestr.xlsx')
#     sheet = wb.active
#
#     current_order = modify_reestr(json_data)
#
#     for th_data in json_data:
#         th_row = current_order[th_data['num_th']]['th_row']
#
#         routes_data = sorted(th_data['addreses'], key=lambda x: x['order'])
#         for i, route in enumerate(routes_data):
#             current_row_num = current_order[th_data['num_th']]['routes'][route['num_route']]
#             new_row_num = th_row + i + 1
#
#             if current_row_num != new_row_num:
#                 sheet.insert_rows(new_row_num)
#                 for col_num, cell_value in enumerate(sheet[current_row_num], 1):
#                     sheet.cell(row=new_row_num, column=col_num, value=cell_value.value)
#                 sheet.delete_rows(current_row_num if current_row_num < new_row_num else current_row_num + 1)
#     wb.save('reestr.xlsx')
#
#
def modify_reestr(json_data):
    wb = openpyxl.load_workbook('reestr.xlsx')
    sheet = wb.active
    current_th_row = 4
    result = {}
    for row in sheet.iter_rows(min_row=current_th_row, values_only=True):
        if row[2]:
            th = next((item for item in json_data if item['num_th'] == row[2]), None)
            if th is not None and 'addreses' in th:
                result[th['num_th']] = {
                    "th_row": current_th_row,
                    "routes": {}
                }
                current_row = current_th_row + 1
                while current_row < sheet.max_row:
                    current_addr_row = sheet.cell(row=current_row,
                                                  column=5).value
                    addr = next((item for item in th['addreses'] if item['num_route'] == current_addr_row), None)
                    if addr:
                        result[th['num_th']]["routes"][addr['num_route']] = current_row
                        current_row += 1
                    else:
                        break
                current_th_row = current_row
            else:
                current_th_row += 1

    return result

# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     data = get_main_json()
#
#     with open('json.txt', 'w', encoding='utf-8') as f:
#         json.dump(data, f, ensure_ascii=False, indent=4, default=date_handler)
#
#     current_mapping = modify_reestr(data)
#
#     reorder_rows_based_on_json(data)
#
#     reordered_mapping = modify_reestr(data)
#
#     message_text = "До переупорядочивания:\n"
#     for th, th_data in current_mapping.items():
#         message_text += f"Номер строчки для num_th = {th}: {th_data['th_row']}\n"
#         message_text += f"Номера строк num_route соответствующие num_th = {th}:\n"
#         for route, route_row in th_data["routes"].items():
#             message_text += f"{route}: {route_row}\n"
#         message_text += "\n"
#
#     message_text += "\nПосле переупорядочивания:\n"
#     for th, th_data in reordered_mapping.items():
#         message_text += f"Номер строчки для num_th = {th}: {th_data['th_row']}\n"
#         message_text += f"Номера строк num_route соответствующие num_th = {th}:\n"
#         for route, route_row in th_data["routes"].items():
#             message_text += f"{route}: {route_row}\n"
#         message_text += "\n"
#
#     await call.message.answer(message_text.strip())
#     await call.message.answer_document(InputFile('json.txt'))

# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     data = get_main_json()
#
#     with open('json.txt', 'w', encoding='utf-8') as f:
#         json.dump(data, f, ensure_ascii=False, indent=4, default=date_handler)
#
#     mapping = modify_reestr(data)
#
#     # Переупорядочивание строк в reestr.xlsx на основе данных JSON
#     reorder_rows_based_on_json(data)
#
#     message_text = ""
#     for th, th_data in mapping.items():
#         message_text += f"Номер строчки для num_th = {th}: {th_data['th_row']}\n"
#         message_text += f"Номера строк num_route соответствующие num_th = {th}:\n"
#         for route, route_row in th_data["routes"].items():
#             message_text += f"{route}: {route_row}\n"
#         message_text += "\n"
#
#     await call.message.answer(message_text.strip())
#     await call.message.answer_document(InputFile('json.txt'))




# Пример использования:
# update_excel_with_route_driver_car('reestr_modified.xlsx', '0000-013333', 'Driver Name', 'Car Model')


from collections import Counter
import logging
import copy


def fix_duplicate_rows(sheet, json_data, current_order):
    logging.info("Starting fix_duplicate_rows...")
    for th_data in json_data:
        routes_data = sorted(th_data['addreses'], key=lambda x: x['order'])
        route_numbers = [route['num_route'] for route in routes_data]

        # Проверяем наличие дублирующихся строк
        duplicates = [num for num, count in Counter(route_numbers).items() if count > 1]

        logging.info(f"Found duplicates: {duplicates} for th_data: {th_data['num_th']}")

        if duplicates:
            for duplicate in duplicates:
                # Заменяем дублирующую строку на отсутствующую строку
                missing_route = set(route_numbers) - set(current_order[th_data['num_th']]['routes'].keys())

                logging.info(f"Missing routes for duplicate {duplicate}: {missing_route}")

                if missing_route:
                    missing_route = missing_route.pop()
                    current_row_num = current_order[th_data['num_th']]['routes'][duplicate]

                    logging.info(
                        f"Replacing duplicate {duplicate} at row {current_row_num} with missing route {missing_route}")

                    for col_num, cell_value in enumerate(sheet[current_row_num], 1):
                        sheet.cell(row=current_row_num, column=col_num, value=cell_value.value)

                    sheet.cell(row=current_row_num, column=5, value=missing_route)
                    current_order[th_data['num_th']]['routes'][missing_route] = current_row_num


def reorder_addresses_in_excel(json_data, file_name='reestr.xlsx'):
    logging.info("Starting reorder_addresses_in_excel...")
    wb = openpyxl.load_workbook(file_name)
    sheet = wb.active

    current_order = get_current_order_from_excel(sheet, json_data)
    original_order = copy.deepcopy(current_order)
    logging.info(f"Current order from Excel: {current_order}")

    fix_duplicate_rows(sheet, json_data, current_order)

    buffered_rows = {}  # Temporary buffer to hold row data

    for th_data in json_data:
        th_row = current_order[th_data['num_th']]['th_row']
        routes_data = sorted(th_data['addreses'], key=lambda x: x['order'])

        logging.info(f"Starting reordering for th_data: {th_data['num_th']}")

        for i, route in enumerate(routes_data):
            new_row_num = th_row + i + 1

            logging.info(f"Intended placement of route {route['num_route']} in row {new_row_num}")

            current_row_num = original_order[th_data['num_th']]['routes'][route['num_route']]

            if current_row_num != new_row_num:
                logging.info(f"Reordering row from {current_row_num} to {new_row_num} for route: {route['num_route']}")

                buffered_rows[new_row_num] = [(cell.value) for cell in sheet[current_row_num]]

                original_order[th_data['num_th']]['routes'][route['num_route']] = new_row_num

    for new_row_num, row_data in buffered_rows.items():
        for col_num, cell_value in enumerate(row_data, 1):
            sheet.cell(row=new_row_num, column=col_num, value=cell_value)
        logging.info(f"Updated row {new_row_num} with route {row_data[4]}")

    wb.save(file_name)


def get_current_order_from_excel(sheet, json_data):
    logging.info("Starting get_current_order_from_excel...")
    current_th_row = 4
    result = {}
    for row in sheet.iter_rows(min_row=current_th_row, values_only=True):
        if row[2]:
            th = next((item for item in json_data if item['num_th'] == row[2]), None)
            if th is not None and 'addreses' in th:
                result[th['num_th']] = {
                    "th_row": current_th_row,
                    "routes": {}
                }
                current_row = current_th_row + 1
                while current_row < sheet.max_row + 1:
                    current_addr_row = sheet.cell(row=current_row, column=5).value
                    addr = next((item for item in th['addreses'] if item['num_route'] == current_addr_row), None)
                    if addr:
                        result[th['num_th']]["routes"][addr['num_route']] = current_row
                        current_row += 1
                    else:
                        break
                current_th_row = current_row
            else:
                current_th_row += 1
    return result


@dp.callback_query_handler(text="Table")
async def show_main_report(call: CallbackQuery):
    logging.info(f"Start show_main_report")
    data = get_main_json()

    with open('json.txt', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, default=date_handler)

    modify_reestr(data)
    reorder_addresses_in_excel(data)

    # Отправка файла пользователю в чат
    temp_file_path = 'reestr.xlsx'
    with open(temp_file_path, "rb") as excel_file:
        await bot.send_document(call.from_user.id, InputFile(excel_file, filename="reestr.xlsx"))


# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     data = get_main_json()
#     with open('json.txt', 'w', encoding='utf-8') as f:
#         json.dump(data, f, ensure_ascii=False, indent=4, default=date_handler)
#     # route_to_row = modify_reestr(data)
#     modify_reestr(data)
#     reorder_addresses_in_excel(data)
#     # message_text = "Номера строк для num_route:\n"
#     # for route, row in route_to_row.items():
#     #     message_text += f"{route}: {row}\n"
#     # await call.message.answer(message_text)
#     await call.message.answer_document(InputFile('json.txt'))


# def reorder_addresses_in_excel(json_data, file_name='reestr.xlsx'):
#     logging.info("Starting reorder_addresses_in_excel...")
#     wb = openpyxl.load_workbook(file_name)
#     sheet = wb.active
#
#     current_order = get_current_order_from_excel(sheet, json_data)
#     logging.info(f"Current order from Excel: {current_order}")
#
#     fix_duplicate_rows(sheet, json_data, current_order)
#
#     for th_data in json_data:
#         th_row = current_order[th_data['num_th']]['th_row']
#         routes_data = sorted(th_data['addreses'], key=lambda x: x['order'])
#
#         for i, route in enumerate(routes_data):
#             new_row_num = th_row + i + 1
#             current_row_num = current_order[th_data['num_th']]['routes'][route['num_route']]
#
#             if current_row_num != new_row_num:
#                 logging.info(f"Reordering row from {current_row_num} to {new_row_num} for route: {route['num_route']}")
#
#                 for col_num, cell_value in enumerate(sheet[current_row_num], 1):
#                     sheet.cell(row=new_row_num, column=col_num, value=cell_value.value)
#
#     wb.save(file_name)

# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     data = get_main_json()
#
#     with open('json.txt', 'w', encoding='utf-8') as f:
#         json.dump(data, f, ensure_ascii=False, indent=4, default=date_handler)
#
#     modify_reestr(data)
#
#     await call.message.answer_document(InputFile('json.txt'))


# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     selected_addresses = get_main_json()
#     if not selected_addresses:
#         logging.info("No addresses to remove found.")
#         return
#     try:
#         wb = openpyxl.load_workbook('reestr.xlsx')
#         sheet = wb.active
#         rows_to_delete = []
#         for cell in sheet['E']:
#             if cell.row == 1:
#                 continue
#             if cell.value in selected_addresses:
#                 rows_to_delete.append(cell.row)
#         for row_index in reversed(rows_to_delete):
#             sheet.delete_rows(row_index, 1)
#         wb.save('reestr.xlsx')
#         logging.info(f"Updated Excel file by removing rows with addresses {selected_addresses}")
#     except Exception as e:
#         logging.error(f"Error processing 'reestr.xlsx'. Exception: {e}")


# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     selected_num_route = get_null_address_delivery()
#     if selected_num_route:
#         logging.info(f"Starting show_daily_report_excel for route {selected_num_route}")
#         try:
#             wb = openpyxl.load_workbook('reestr.xlsx')
#             logging.info(f"'reestr.xlsx' file found and opened successfully.")
#         except Exception as e:
#             logging.error(f"Error opening 'reestr.xlsx'. Exception: {e}")
#             return
#         sheet = wb.active
#         rows_to_delete = []
#
#         for row in sheet.iter_rows(min_col=5, max_col=5):
#             logging.info(f"Row {row[0].row}: num_route value - {row[0].value}")
#             if row[0].value in selected_num_route:
#                 rows_to_delete.append(row[0].row)
#
#         for row_index in reversed(rows_to_delete):
#             sheet.delete_rows(row_index, 1)
#         wb.save('reestr.xlsx')
#         logging.info(f"Updated Excel file by removing rows with driver {selected_num_route}")

# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     selected_num_route = get_num_route()
#     if selected_num_route:
#         logging.info(f"Starting show_daily_report_excel for route {selected_num_route}")
#         wb = openpyxl.load_workbook('reestr.xlsx')
#         sheet = wb.active
#         for row in reversed(sheet.iter_rows(min_col=5, max_col=5)):
#             if not row[0].value or row[0].value == 'NULL':
#                 sheet.delete_rows(row[0].row, 1)
#         wb.save('reestr.xlsx')
#         logging.info(f"Updated Excel file with driver {selected_num_route}")

# @dp.callback_query_handler(text="Table")
# async def show_main_report(call: CallbackQuery):
#     logging.info(f"Start show_main_report")
#     selected_num_route = get_num_route()
#     if selected_num_route:
#         logging.info(f"Starting show_daily_report_excel for route {selected_num_route}")
#         wb = openpyxl.load_workbook('reestr.xlsx')
#         sheet = wb.active
#         for row in reversed(sheet.iter_rows(min_col=5, max_col=5)):
#             logging.info(f"Row {row[0].row}: num_route value - {row[0].value}")
#             if row[0].value in selected_num_route:
#                 sheet.delete_rows(row[0].row, 1)
#         wb.save('reestr.xlsx')
#         logging.info(f"Updated Excel file by removing rows with driver {selected_num_route}")


@dp.callback_query_handler(text="Report")
async def show_daily_report_excel(call: CallbackQuery):
    temp_file_path = "report_internal.xlsx"

    workbook = Workbook()
    sheet = workbook.active

    current_date = datetime.now().date()

    header_data = [
        ('Погрузка и доставка продукции по ТТ',),
        (f'Дата: {current_date}',),
        ('Номер авто', 'Водитель', 'Время\n прибытия', 'Время\nпогрузки', 'Время\nвыезда',
         'Время прибытия\nпо регламенту', 'Опоздание', 'Простой', 'Время\nпогрузки', 'Цель\nвъезда/выезда')
    ]

    info_for_report = get_info_for_report()

    report_data = [
        (row[2],  # num_car
         row[1],  # lastname
         row[3].strftime('%H:%M') if row[3] else '',  # arrival_time_fact
         row[4].strftime('%H:%M') if row[4] else '',  # shipment_time
         row[5].strftime('%H:%M') if row[5] else '',  # departure_time
         row[6].strftime('%H:%M') if row[6] else '',  # arrival_time
         str((datetime.combine(datetime.min, row[3]) - datetime.combine(
                 datetime.min, row[6])).seconds // 60) + ' мин' if row[3] and row[6] and row[3] > row[6] else '',  # Опоздание
         str((datetime.combine(datetime.min, row[4]) - datetime.combine(
             datetime.min, row[3])).seconds // 60) + ' мин' if row[3] and row[4] else '',  # Простой
         str((datetime.combine(datetime.min, row[5]) - datetime.combine(
             datetime.min, row[4])).seconds // 60) + ' мин' if row[4] and row[5] else '',  # Время погрузки
         'погрузка'  # Цель въезда/выезда
         ) for row in info_for_report
    ]

    data = header_data + report_data

    for row in data:
        sheet.append(row)
    for row_idx in [1, 2]:
        sheet.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=10)
        cell = sheet.cell(row=row_idx, column=1)
        cell.alignment = Alignment(horizontal='center')
    for cell in sheet[3]:
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

    sheet.row_dimensions[3].height = 55

    for col_letter in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']:
        sheet.column_dimensions[col_letter].width = 18

    workbook.save(temp_file_path)
    with open(temp_file_path, "rb") as excel_file:
        await bot.send_document(call.from_user.id, InputFile(excel_file, filename="report_internal.xlsx"))
    os.remove(temp_file_path)
    await call.answer()


async def on_startup(dispatcher):
    database.tunnel.start()


async def on_shutdown(dispatcher):
    database.tunnel.stop()

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
