from aiogram import Bot, Dispatcher, executor, types
import markup as nav
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from database import get_drivers_for_route, get_daily_report, get_routes,\
    insert_driver_for_route, get_num_route
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
TOKEN = "6441679596:AAFtUUqRBRlCGztaIZ8uSfg4ekrz4Vi24fs"
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
    mess = f'👋  Приветствую, {message.from_user.first_name}!  Я твой личный диспетчер! '
    await bot.send_message(message.chat.id, mess, reply_markup=nav.mainMenu)
    logging.info(f"User {message.from_user.username} {message.from_user.id} started the bot.")


@dp.callback_query_handler(text="inputBD")
async def address(message: types.Message):
    await bot.send_message(message.from_user.id, "Please attach the Excel file (reestr.xlsx):")
    await EnterForm.waiting_for_reestr.set()


@dp.callback_query_handler(text="distribute_routes")
async def distribute_route(query: types.CallbackQuery):
    try:
        await bot.send_message(query.from_user.id, "Распределите маршруты между водителями")
        #tomorrow_date = (datetime.now() + timedelta(days=1)).date()
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
                driver_buttons = [KeyboardButton(driver[0]) for driver in all_drivers]
                driver_keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(*driver_buttons)
                await bot.send_message(message.from_user.id, "Select a driver:", reply_markup=driver_keyboard)

                @dp.message_handler(lambda message: message.text in [driver[0] for driver in all_drivers])
                async def handle_driver_choice(driver_message: types.Message):
                    nonlocal selected_route
                    selected_driver = driver_message.text
                    await bot.send_message(
                        driver_message.from_user.id, f"You selected route: {selected_route}, driver: {selected_driver}")
                    insert_driver_for_route(selected_route, selected_driver)
            else:
                await bot.send_message(message.from_user.id, "No drivers found for this route.")
    except Exception as e:
        logging.error(f"Error in distribute_route: {str(e)}")


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

        ths = await parsed_reestr("reestr.xlsx")

        formatted_message = "Parsed transportation records:\n\n"
        for idx, th in enumerate(ths, start=1):
            formatted_message += f"Transportation {idx}:\n"
            formatted_message += f"Number: {th['num_th']}\n"
            formatted_message += f"Date: {th['date_th']}\n"
            formatted_message += f"Total Count Boxes: {th['total_count_boxes']}\n"
            formatted_message += f"Total Weight: {th['total_weight']}\n\n"

        await message.answer("Эксель файл сохранён")
        await message.answer(formatted_message)
        database.insert_excel_to_db("reestr.xlsx", database.conn)
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


@dp.callback_query_handler(text="Report")
async def show_daily_report(call: CallbackQuery):
    report_date = datetime.now().date()
    daily_report = get_daily_report(report_date)
    report_message = "Daily Report for {}:\n".format(report_date)
    for row in daily_report:
        car_number, last_name, scheduled_arrival, actual_arrival, shipment, departure = row
        report_message += f"Номер машины: {car_number}, Фамилия: {last_name}, Прибытие по регламенту: {scheduled_arrival}, Прибытие фактическое: {actual_arrival}, Shipment: {shipment}, Departure: {departure}\n"
    await bot.send_message(call.from_user.id, report_message)
    await call.answer()


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


@dp.callback_query_handler(text="Table")
async def show_main_report(call: CallbackQuery):
    logging.info(f"Start show_main_report")
    selected_num_route = get_num_route()
    if selected_num_route:
        logging.info(f"Starting show_daily_report_excel for route {selected_num_route}")
        try:
            wb = openpyxl.load_workbook('reestr.xlsx')
            logging.info(f"'reestr.xlsx' file found and opened successfully.")
        except Exception as e:
            logging.error(f"Error opening 'reestr.xlsx'. Exception: {e}")
            return
        sheet = wb.active
        rows_to_delete = []

        for row in sheet.iter_rows(min_col=5, max_col=5):
            logging.info(f"Row {row[0].row}: num_route value - {row[0].value}")
            if row[0].value in selected_num_route:
                rows_to_delete.append(row[0].row)

        for row_index in reversed(rows_to_delete):
            sheet.delete_rows(row_index, 1)
        wb.save('reestr.xlsx')
        logging.info(f"Updated Excel file by removing rows with driver {selected_num_route}")


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
# async def show_daily_report_excel(call: CallbackQuery):
#     report_date = datetime.now().date()
#     daily_report = get_daily_report(report_date)
#     if daily_report is None:
#         daily_report = []
#     temp_file_path = "daily_report.xlsx"
#     workbook = openpyxl.Workbook()
#     sheet = workbook.active
#     column_names = ["Номер машины", "Фамилия", "Имя", "Отчество", "Прибытие по регламенту", "Прибытие фактическое",
#                     "Погрузка", "Выезд"]
#     sheet.append(column_names)
#     for row in daily_report:
#         sheet.append(row)
#     for row in sheet.iter_rows(min_row=1, max_row=1, max_col=len(column_names)):
#         for cell in row:
#             cell.alignment = Alignment(horizontal='center')
#     workbook.save(temp_file_path)
#     with open(temp_file_path, "rb") as excel_file:
#         await bot.send_document(call.from_user.id, InputFile(excel_file))
#     os.remove(temp_file_path)
#     await call.answer()


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
