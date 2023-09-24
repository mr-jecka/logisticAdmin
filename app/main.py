from datetime import datetime
import logging
from aiogram import Bot, Dispatcher, executor, types
import markup as nav
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.filters import Command
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from database import insert_scheduled_arrival,\
    insert_actual_arrival, insert_shipment, insert_departure, get_daily_report
from aiogram.types import CallbackQuery, InputMediaPhoto
#from media import show_picture
from logger import logging

logging.basicConfig(level=logging.INFO)
TOKEN = "6441679596:AAFtUUqRBRlCGztaIZ8uSfg4ekrz4Vi24fs"
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

class EnterForm(StatesGroup):
    waiting_for_scheduled_arrival = State()
    waiting_for_actual_arrival = State()
    waiting_for_shipment = State()
    waiting_for_departure = State()

nav.init(dp)

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    mess = f'👋  Приветствую, {message.from_user.first_name}!  Я твой личный диспетчер! '
    await bot.send_message(message.chat.id, mess, reply_markup=nav.mainMenu)
    logging.info(f"User {message.from_user.username} started the bot.")

@dp.callback_query_handler(text="Scheduled_arrival")
async def add_arrival(message: types.Message):
    await bot.send_message(message.from_user.id, "Введите время прибытия: например, 09:45")
    await EnterForm.waiting_for_scheduled_arrival.set()

@dp.message_handler(state=EnterForm.waiting_for_scheduled_arrival)
async def process_add_scheduled_arrival(message: types.Message, state: FSMContext):
    try:
        date_time_str = message.text.strip()
        date_time_obj = datetime.strptime(date_time_str, '%H:%M')
        user_id = message.from_user.id
        insert_scheduled_arrival(user_id, date_time_obj)
        await state.finish()
        await bot.send_message(message.from_user.id, "Есть прибытие!")
    except ValueError:
        await bot.send_message(message.from_user.id, "неверный формат времени")
    finally:
        await start(message)

@dp.callback_query_handler(text="Actual_arrival")
async def add_arrival(message: types.Message):
    await bot.send_message(message.from_user.id, "Введите время прибытия: например, 09:45")
    await EnterForm.waiting_for_actual_arrival.set()

@dp.message_handler(state=EnterForm.waiting_for_actual_arrival)
async def process_add_actual_arrival(message: types.Message, state: FSMContext):
    try:
        date_time_str = message.text.strip()
        date_time_obj = datetime.strptime(date_time_str, '%H:%M')
        user_id = message.from_user.id
        insert_actual_arrival(user_id, date_time_obj)
        await state.finish()
        await bot.send_message(message.from_user.id, "Есть прибытие!")
    except ValueError:
        await bot.send_message(message.from_user.id, "неверный формат времени")
    finally:
        await start(message)

@dp.callback_query_handler(text="Shipment")
async def add_shipment(message: types.Message):
    await bot.send_message(message.from_user.id, "Введите время начала погрузки: например, 10:45")
    await EnterForm.waiting_for_shipment.set()

@dp.message_handler(state=EnterForm.waiting_for_shipment)
async def process_add_shipment(message: types.Message, state: FSMContext):
    try:
        date_time_str = message.text.strip()
        date_time_obj = datetime.strptime(date_time_str, '%H:%M')
        user_id = message.from_user.id
        insert_shipment(user_id, date_time_obj)
        await state.finish()
        await bot.send_message(message.from_user.id, "Время погрузки зафиксировано!")
    except ValueError:
        await bot.send_message(message.from_user.id, "неверный формат времени")
    finally:
        await start(message)

@dp.callback_query_handler(text="Departure")
async def add_departure(message: types.Message):
    await bot.send_message(message.from_user.id, "Введите время выезда: например, 11:20")
    await EnterForm.waiting_for_departure.set()

@dp.message_handler(state=EnterForm.waiting_for_departure)
async def process_add_departure(message: types.Message, state: FSMContext):
    try:
        date_time_str = message.text.strip()
        date_time_obj = datetime.strptime(date_time_str, '%H:%M')
        user_id = message.from_user.id
        insert_departure(user_id, date_time_obj)
        await state.finish()
        await bot.send_message(message.from_user.id, "Счастливого пути!")
    except ValueError:
        await bot.send_message(message.from_user.id, "неверный формат времени")
    finally:
        await start(message)

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

from PIL import Image, ImageDraw, ImageFont

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

from tabulate import tabulate
import os
import openpyxl
from openpyxl.styles import Alignment
from aiogram.types import InputFile


@dp.callback_query_handler(text="Table")
async def show_daily_report_excel(call: CallbackQuery):
    report_date = datetime.now().date()
    daily_report = get_daily_report(report_date)
    if daily_report is None:
        daily_report = []
    # Create a temporary Excel file
    temp_file_path = "daily_report.xlsx"
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    column_names = ["Номер машины", "Фамилия", "Имя", "Отчество", "Прибытие по регламенту", "Прибытие фактическое",
                    "Погрузка", "Выезд"]
    sheet.append(column_names)

    for row in daily_report:
        sheet.append(row)

    for row in sheet.iter_rows(min_row=1, max_row=1, max_col=len(column_names)):
        for cell in row:
            cell.alignment = Alignment(horizontal='center')

    workbook.save(temp_file_path)

    # Send the Excel file as a document
    with open(temp_file_path, "rb") as excel_file:
        await bot.send_document(call.from_user.id, InputFile(excel_file))

    # Clean up the temporary file
    os.remove(temp_file_path)

    await call.answer()


@dp.callback_query_handler(text="Table_1")
async def show_daily_report(call: CallbackQuery):
    # Assuming you want to fetch the report for the current date
    report_date = datetime.now().date()
    daily_report = get_daily_report(report_date)

    # Format the report data as a table with column names
    table_data = [["Номер машины", "Фамилия", "Имя", "Отчество", "Прибытие по регламенту", "Прибытие фактическое","Погрузка", "Выезд"]]
    for row in daily_report:
        car_number, last_name, first_name, patronymic, scheduled_arrival, actual_arrival, shipment, departure = row
        table_data.append([car_number, last_name, first_name, first_name, patronymic, scheduled_arrival, actual_arrival, shipment, departure])

    # Set the alignment for each column to 'center'
    colalign = ("center",) * 4

    # Generate the table with aligned text
    table = tabulate(table_data, headers="firstrow", tablefmt="grid", colalign=colalign)

    # Send the report table to the user
    await bot.send_message(call.from_user.id, f"Отчёт за {report_date}:\n{table}")

    # Answer the callback query to remove the "Report" button's notification
    await call.answer()


# @dp.callback_query_handler(text="Picture")
# async def show_daily_report(call: CallbackQuery):
#     report_date = datetime.now().date()
#     daily_report = get_daily_report(report_date)
#
#     img = Image.new("RGB", (500, 450), color=(255, 255, 255))
#     draw = ImageDraw.Draw(img)
#
#     font = ImageFont.truetype("arial.ttf", 12)
#
#     y = 20
#     for row in daily_report:
#         user_id, arrival, shipment, departure = row
#         report_text = f"Водитель: {user_id}, Прибытие: {arrival}, Погрузка: {shipment}, Убытие: {departure}"
#         draw.text((20, y), report_text, font=font, fill=(0, 0, 0))
#         y += 20
#
#     img.save("daily_report.png")
#
#     with open("daily_report.png", "rb") as f:
#         await bot.send_photo(call.from_user.id, f)
#
#     await call.answer()



@dp.message_handler(Command("cancel"), state="*")
async def cancel_command(message: types.Message, state: FSMContext):
    await state.finish()
    await bot.send_message(message.from_user.id, "Transaction canceled.")
    await start(message)

@dp.message_handler(Command("cancel"), state="*")
async def cancel_command(message: types.Message, state: FSMContext):
    await state.finish()
    await bot.send_message(message.from_user.id, "Transaction canceled.")
    await start(message)

@dp.message_handler()
async def handle_invalid_input(message: types.Message):
    await bot.send_message(message.from_user.id, "Invalid input. Please try again.")




@dp.callback_query_handler(text="buy_usdt")
async def show_dollar_pictures(query: CallbackQuery):
    await show_picture(bot, query)

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)