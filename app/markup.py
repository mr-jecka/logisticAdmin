
# from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
#
# from aiogram.filters.callback_data import CallbackData
#
# dispatcher = None
#
# def init(dp):
#     global dispatcher
#     dispatcher = dp
#
# builder = InlineKeyboardBuilder()
# btnPicture = InlineKeyboardButton(text="📝  Отчёт - адреса", callback_data="Addresses")
# btnTable = InlineKeyboardButton(text="📝  Отчёт - заказчику", callback_data="Table")
# builder.button(btnPicture)
# builder.button(btnTable)
# builder.adjust(1, 2)
# mainMenu = builder.as_markup()



from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

dispatcher = None

def init(dp):
    global dispatcher
    dispatcher = dp

mainMenu = InlineKeyboardMarkup(row_width=1)
answerMenu = InlineKeyboardMarkup()

btnScheduledArrival = InlineKeyboardButton(text="⬇️  Прибытие по регламенту", callback_data="Scheduled_arrival")
btnActualArrival = InlineKeyboardButton(text="⬇️  Прибытие фактическое", callback_data="Actual_arrival")
btnShipment = InlineKeyboardButton(text="🅿️  Погрузка", callback_data="Shipment")
btnDeparture = InlineKeyboardButton(text="⬆️  Выезд", callback_data="Departure")
btnReport = InlineKeyboardButton(text="📝  Отчёт", callback_data="Report")
btnPicture = InlineKeyboardButton(text="📝  Отчёт - адреса", callback_data="Addresses")
btnTable = InlineKeyboardButton(text="📝  Отчёт - заказчику", callback_data="Table")
btnInput = InlineKeyboardButton(text="➕  Записать в БД новую накладную", callback_data="inputBD")
btnDistribute = InlineKeyboardButton(text="🚛  Распределить маршруты", callback_data="distribute_routes")
btnDriver = InlineKeyboardButton(text="🚛  Выбрать водителя", callback_data="distribute_driver")


mainMenu.insert(btnPicture)
mainMenu.insert(btnTable)
mainMenu.insert(btnInput)
mainMenu.insert(btnDistribute)
answerMenu.insert(btnDriver)
#mainMenu.insert(btnScheduledArrival)
#mainMenu.insert(btnActualArrival)
#mainMenu.insert(btnShipment)
#mainMenu.insert(btnDeparture)
#mainMenu.insert(btnReport)


# from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
# menu = [
#     [InlineKeyboardButton(text="📝  Отчёт - адреса", callback_data="Addresses"),
#     InlineKeyboardButton(text="📝  Отчёт - заказчику", callback_data="Table")]
# ]
# menu = InlineKeyboardMarkup(inline_keyboard=menu)
# exit_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="◀️ Выйти в меню")]], resize_keyboard=True)
# iexit_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Выйти в меню", callback_data="menu")]])
#
