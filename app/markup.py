from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

dispatcher = None


def init(dp):
    global dispatcher
    dispatcher = dp


mainMenu = InlineKeyboardMarkup(row_width=1)
answerMenu = InlineKeyboardMarkup(row_width=1)

btnScheduledArrival = InlineKeyboardButton(text="⬇️  Прибытие по регламенту", callback_data="Scheduled_arrival")
btnActualArrival = InlineKeyboardButton(text="⬇️  Прибытие фактическое", callback_data="Actual_arrival")
btnShipment = InlineKeyboardButton(text="🅿️  Погрузка", callback_data="Shipment")
btnDeparture = InlineKeyboardButton(text="⬆️  Выезд", callback_data="Departure")
btnReport = InlineKeyboardButton(text="📝  Отчёт", callback_data="Report")
btnPicture = InlineKeyboardButton(text="📝  Отчёт - картинка", callback_data="Picture")
btnTable = InlineKeyboardButton(text="📝  Отчёт - таблица", callback_data="Table")
btnInput = InlineKeyboardButton(text="➕  Записать в БД новую накладную", callback_data="inputBD")
btnDistribute = InlineKeyboardButton(text="🚛  Распределить маршруты", callback_data="distribute_routes")
btnDriver = InlineKeyboardButton(text="🚛  Выбрать водителя", callback_data="distribute_driver")


#mainMenu.insert(btnScheduledArrival)
#mainMenu.insert(btnActualArrival)
#mainMenu.insert(btnShipment)
#mainMenu.insert(btnDeparture)
#mainMenu.insert(btnReport)
mainMenu.insert(btnPicture)
mainMenu.insert(btnTable)
mainMenu.insert(btnInput)
mainMenu.insert(btnDistribute)
answerMenu.insert(btnDriver)
