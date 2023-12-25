from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

dispatcher = None


def init(dp):
    global dispatcher
    dispatcher = dp


mainMenu = InlineKeyboardMarkup(row_width=1)
mainMenu2 = InlineKeyboardMarkup(row_width=1)
mainMenu3 = InlineKeyboardMarkup(row_width=1)

answerMenu = InlineKeyboardMarkup()

btnReport = InlineKeyboardButton(text="🗒  Отчёт о погрузке", callback_data="internalReport")
btnTable = InlineKeyboardButton(text="📝  Отчёт по адресам", callback_data="reportForCustomer")
btnReport14 = InlineKeyboardButton(text="1️⃣4️⃣  Отчёт до 14", callback_data="report14")
btnInput = InlineKeyboardButton(text="➕  Записать в БД новую накладную", callback_data="inputBD")
btnOptimalRoute = InlineKeyboardButton(text="🛣  Построить оптимальные маршруты", callback_data="optimalRoute")
btnOptimalReport = InlineKeyboardButton(text="🗒  Оптимальный реестр", callback_data="optimalReport")
btnOverweight = InlineKeyboardButton(text="⚖️  Подсветить перевес", callback_data="overWeight")
btnTHforDrivers = InlineKeyboardButton(text="⚖️  Присвоить водителям TH", callback_data="THforDrivers")
btnDistribute = InlineKeyboardButton(text="🚛  Распределить маршруты", callback_data="distribute_routes")
btnDistrToday = InlineKeyboardButton(text="1️⃣  На сегодня", callback_data="distr_today")
btnDistrTomorrow = InlineKeyboardButton(text="2️⃣  На завтра", callback_data="distr_tomorrow")
btnCancel = InlineKeyboardButton(text="❌  Отмена", callback_data="Cancel")
btnReportToday = InlineKeyboardButton(text="📝  Сегодняшний", callback_data="report_today")
btnReportYesterday = InlineKeyboardButton(text="📝  Вчерашний", callback_data="report_yesterday")
btnReportTomorrow = InlineKeyboardButton(text="📝  Завтрашний", callback_data="report_tomorrow")


mainMenu.insert(btnReport)
mainMenu.insert(btnTable)
mainMenu.insert(btnReport14)
mainMenu.insert(btnInput)
mainMenu.insert(btnOptimalRoute)
mainMenu.insert(btnOptimalReport)
mainMenu.insert(btnDistribute)
mainMenu.insert(btnOverweight)
mainMenu.insert(btnTHforDrivers)
mainMenu2.insert(btnDistrToday)
mainMenu2.insert(btnDistrTomorrow)
mainMenu2.insert(btnCancel)
mainMenu3.insert(btnReportToday)
mainMenu3.insert(btnReportYesterday)
mainMenu3.insert(btnReportTomorrow)
