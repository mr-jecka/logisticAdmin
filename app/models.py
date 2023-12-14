from database import Base
from sqlalchemy import Column, Integer, String, Numeric, Boolean, Time, Date, ForeignKey, ARRAY, JSON
from sqlalchemy.dialects.postgresql import UUID


class DriversTable(Base):
    __tablename__ = 'Drivers'

    id = Column(Integer, primary_key=True)  # Локальный id в нашей БД
    user_id = Column(Integer)  # id в telegram
    last_name = Column(String(255), nullable=False)  # Фамилия водителя
    first_name = Column(String(255), nullable=False)  # Имя водителя
    patronymic = Column(String(255))  # Отчество водителя
    num_car = Column(String(255), nullable=False)  # Номер машины
    like_num_shop = Column(ARRAY(Integer))


class Routes(Base):
    __tablename__ = 'Routes'

    id = Column(UUID, primary_key=True)  # id реестра на день
    date_reestr = Column(Date)  # Дата реестра
    id_file_reestr = Column(String)  # id файла реестра, если будем хранить не просто в папке локальной,
    # сейчас просто название файла можно хранить или ничего


class ReestrTable(Base):
    __tablename__ = 'Reestr_table'

    id = Column(UUID, primary_key=True)  # id локальный транспортных накладных
    id_route = Column(UUID, ForeignKey('Routes.id'))  # Ссылается на id реестра за день
    num_th = Column(String(20))  # Номер транспортной накладной
    notification = Column(Boolean)  # получил ли водитель уведомление о маршрутах за день
    arrival_time = Column(Time)  # Запланированное время прибытия
    shipment_time = Column(Time)  # Запланированное время отправления
    departure_time = Column(Time)  # Запланированное время выезда
    arrival_time_fact = Column(Time)  # Фактическое время прибытия


class Addresses(Base):
    __tablename__ = 'Addresses'

    id = Column(UUID, primary_key=True)  # Локальный id адреса
    num_shop = Column(String(20))  # Номер магазина
    code_tt = Column(String(20))  # Код ТТ
    address_delivery = Column(String(255))  # Адрес доставки в словесном представлении
    longitude = Column(Numeric(10, 2))  # Широта адреса доставки
    latitude = Column(Numeric(10, 2))  # Долгота адреса доставки
    details = Column(JSON, nullable=True)  # Примечания адреса доставки


class AddressTable(Base):
    __tablename__ = 'address_table'

    id_th = Column(UUID, ForeignKey('Reestr_table.id'))  # Ссылается на номер транспортной накладной
    id_address = Column(UUID, ForeignKey('Addresses.id'))  # Ссылается на адрес
    id_driver = Column(UUID, ForeignKey('Drivers.id'))  # Ссылается на водителя
    num_route = Column(String)  # Номер маршрута
    count_boxes = Column(Integer)  # Количество коробок на доставку по адресу
    weight = Column(Numeric(10, 2))  # Общий вес на адрес
    index_number = Column(Integer)  # Номер по порядку точки в маршруте
