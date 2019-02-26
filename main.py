import csv
import re

from pymongo import MongoClient
from datetime import datetime


client = MongoClient()
db = client.concerts
concerts_collection = db.artist


def read_data(csv_file, db):
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        ticket_list = []
        for line in reader:
            ticket_list.append({
                'Исполнитель': line['Исполнитель'],
                'Цена': int(line["Цена"]),
                'Место': line["Место"],
                'Дата': datetime.strptime(line["Дата"] + '.2019', format('%d.%m.%Y'))
            })
    db.concerts_collection.insert_many(ticket_list)


def find_cheapest(db):
    """
    Найти самые дешевые билеты
    Документация: https://docs.mongodb.com/manual/reference/operator/aggregation/sort/
    """
    cheap_tiket = db.concerts_collection.aggregate([{'$sort': {'Цена': 1}}])
    return list(cheap_tiket)



def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и выведите их по возрастанию цены
    """
    regex = re.compile('(' + name + ')')
    result_search = db.concerts_collection.find({'Исполнитель': regex})
    result = sorted(result_search, key=lambda x: x['Цена'])
    return result


if __name__ == '__main__':
    read_data('artists.csv', db)
    print(find_cheapest(db))
    print(find_by_name('а', db))



