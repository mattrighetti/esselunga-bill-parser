import re
import sqlite3
import sys
from PyPDF2 import PdfFileReader

class Product:
    def __init__(self, name: str, price: str, iva: str):
        self.name = name.strip(' ')
        self.price = float(price.replace(',', '.'))
        self.iva = iva
        self.quantity = 1

    def increment(self):
        self.quantity += 1

    def __str__(self):
        return f"Person({self.name}, {self.price}, quantity: {self.quantity})"


def runMigration(path):
    db = sqlite3.connect(path)
    cur = db.cursor()
    cur.execute(
        '''
        create table if not exists products(
            identifier text,
            date datetime,
            quantity integer,
            price decimal(7, 2),
            iva char(1)
        )
        '''
    )
    cur.execute(
        '''
        create table if not exists iva(
            type char(1) primary key not null,
            percent integer not null
        )
        '''
    )
    cur.execute(
        '''
        insert into iva
        values 
        ('a', 4),
        ('c', 10),
        ('d', 22);
        '''
    )
    db.commit()
    return db


def insert(product, db, day, month, year):
    cursor = db.cursor()
    cursor.execute(
        '''
        insert into products
        values (?, ?, ?, ?)
        ''',
        product
    )
    db.commit()


def batch_insert(products, db, day, month, year):
    cursor = db.cursor()
    data = cursor.executemany(
        '''
        insert into products
        values (?, ?, ?, ?, ?)
        ''',
        [(p.name, f'{year}-{month}-{day} 19:00:00', p.quantity, p.price, p.iva) for p in products]
    )
    cursor.close()
    db.commit()


def parseBill(contents) -> [Product]:
    p_reg = r'^(.*) ([\*(a|b|c|d)])+\s+([0-9]{1,3},[0-9]{2})\s+$'

    p_dict = {}
    p_batch = []
    for c in contents:
        c = str(c).replace('•', ' ')
        if re.search(p_reg, c):
            a = re.search(p_reg, c)
            name = a.group(1).strip(' ')
            if name in p_dict:
                p_dict[name].increment()
            else:
                p = Product(name, a.group(3), a.group(2))
                p_batch.append(p)
                p_dict[name] = p

    return p_batch


def parse_date_from_filename(filename: str) -> (int, int, int):
    reg_shop_date = r'([0-9]{2})([0-9]{2})([0-9]{4})_.*.pdf$'
    if re.search(reg_shop_date, filename):
        a = re.search(reg_shop_date, filename)
        return (a.group(1), a.group(2), a.group(3))
    else:
        raise InvalidFilenameFormat


if __name__ == '__main__':
    file = sys.argv[1]
    sqlite_path = sys.argv[2]

    (day, month, year) = parse_date_from_filename(file)
    print("trying with", day, month, year)
    db = runMigration(sqlite_path)

    with open(file, 'rb') as f:
        reader = PdfFileReader(f)
        contents = reader.getPage(0).extractText().split('\n')
    p_batch = parseBill(contents)

    batch_insert(p_batch, db, day, month, year)