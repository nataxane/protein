import datetime
import random
import string

import cx_Oracle


PRODUCERS_COUNT = 100000
CUSTOMERS_COUNT = 100000
CITIES_COUNT = 1000

BATCH_SIZE = 10000


def connect():
    ip = '23.96.21.245'
    port = 1521
    SID = 'cdb1'
    user = 'SYS'
    passwd = 'OraPasswd1'

    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    conn = cx_Oracle.connect(user, passwd, dsn_tns, mode=cx_Oracle.SYSDBA)

    return conn


def generate_random_string(min_length, max_length):
    string_length = random.randint(min_length, max_length)

    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(string_length))


def populate_cities(conn):
    print "{}: start populating 'cities'".format(datetime.datetime.now())

    countries = ["Switzerland", "France", "Netherlands", "Brazil", "Germany", "Japan"]
    rows = []

    for i in range(CITIES_COUNT):
        city = generate_random_string(5, 10)

        if i % 100 == 0:
            country = "Russia"
        else:
            country = random.choice(countries)

        rows.append((i, city, country))

    cur = conn.cursor()
    cur.executemany("INSERT INTO cities(ct_id, ct_name, country) VALUES (:1, :2, :3)", rows)
    conn.commit()

    print "{}: added {} rows to 'cities'".format(datetime.datetime.now(), CITIES_COUNT)

    cur.close()


def populate_producers(conn):
    print "{}: start populating 'producers'".format(datetime.datetime.now())

    items = []
    cities_array_type = conn.gettype("CITIES_ARRAY")
    cur = conn.cursor()

    for i in range(PRODUCERS_COUNT/BATCH_SIZE):
        rows = []
        for j in range(BATCH_SIZE):
            item = generate_random_string(5, 15)
            name = generate_random_string(5, 7)

            cities_length = random.randint(1, 5)
            cities_array = cities_array_type.newobject()

            for _ in range(cities_length):
                cities_array.append(random.randint(0, CITIES_COUNT - 1))

            items.append(item)
            rows.append((name, item, cities_array))

        cur.executemany("INSERT INTO producers(p_name, item, cities) VALUES (:1, :2, :3)", rows)
        conn.commit()
        print "{}: added {} rows to 'producers'".format(datetime.datetime.now(), BATCH_SIZE * (i + 1))

    cur.close()

    return items


def populate_customers(conn, items):
    print "{}: start populating 'customers'".format(datetime.datetime.now())

    cur = conn.cursor()

    for i in range(CUSTOMERS_COUNT/BATCH_SIZE):
        rows = []
        for j in range(BATCH_SIZE):
            name = generate_random_string(5, 10)
            item = random.choice(items)

            rows.append((name, item))

        cur.executemany("INSERT INTO customers1(c_name, item) VALUES (:1, :2)", rows)
        conn.commit()

        print "{}: added {} rows to 'customers'".format(datetime.datetime.now(), BATCH_SIZE * (i + 1))

    cur.close()


def main():
    conn = connect()

    cur = conn.cursor()
    cur.execute("DELETE FROM producers")
    cur.execute("DELETE FROM customers")
    cur.execute("DELETE FROM cities")
    conn.commit()
    cur.close()

    populate_cities(conn)
    items = populate_producers(conn)
    populate_customers(conn, items)

    conn.close()


if __name__ == "__main__":
    main()