import pyodbc
import time
from datetime import datetime
from decimal import Decimal
import psycopg2
import sys

DEST_HOST = ''
DEST_DB = ''
DEST_USER = ''
DEST_PASSWORD = ''

SOURCE_DSN = ''
SOURCE_UID = ''

EXCLUDES = []


pcon = psycopg2.connect(host=DEST_HOST, dbname=DEST_DB, user=DEST_USER,
    password=DEST_PASSWORD)
pcur = pcon.cursor()

connection = pyodbc.connect("DSN=%s;Uid=%s" % (SOURCE_DSN, SOURCE_UID))
cursor = connection.cursor()
cursor.execute("SELECT * FROM information_schema.tables")
tables = [x[2] for x in cursor.fetchall()]
table_count = len(tables)

print("Table Count: %d" % table_count)

pcur.execute("CREATE TABLE IF NOT EXISTS odbc2postgres(date TIMESTAMP, "
    "action VARCHAR)")
pcur.execute("INSERT INTO odbc2postgres VALUES (NOW(), 'START')")
pcon.commit()

start = time.time()
print('Total: ', len(tables))
limit = 10000
counter = 0
for table in tables:
    counter += 1
    print('Table: "%s" (%s/%s)' % (table, counter, table_count))
    if table in EXCLUDES:
        continue

    cursor.execute('SELECT count(*) FROM "%s"' % table)
    print("Size: %s" % cursor.fetchone()[0])
    cursor.execute('SELECT * FROM "%s"' % table)
    records = cursor.fetchmany(limit)


    des = []
    for column in cursor.description:
       t = column[1]
       if t == type(''):
           t = 'VARCHAR'
       elif t == int:
           t = 'INTEGER'
       elif t == datetime:
           t = 'TIMESTAMP'
       elif t == float:
           t = 'FLOAT'
       elif t == Decimal:
           t = 'NUMERIC'
       elif t == bytearray:
           t = 'BYTEA'
       elif t == bool:
           t = 'BOOLEAN'
       else:
           print("TYPE: ", t, type(t))
           sys.exit(1)
       des.append('"%s" %s' % (column[0], t))

    pcur.execute('DROP TABLE IF EXISTS "%s"' % table)
    pcur.execute('CREATE TABLE IF NOT EXISTS "%s" (%s)' % (table,
            ', '.join(des)))

    while records:
        print("Preparing %d records..." % len(records))
        new_records = []
        for record in records:
            new_record = []
            for value in record:
                if isinstance(value, datetime):
                    value = value.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(value, Decimal):
                    value = str(value)
                elif isinstance(value, bytes):
                    value = str(value)
                    value = None
                new_record.append(value)
            new_records.append(new_record)

        if new_records:
            print("Inserting %d records..." % len(records))
            values = '(%s)' % ', '.join(['%s' for x in new_record])
            query = 'INSERT INTO "%s" VALUES %s' % (table, ', '.join([values
                        for x in new_records]))
            nr = []
            for x in new_records:
                for v in x:
                    nr.append(v)
            pcur.execute(query, nr)
        pcon.commit()

        records = cursor.fetchmany(limit)


pcur.execute("INSERT INTO odbc2postgres VALUES (NOW(), 'STOP')")
pcon.commit()

end = time.time()
print('Time: ', end - start)
