import pyodbc
import time
from datetime import datetime
from decimal import Decimal
import psycopg2
import sys
import argparse

parser = argparse.ArgumentParser(description='Copies all tables and data from '
    'an ODBC connection to a PostgreSQL database')
parser.add_argument('--dest-host', dest='dest_host')
parser.add_argument('--dest-database', dest='dest_database')
parser.add_argument('--dest-user', dest='dest_user')
parser.add_argument('--dest-password', dest='dest_password')
parser.add_argument('--source-dsn', dest='source_dsn')
parser.add_argument('--source-uid', dest='source_uid')
parser.add_argument('--source-password', dest='source_password')
parser.add_argument('--exclude', dest='exclude', nargs='+', default=[],
    help='tables to be excluded')
parser.add_argument('--include', dest='include', nargs='+', default=[],
    help='tables to be included (all by default)')

args = parser.parse_args()

pcon = psycopg2.connect(host=args.dest_host, dbname=args.dest_database,
    user=args.dest_user, password=args.dest_password)
pcur = pcon.cursor()

datasource = []
if args.source_dsn:
    datasource.append('DSN=%s' % args.source_dsn)
if args.source_uid:
    datasource.append('Uid=%s' % args.source_uid)
if args.source_password:
    datasource.append('PWD=%s' % args.source_password)
connection = pyodbc.connect(';'.join(datasource))

cursor = connection.cursor()
cursor.execute("SELECT * FROM information_schema.tables")
if args.include:
    tables = args.include
else:
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
    if table in args.exclude:
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
