#!/usr/bin/env python3
import sys
import csv
import MySQLdb
import psycopg2

def sql_error(e):
    print("SQL error: ", str(e))

def file_error(e):
    print("File error: ", str(e))

def export_csv(cursor, databaseName, tableName):
    columns = None
    data = None
    try:
        cursor.execute("select column_name from information_schema.columns where table_name = %s", (tableName,))
        columns = cursor.fetchall()
        columns = [column[0] for column in columns]
        cursor.execute("select {} from {}".format(", ".join(columns), tableName))
        data = cursor.fetchall()
    except Exception as e:
        sql_error(e)

    if columns == None or data == None:
        return

    try:
        with open("./{}-{}.csv".format(databaseName, tableName), newline='', mode='w') as file:
            writer = csv.writer(file, quotechar='\'', quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            print(columns)
            for row in data:
                print(row)
                writer.writerow(row)
            
    except IOError as e:
        file_error(e)
    return

def export(cursor, databaseName, table):
    
    if table == None:
        try:
            cursor.execute("select table_name from information_schema.tables where table_schema in ('public', %s);", (databaseName,))
            data = cursor.fetchall()
        except Exception as e:
            sql_error(e)
        print(data)
        table = tuple(data)
    else:
        table = ((table,),)

    for name in table:
        print(name[0])
        export_csv(cursor, databaseName, name[0])
    return

def main(argc, argv):
    argv = argv[1:]
    argc = argc - 1
    print(argv, argc)

    if argc < 5 or argc > 6 or argv[1] == "-h" or argv[1] == "--help":
        print("""usage:
        ./export-sql.py MySQL      address user password database [table]
        ./export-sql.py PostgreSQL address user password database [table]""")
        exit()

    dbType =       argv[0]
    address =      argv[1]
    user =         argv[2]
    password =     argv[3]
    databaseName = argv[4]
    table =        argv[5] if argc == 6 else None

    database = None
    try:
        if dbType == "MySQL":
            database = MySQLdb.connect(address, user, password, databaseName)
        else:
            database = psycopg2.connect(host=address, user=user, password=password, database=databaseName)
    except Exception as e:
        sql_error(e)

    if database == None:
        exit()

    print(dbType, address, user, password, databaseName, table)

    cursor = database.cursor()
    export(cursor, databaseName, table)

    cursor.close()
    database.rollback()
    database.close()

main(len(sys.argv), sys.argv)