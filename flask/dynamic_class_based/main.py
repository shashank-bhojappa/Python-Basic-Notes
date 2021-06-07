# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import mysql.connector as connection
from flask import Flask, render_template, request, jsonify
import csv
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from database_backend import mysql
from database_backend import mongodb
from database_backend import cassandra

app = Flask(__name__)

@app.route('/mysql/create_table', methods=['POST'])
def mysql_create_table():
    host = request.json['host']
    user = request.json['user']
    passwd = request.json['passwd']
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    query_info = request.json['query_info']

    table_creation = mysql()
    mydb,cursor = table_creation.establish_connection(host,user,passwd)
    table_creation.create_table(mydb,cursor,database_name,table_name,query_info)
    return "Table is Created!!"
# {
#     "host":"localhost",
#     "user":"root",
#     "passwd":"mysql",
#     "database_name":"first_test",
#     "table_name":"estonia_passengers",
#     "query_info":"PassengerId int(10),Country varchar(30), Firstname varchar(50),Lastname varchar(50),Sex varchar(5), Age int(5), Category varchar(5), Survived int(5)"
# }

@app.route('/mysql/bulk_data', methods=['POST'])
def bulk_data():
    host = request.json['host']
    user = request.json['user']
    passwd = request.json['passwd']
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path = request.json['file_path']
    delimiter = request.json['delimiter']
    query_info = request.json['query_info']

    data_insert = mysql()
    mydb, cursor = data_insert.establish_connection(host, user, passwd)
    data_insert.insert_bulk_data(mydb,cursor,database_name,table_name,file_path,delimiter,query_info)
    return "Bulk data is inserted!!"
# {
#     "host":"localhost",
#     "user":"root",
#     "passwd":"mysql",
#     "database_name":"first_test",
#     "table_name":"estonia_passengers",
#     "file_path": "E:\\Full_Stack_data_science\\Python\\mySQL\\19.2.MySQL\\Imp files\\estonia-passenger-list.csv",
#     "delimiter":",",
#     "query_info":"%s,%s,%s,%s,%s,%s,%s,%s" Note: "how many columns are there add so many %s"
# }

@app.route('/mysql/single_data', methods=['POST'])
def mysql_single_data():
    host = request.json['host']
    user = request.json['user']
    passwd = request.json['passwd']
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    query_info = request.json['query_info']

    record_insert = mysql()
    mydb,cursor = record_insert.establish_connection(host,user,passwd)
    record_insert.insert_single_data(mydb,cursor,database_name,table_name,query_info)
    return "Record inserted inside the table"
# {
#     "host":"localhost",
#     "user":"root",
#     "passwd":"mysql",
#     "database_name":"first_test",
#     "table_name":"estonia_passengers",
#     "query_info":"991,'India','Abdul','Kalam','M',75,'P',0"
# }

@app.route('/mysql/update_data', methods=['POST'])
def mysql_update_data():
    host = request.json['host']
    user = request.json['user']
    passwd = request.json['passwd']
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    set_query_info = request.json['set_query_info']
    where_query_info = request.json['where_query_info']

    record_update = mysql()
    mydb,cursor = record_update.establish_connection(host,user,passwd)
    record_update.update_data(mydb,cursor,database_name,table_name,set_query_info,where_query_info)
    return "Record updated inside the table"
    # Example 1 - where set value and where value are integers
    # {
    #     "host": "localhost",
    #     "user": "root",
    #     "passwd": "mysql",
    #     "database_name": "first_test",
    #     "table_name": "estonia_passengers",
    #     "set_query_info": "Age=90",
    #     "where_query_info": "PassengerId=991"
    # }
    # Example 2 - where set value and where value are strings
    # {
    #     "host": "localhost",
    #     "user": "root",
    #     "passwd": "mysql",
    #     "database_name": "first_test",
    #     "table_name": "estonia_passengers",
    #     "set_query_info": "Firstname='Sir Dhyan'",
    #     "where_query_info": "Lastname = 'Chand'"
    # }

@app.route('/mysql/delete_data', methods=['POST'])
def mysql_delete_data():
    host = request.json['host']
    user = request.json['user']
    passwd = request.json['passwd']
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    where_query_info = request.json['where_query_info']

    record_delete = mysql()
    mydb,cursor = record_delete.establish_connection(host,user,passwd)
    record_delete.delete_data(mydb,cursor,database_name,table_name,where_query_info)
    return "Record deleted inside the table"
#Example 1 - where value is string
# {
#     "host":"localhost",
#     "user":"root",
#     "passwd":"mysql",
#     "database_name":"first_test",
#     "table_name":"estonia_passengers",
#     "where_query_info": "Lastname = 'Kalam'"
# }
# Example 2 - where value is integer
# {
#     "host":"localhost",
#     "user":"root",
#     "passwd":"mysql",
#     "database_name":"first_test",
#     "table_name":"estonia_passengers",
#     "where_query_info": "PassengerId = 991"
# }

@app.route('/mysql/download_data', methods=['POST'])
def mysql_download_data():
    host = request.json['host']
    user = request.json['user']
    passwd = request.json['passwd']
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path = request.json['file_path']
    file_name = request.json['file_name']

    record_download = mysql()
    mydb,cursor = record_download.establish_connection(host,user,passwd)
    record_download.download_data(mydb,cursor,database_name,table_name,file_path,file_name)
    return "Records downloaded to a csv file"
# {
#     "host":"localhost",
#     "user":"root",
#     "passwd":"mysql",
#     "database_name":"first_test",
#     "table_name":"estonia_passengers",
#     "file_path":"E:\\Full_Stack_data_science\\Python\\flask\\",
#     "file_name":"passengers_details.csv"
# }

@app.route('/mongodb/create_table', methods=['POST'])
def mongodb_create_table():
    database_name = request.json['database_name']
    table_name = request.json['table_name']

    table_creation = mongodb()
    client = table_creation.establish_connection()
    table_creation.create_table(client,database_name,table_name)
    return "Table Created!!"
# {
#     "database_name":"first_test",
#     "table_name":"passengers"
# }

@app.route('/mongodb/bulk_data', methods=['POST'])
def bulk_data_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path  = request.json['file_path']

    data_insert = mongodb()
    client = data_insert.establish_connection()
    data_insert.insert_bulk_data(client,database_name,table_name,file_path)
    return "Bulk records are done inserting into the table"
# {
#     "database_name":"first_test",
#     "table_name":"passengers",
#     "file_path": "E:\\Full_Stack_data_science\\Python\\mySQL\\19.2.MySQL\\Imp files\\estonia-passenger-list.csv"
# }

@app.route('/mongodb/single_data', methods=['POST'])
def mongodb_single_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    record = request.json['record']

    record_insert = mongodb()
    client = record_insert.establish_connection()
    record_insert.insert_single_data(client,database_name,table_name,record)
    return "Single record inserted into the table"
# {
#     "database_name":"first_test",
#     "table_name":"passengers",
#     "record": {"PassengerId": 990, "Country": "India", "Firstname": "Rajesh", "Lastname":"Koothrapalli","Sex": "M", "Age": 43, "Category": "P", "Survived": 1}
# }

@app.route('/mongodb/multiple_data', methods=['POST'])
def mongodb_multiple_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    multiple_record = request.json['multiple_record']

    multi_record_insert = mongodb()
    client = multi_record_insert.establish_connection()
    multi_record_insert.insert_multiple_data(client,database_name,table_name,multiple_record)
    return "Multiple records inserted into the table"
# {
#     "database_name":"first_test",
#     "table_name":"passengers",
#     "multiple_record": [{"PassengerId": 991, "Country": "India", "Firstname": "Dr.Abdul", "Lastname":"Kalam","Sex": "M", "Age": 75, "Category": "P", "Survived": 0},{"PassengerId": 992, "Country": "India", "Firstname": "Dr.Dhyan", "Lastname":"Chand","Sex": "M", "Age": 90, "Category": "P", "Survived": 0}]
# }

@app.route('/mongodb/update_data', methods=['POST'])
def mongodb_update_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    present_data = request.json['present_data']
    new_data = request.json['new_data']

    record_update = mongodb()
    client = record_update.establish_connection()
    record_update.update_data(client,database_name,table_name,present_data,new_data)
    return "Record updated in the table"
# {
#     "database_name":"first_test",
#     "table_name":"passengers",
#     "present_data": {"Firstname": "Dr.Dhyan"},
#     "new_data": {"$set": {"Firstname": "Sir.Dhyan"}}
# }

@app.route('/mongodb/delete_data', methods=['POST'])
def mongodb_delete_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    query_to_delete = request.json['query_to_delete']

    record_delete = mongodb()
    client = record_delete.establish_connection()
    record_delete.delete_data(client,database_name,table_name,query_to_delete)
    return "Records deleted from the table"
# {
#     "database_name":"first_test",
#     "table_name":"passengers",
#     "query_to_delete":{"PassengerId": 991}
# }

@app.route('/mongodb/download_data', methods=['POST'])
def mongodb_download_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path = request.json['file_path']
    delimiter = request.json['delimiter']

    record_download = mongodb()
    client = record_download.establish_connection()
    record_download.download_data(client,database_name,table_name,file_path,delimiter)
    return "Records from table are Exported into a csv file"
# {
#     "database_name":"first_test",
#     "table_name":"passengers",
#     "file_path": "E:\\Full_Stack_data_science\\Python\\flask\\passengers_details_mdb.csv",
#     "delimiter": ","
# }

@app.route('/cassandra/create_table', methods=['POST'])
def cassandra_create_table():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    query_info = request.json['query_info']

    table_creation = cassandra()
    session = table_creation.establish_connection()
    table_creation.create_table(session,database_name,table_name,query_info)
    return "Table created!!"
# {
#     "database_name":"first_test",
#     "table_name":"estonia",
#     "query_info":"PassengerId int PRIMARY KEY,Country varchar, Firstname varchar,Lastname varchar,Sex varchar, Age int, Category varchar, Survived int"
# }

@app.route('/cassandra/bulk_data', methods=['POST'])
def bulk_data_cass():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    column_names = request.json['column_names']
    column_places = request.json['column_places']
    file_path = request.json['file_path']
    delimiter = request.json['delimiter']
    #list_query = request.json['list_query']

    data_insert = cassandra()
    session = data_insert.establish_connection()
    data_insert.insert_bulk_data(session,database_name,table_name,file_path,delimiter,column_names,column_places)
    return "Bulk data inserted into the table"
# {
#     "database_name":"first_test",
#     "table_name":"estonia",
#     "column_names":"PassengerId,Country,Firstname,Lastname,Sex,Age,Category,Survived",
#     "column_places":"%s,%s,%s,%s,%s,%s,%s,%s",
#     "file_path": "E:\\Full_Stack_data_science\\Python\\mySQL\\19.2.MySQL\\Imp files\\estonia-passenger-list.csv",
#     "delimiter":",",
#     "list_query":"int(value[0]), str(value[1]), str(value[2]), str(value[3]), str(value[4]), int(value[5]), str(value[6]), int(value[7])"
# }

@app.route('/cassandra/single_data', methods=['POST'])
def cassandra_single_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    column_names = request.json['column_names']
    column_values = request.json['column_values']

    record_insert = cassandra()
    session = record_insert.establish_connection()
    record_insert.insert_single_data(session,database_name,table_name,column_names,column_values)
    return "Single data is inserted into the table"
# {
#     "database_name":"first_test",
#     "table_name":"estonia",
#     "column_names":"PassengerId,Country,Firstname,Lastname,Sex,Age,Category,Survived",
#     "column_values":"991,'India','Abdul','Kalam','M',75,'P',0"
# }

@app.route('/cassandra/update_data', methods=['POST'])
def cassandra_update_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    set_query = request.json['set_query']
    where_query = request.json['where_query']

    record_update = cassandra()
    session = record_update.establish_connection()
    record_update.update_data(session,database_name,table_name,set_query,where_query)
    return "The data inside the table is updated"
# {
#     "database_name":"first_test",
#     "table_name":"estonia",
#     "set_query":"Firstname = 'Dr.Abdul'",
#     "where_query":"PassengerId = 991" #where should be a primary key
# }

@app.route('/cassandra/delete_data', methods=['POST'])
def cassandra_delete_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    where_query = request.json['where_query']

    record_delete = cassandra()
    session = record_delete.establish_connection()
    record_delete.delete_data(session,database_name,table_name,where_query)
    return "The data is deleted from the table"
# {
#     "database_name":"first_test",
#     "table_name":"estonia",
#     "where_query":"PassengerId = 991"
# }

@app.route('/cassandra/download_data', methods=['POST'])
def cassandra_download_data():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path = request.json['file_path']
    file_name = request.json['file_name']

    record_download = cassandra()
    session = record_download.establish_connection()
    record_download.download_data(session,database_name,table_name,file_path,file_name)
    return "Downloaded all the data from the table into a csv file"

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app.run()

