import mysql.connector as connection
from flask import Flask, render_template, request, jsonify
import csv
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

app = Flask(__name__)

@app.route('/mysql/create_table', methods=['POST'])
def create_table():
    database = request.json['database']
    passwd = request.json['passwd']
    table_name = request.json['table_name']
    column1_name = request.json['column1_name']
    data_type1 = request.json['data_type1']
    column2_name = request.json['column2_name']
    data_type2 = request.json['data_type2']
    column3_name = request.json['column3_name']
    data_type3 = request.json['data_type3']
    column4_name = request.json['column4_name']
    data_type4 = request.json['data_type4']
    column5_name = request.json['column5_name']
    data_type5 = request.json['data_type5']
    column6_name = request.json['column6_name']
    data_type6 = request.json['data_type6']
    column7_name = request.json['column7_name']
    data_type7 = request.json['data_type7']
    column8_name = request.json['column8_name']
    data_type8 = request.json['data_type8']

    mydb = connection.connect(host="localhost", database=database, user="root", passwd=passwd, use_pure=True)
    try:

        # check if the connection is established
        print(mydb.is_connected())

        query = "CREATE TABLE {}.{}({} {}(5),{} {}(20),{} {}(50),{} {}(60),{} {}(10),{} {}(10),{} {}(5),{} {}(10));".format(database,table_name,column1_name,data_type1,column2_name,data_type2,column3_name,data_type3,column4_name,data_type4,column5_name,data_type5,column6_name,data_type6,column7_name,data_type7,column8_name,data_type8)

        cursor = mydb.cursor()  # create a cursor to execute queries
        cursor.execute(query)
        mydb.close()
        return "Table Created!!"
    except Exception as e:
        mydb.close()
        return str(e)
    # {
    #     "database": "first_test",
    #     "passwd": "mysql",
    #     "table_name": "passengers",
    #     "column1_name": "PassengerId",
    #     "data_type1": "INT",
    #     "column2_name": "Country",
    #     "data_type2": "VARCHAR",
    #     "column3_name": "Firstname",
    #     "data_type3": "VARCHAR",
    #     "column4_name": "Lastname",
    #     "data_type4": "VARCHAR",
    #     "column5_name": "Sex",
    #     "data_type5": "VARCHAR",
    #     "column6_name": "Age",
    #     "data_type6": "INT",
    #     "column7_name": "Category",
    #     "data_type7": "VARCHAR",
    #     "column8_name": "Survived",
    #     "data_type8": "INT"
    # }

@app.route('/mysql/insert_multiple_data', methods=['POST'])
def insert_multiple_data():
    database = request.json['database']
    passwd = request.json['passwd']
    table_name = request.json['table_name']
    file_path = request.json['file_path']
    delimiter = request.json['delimiter']

    mydb = connection.connect(host="localhost", database=database, user="root", passwd=passwd, use_pure=True)
    print(mydb.is_connected())
    cursor = mydb.cursor()
    try:
        with open(file_path, 'r') as data:
            next(data)
            data_csv = csv.reader(data, delimiter=delimiter)
            for i in data_csv:
                try:
                    query = "INSERT INTO {}.{} values(%s,%s,%s,%s,%s,%s,%s,%s)".format(database, table_name)
                    cursor.execute(query, list(i))
                except Exception as e:
                    print("Not inserted because of error: ".format(str(e)))
            mydb.commit()
            mydb.close()
            return "All the values are inserted"

    except Exception as e:
        return "Error while Inserting Data: {}".format(str(e))

    # {
    #     "database": "first_test",
    #     "passwd": "mysql",
    #     "table_name": "passengers",
    #     "file_path": "E:\\Full_Stack_data_science\\Python\\mySQL\\19.2.MySQL\\Imp files\\estonia-passenger-list.csv",
    #     "delimiter": ","
    # }

@app.route('/mysql/insert_single_data', methods=['POST'])
def insert_single_data():
    database = request.json['database']
    passwd = request.json['passwd']
    table_name = request.json['table_name']
    col1_data = request.json['col1_data']
    col2_data = request.json['col2_data']
    col3_data = request.json['col3_data']
    col4_data = request.json['col4_data']
    col5_data = request.json['col5_data']
    col6_data = request.json['col6_data']
    col7_data = request.json['col7_data']
    col8_data = request.json['col8_data']

    mydb = connection.connect(host="localhost", database=database, user="root", passwd=passwd, use_pure=True)
    print(mydb.is_connected())
    cursor = mydb.cursor()
    try:
        #Note: here string values are enclosed in single quotes, or else it wont work
        query = "INSERT INTO {}.{} values({},'{}','{}','{}','{}',{},'{}',{});".format(database,table_name,col1_data,col2_data,col3_data,col4_data,col5_data,col6_data,col7_data,col8_data)
        cursor.execute(query)
        mydb.commit()
        mydb.close()  # close the connection
        return "Data Inserted"
    except Exception as e:
        mydb.close()
        return str(e)

    # {
    #     "database": "first_test",
    #     "passwd": "mysql",
    #     "table_name": "passengers",
    #     "col1_data": 992,
    #     "col2_data": "Malaysia",
    #     "col3_data": "Mayuri",
    #     "col4_data": "Kanth",
    #     "col5_data": "F",
    #     "col6_data": 64,
    #     "col7_data": "P",
    #     "col8_data": 1
    # }

@app.route('/mysql/update_data', methods=['POST'])
def update_data():
    database = request.json['database']
    passwd = request.json['passwd']
    table_name = request.json['table_name']
    update_col_name = request.json['update_col_name']
    update_col_data = request.json['update_col_data']
    where_col_name = request.json['where_col_name']
    where_col_data = request.json['where_col_data']

    mydb = connection.connect(host="localhost", database=database, user="root", passwd=passwd, use_pure=True)
    print(mydb.is_connected())
    cursor = mydb.cursor()
    try:
        query = "UPDATE {}.{} SET {} = '{}' WHERE {} = '{}';".format(database,table_name,update_col_name,update_col_data,where_col_name,where_col_data)
        cursor = mydb.cursor()  # create a cursor to execute queries
        cursor.execute(query)
        mydb.commit()
        mydb.close()  # close the connection
        return "Data Updated!!"
    except Exception as e:
        mydb.close()
        return str(e)

    # {
    #     "database": "first_test",
    #     "passwd": "mysql",
    #     "table_name": "passengers",
    #     "update_col_name": "Age",
    #     "update_col_data": 66,
    #     "where_col_name": "PassengerId",
    #     "where_col_data": 992
    # }

@app.route('/mysql/delete_data', methods=['POST'])
def delete_data():
    database = request.json['database']
    passwd = request.json['passwd']
    table_name = request.json['table_name']
    where_col_name = request.json['where_col_name']
    where_col_data = request.json['where_col_data']

    mydb = connection.connect(host="localhost", database=database, user="root", passwd=passwd, use_pure=True)
    print(mydb.is_connected())
    cursor = mydb.cursor()
    try:
        query = "DELETE FROM {}.{} WHERE {} = '{}';".format(database,table_name,where_col_name,where_col_data)
        #query = "UPDATE {}.{} SET {} = '{}' WHERE {} = '{}';".format(database,table_name,update_col_name,update_col_data,where_col_name,where_col_data)
        cursor = mydb.cursor()  # create a cursor to execute queries
        cursor.execute(query)
        mydb.commit()
        mydb.close()  # close the connection
        return "Data row Deleted!!"
    except Exception as e:
        mydb.close()
        return str(e)

@app.route('/mysql/download_data', methods=['POST'])
def download_data():
    database = request.json['database']
    passwd = request.json['passwd']
    table_name = request.json['table_name']
    file_path = request.json['file_path'] #E:\\Full_Stack_data_science\\Python\\flask\\
    file_name = request.json['file_name']

    mydb = connection.connect(host="localhost", database=database, user="root", passwd=passwd, use_pure=True)
    print(mydb.is_connected())
    cursor = mydb.cursor()
    try:
        query = "select * from {}.{};".format(database,table_name)
        cursor.execute(query)

        with open(file_path + file_name, "w") as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in cursor.description)
            for row in cursor:
                writer.writerow(row)
        return "Data downloaded and saved in a csv file"
    except Exception as e:
        mydb.close()
        return str(e)

    # {
    #     "database": "first_test",
    #     "passwd": "mysql",
    #     "table_name": "passengers",
    #     "file_path": "E:\\Full_Stack_data_science\\Python\\flask\\",
    #     "file_name": "passengers_details.csv"
    # }

import pymongo



@app.route('/mongodb/create_table', methods=['POST'])
def create_table_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    db_2[table_name]
    return "Table created"

    # {
    #     "database_name": "mongo_test",
    #     "table_name": "products"
    # }

@app.route('/mongodb/insert_single_data', methods=['POST'])
def insert_single_data_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    record = request.json['record']
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    collection = db_2[table_name]
    collection.insert_one(record)
    return "Data inserted"
    #Example1
    # {
    #     "database_name":"mongo_test",
    #     "table_name": "products",
    #     "record": {"companyName": "iNeuron",
    #                "product": "Affordable AI",
    #                "courseOffered": "Deep Learning for Computer Vision",
    #                "name": ["Mannoj", "kumar", 5466],
    #                "staff_detail": {"name": "Ashok", "mail_id": "ashok@email.com", "ph_number": 543535}}
    # }
    #Example2
    # {
    #     "database_name": "mongo_test",
    #     "table_name": "passengers",
    #     "record": {"PassengerId": 990, "Country": "India", "Firstname": "Rajesh", "Lastname": "Koothrapalli",
    #                "Sex": "M", "Age": 43, "Category": "P", "Survived": 1}
    # }

@app.route('/mongodb/insert_multiple_data', methods=['POST'])
def insert_multiple_data_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    list_record = request.json['list_record']
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    collection = db_2[table_name]
    collection.insert_many(list_record)
    return "Multiple Data inserted"

    # {
    #     "database_name": "mongo_test",
    #     "table_name": "products",
    #     "list_record": [
    #         {"companyName": "iNeuron",
    #          "product": "Affordable AI",
    #          "courseOffered": "Machine Learning with Deployment"},
    #         {"companyName": "iNeuron",
    #          "product": "Affordable AI",
    #          "courseOffered": "Deep Learning for NLP and Computer vision"},
    #         {"companyName": "iNeuron",
    #          "product": "Master Program",
    #          "courseOffered": "Data Science Masters Program",
    #          "test": "Siddharth",
    #          "complex": [{"name": "Sharanya", "list": [554, 545, 454, 54, 5, 4]}, {"email_id": "sharanya@email.com"},
    #                      {"phone_no": 345345345353}, [4, 54, 534, 5, 45, 5, 45, 4]]}]
    # }

@app.route('/mongodb/insert_multiple_data_csv', methods=['POST'])
def insert_multiple_data_csv_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path = request.json['file_path'] # E:\\Full_Stack_data_science\\Python\\mySQL\\19.2.MySQL\\Imp files\\estonia-passenger-list.csv
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    collection = db_2[table_name]
    df = pd.read_csv(file_path)
    data = df.to_dict('records')
    collection.insert_many(data, ordered=False)
    return "Importing data into database done"

    # {
    #     "database_name": "mongo_test",
    #     "table_name": "passengers",
    #     "file_path": "E:\\Full_Stack_data_science\\Python\\mySQL\\19.2.MySQL\\Imp files\\estonia-passenger-list.csv"
    # }

@app.route('/mongodb/update_data', methods=['POST'])
def update_data_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    present_data = request.json['present_data']
    new_data = request.json['new_data']
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    collection = db_2[table_name]
    collection.update_one(present_data, new_data)
    return "Data updated"
    #Example1
    # {
    #     "database_name": "mongo_test",
    #     "table_name": "products",
    #     "present_data": {"courseOffered": "Machine Learning with Deployment"},
    #     "new_data": {"$set": {"courseOffered": "ML and DL with Deployment"}}
    # }
    #Example2
    # {
    #     "database_name": "mongo_test",
    #     "table_name": "passengers",
    #     "present_data": {"PassengerId": 990},
    #     "new_data": {"$set": {"PassengerId": 991}}
    # }

@app.route('/mongodb/delete_data', methods=['POST'])
def delete_data_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    query_to_delete = request.json['query_to_delete']
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    collection = db_2[table_name]
    collection.delete_one(query_to_delete)
    return "Data deleted"

    # {
    #     "database_name": "mongo_test",
    #     "table_name": "products",
    #     "query_to_delete": {"product": "Master Program"}
    # }
    #Example
    # {
    #     "database_name": "mongo_test",
    #     "table_name": "passengers",
    #     "query_to_delete": {"PassengerId": 991}
    # }

@app.route('/mongodb/download_data', methods=['POST'])
def download_data_mdb():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    file_path_to_save = request.json['file_path_to_save'] # E:\\Full_Stack_data_science\\Python\\flask\\passengers_details_mdb.csv
    delimiter = request.json['delimiter']
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    db_2 = client[database_name]
    collection = db_2[table_name]
    cursor = collection.find()
    mongo_docs = list(cursor)
    docs = pd.DataFrame(columns=[])
    for doc in mongo_docs:
        doc["_id"] = str(doc["_id"])
        doc_id = doc["_id"]
        series_obj = pd.Series(doc, name=doc_id)
        docs = docs.append(series_obj)
    docs.to_csv(file_path_to_save, delimiter)
    return "Downloaded data into csv file"
    #Example1
    # {
    #     "database_name": "mongo_test",
    #     "table_name": "products",
    #     "file_path_to_save": "E:\\Full_Stack_data_science\\Python\\flask\\products_mdb.csv",
    #     "delimiter": ","
    # }
    #Example2
    # {
    #     "database_name": "mongo_test",
    #     "table_name": "passengers",
    #     "file_path_to_save": "E:\\Full_Stack_data_science\\Python\\flask\\passengers_details_mdb.csv",
    #     "delimiter": ","
    # }
@app.route('/cassandra/create_table', methods=['POST'])
def create_table_cass():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    column1_name = request.json['column1_name']
    data_type1 = request.json['data_type1']
    column2_name = request.json['column2_name']
    data_type2 = request.json['data_type2']
    column3_name = request.json['column3_name']
    data_type3 = request.json['data_type3']
    column4_name = request.json['column4_name']
    data_type4 = request.json['data_type4']
    column5_name = request.json['column5_name']
    data_type5 = request.json['data_type5']
    column6_name = request.json['column6_name']
    data_type6 = request.json['data_type6']
    column7_name = request.json['column7_name']
    data_type7 = request.json['data_type7']
    column8_name = request.json['column8_name']
    data_type8 = request.json['data_type8']

    cluster = Cluster()
    session = cluster.connect()
    row = session.execute("SELECT release_version from system.local").one() # Check connection

    if row:
        print(row[0])
    else:
        print("An Error has Occured")

    session.execute("USE {}".format(database_name))
    query = "CREATE TABLE {}.{}({} {} PRIMARY KEY,{} {},{} {},{} {},{} {},{} {},{} {},{} {});".format(database_name,table_name,column1_name,data_type1,column2_name,data_type2,column3_name,data_type3,column4_name,data_type4,column5_name,data_type5,column6_name,data_type6,column7_name,data_type7,column8_name,data_type8)
    session.execute(query).one()
    return "Table created"

    # {
    #     "database_name": "first_test",
    #     "table_name": "passenger_details",
    #     "column1_name": "PassengerId",
    #     "data_type1": "INT",
    #     "column2_name": "Country",
    #     "data_type2": "VARCHAR",
    #     "column3_name": "Firstname",
    #     "data_type3": "VARCHAR",
    #     "column4_name": "Lastname",
    #     "data_type4": "VARCHAR",
    #     "column5_name": "Sex",
    #     "data_type5": "VARCHAR",
    #     "column6_name": "Age",
    #     "data_type6": "INT",
    #     "column7_name": "Category",
    #     "data_type7": "VARCHAR",
    #     "column8_name": "Survived",
    #     "data_type8": "INT"
    # }
@app.route('/cassandra/insert_single_data', methods=['POST'])
def insert_single_data_cass():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    column1_name = request.json['column1_name']
    data_value1 = request.json['data_value1']
    column2_name = request.json['column2_name']
    data_value2 = request.json['data_value2']
    column3_name = request.json['column3_name']
    data_value3 = request.json['data_value3']
    column4_name = request.json['column4_name']
    data_value4 = request.json['data_value4']
    column5_name = request.json['column5_name']
    data_value5 = request.json['data_value5']
    column6_name = request.json['column6_name']
    data_value6 = request.json['data_value6']
    column7_name = request.json['column7_name']
    data_value7 = request.json['data_value7']
    column8_name = request.json['column8_name']
    data_value8 = request.json['data_value8']

    cluster = Cluster()
    session = cluster.connect()
    row = session.execute("SELECT release_version from system.local").one() # Check connection

    if row:
        print(row[0])
    else:
        print("An Error has Occured")
    # make sure the placeholder for values having string have single quotes
    query = "INSERT INTO {}.{}({},{},{},{},{},{},{},{}) values({},'{}','{}','{}','{}',{},'{}',{});".format(database_name,table_name,column1_name,column2_name,column3_name,column4_name,column5_name,column6_name,column7_name,column8_name,data_value1,data_value2,data_value3,data_value4,data_value5,data_value6,data_value7,data_value8)
    session.execute(query)
    return "Data inserted"

    # {
    #     "database_name": "first_test",
    #     "table_name": "passenger_details",
    #     "column1_name": "PassengerId",
    #     "data_value1": 991,
    #     "column2_name": "Country",
    #     "data_value2": "India",
    #     "column3_name": "Firstname",
    #     "data_value3": "Ram",
    #     "column4_name": "Lastname",
    #     "data_value4": "Kishore",
    #     "column5_name": "Sex",
    #     "data_value5": "M",
    #     "column6_name": "Age",
    #     "data_value6": 56,
    #     "column7_name": "Category",
    #     "data_value7": "P",
    #     "column8_name": "Survived",
    #     "data_value8": 0
    # }

@app.route('/cassandra/update_data', methods=['POST'])
def update_data_cass():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    column_name = request.json['column_name']
    data_value = request.json['data_value']
    where_column = request.json['where_column']
    where_data = request.json['where_data']

    cluster = Cluster()
    session = cluster.connect()
    row = session.execute("SELECT release_version from system.local").one()  # Check connection

    if row:
        print(row[0])
    else:
        print("An Error has Occured")
    # make sure the placeholder for values having string have single quotes
    session.execute("UPDATE {}.{} SET {}='{}' WHERE {}={};".format(database_name,table_name,column_name,data_value,where_column,where_data))
    return "Data updated"

    # {
    #     "database_name": "first_test",
    #     "table_name": "passenger_details",
    #     "column_name": "Lastname",
    #     "data_value": "Aahuja",
    #     "where_column": "PassengerId",
    #     "where_data": 991
    # }
@app.route('/cassandra/delete_data', methods=['POST'])
def delete_data_cass():
    database_name = request.json['database_name']
    table_name = request.json['table_name']
    where_column = request.json['where_column']
    where_data = request.json['where_data']

    cluster = Cluster()
    session = cluster.connect()
    row = session.execute("SELECT release_version from system.local").one()  # Check connection

    if row:
        print(row[0])
    else:
        print("An Error has Occured")
    # make sure the placeholder for values having string have single quotes
    session.execute("DELETE FROM {}.{} WHERE {}={};".format(database_name, table_name, where_column, where_data))
    return "Data deleted"
    # {
    #     "database_name": "first_test",
    #     "table_name": "passenger_details",
    #     "where_column": "PassengerId",
    #     "where_data": 991
    # }

if __name__ == '__main__':
    app.run()

