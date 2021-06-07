import mysql.connector as connection
from flask import Flask, render_template, request, jsonify
import csv
import pymongo
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging as lg
import os

class mysql:
    def logger(self, ex):
        try:
            lg.basicConfig(filename=os.getcwd()+'\\'+'files.log',level=lg.INFO, format='%(asctime)s %(message)s')
            lg.info(ex)
        except Exception as ex1:
            print('There is an error in logging of type: ', ex1)

    def establish_connection(self,host,user,passwd):
        mydb = connection.connect(host=host, user=user, passwd=passwd, use_pure=True)
        print(mydb.is_connected())
        print("The connection is established!!")
        self.logger("The connection is established!!")
        cursor = mydb.cursor()
        return mydb, cursor

    def create_table(self,mydb,cursor,database_name,table_name,query_info):
        try:
            query = "CREATE TABLE {}.{}({});".format(database_name,table_name,query_info)
            cursor.execute(query)
            print("Table created")
            self.logger("Table created")
            mydb.close()
        except Exception as e:
            mydb.close()
            self.logger("Table could not be created")
            print(str(e))
            return str(e)

    def insert_bulk_data(self,mydb,cursor,database_name,table_name,file_path,delimiter,query_info):
        try:
            with open(file_path, 'r') as data:
                next(data)
                data_csv = csv.reader(data, delimiter=delimiter)
                for i in data_csv:
                    try:
                        query = "INSERT INTO {}.{} values({});".format(database_name, table_name,query_info)
                        cursor.execute(query, list(i))
                        print("Bulk Data inserted")
                        self.logger("Bulk Data inserted")
                    except Exception as e:
                        print("Not inserted because of error: ".format(str(e)))
                        self.logger("Not inserted because of error: ".format(str(e)))
                mydb.commit()
                mydb.close()

        except Exception as e:
            mydb.close()
            print(str(e))
            return "Error while Inserting Data: {}".format(str(e))

    def insert_single_data(self,mydb,cursor,database_name,table_name,query_info):
        try:
            query = "INSERT INTO {}.{} values({});".format(database_name,table_name,query_info)
            cursor.execute(query)
            mydb.commit()
            print("Record Inserted")
            self.logger("Record Inserted")
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))
            self.logger("Record could not be inserted")
            return str(e)

    def update_data(self,mydb,cursor,database_name,table_name,set_query_info,where_query_info):
        try:
            query = "UPDATE {}.{} SET {} WHERE {};".format(database_name,table_name,set_query_info,where_query_info)
            cursor.execute(query)
            mydb.commit()
            self.logger("Record updated!!")
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))
            self.logger("Record could not be updated")
            return str(e)

    def delete_data(self,mydb,cursor,database_name,table_name,where_query_info):
        try:
            query = "DELETE FROM {}.{} WHERE {};".format(database_name,table_name,where_query_info)
            cursor.execute(query)
            mydb.commit()
            print("Record deleted!!")
            self.logger("Record deleted!!")
            mydb.close()
        except Exception as e:
            mydb.close()
            print(str(e))
            self.logger("Record could not be deleted")
            return str(e)

    def download_data(self,mydb,cursor,database_name,table_name,file_path,file_name):
        try:
            query = "select * from {}.{};".format(database_name, table_name)
            cursor.execute(query)

            with open(file_path + file_name, "w") as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in cursor.description)
                for row in cursor:
                    writer.writerow(row)
            print("Data downloaded and saved in a csv file")
            self.logger("Data downloaded and saved in a csv file")
        except Exception as e:
            mydb.close()
            print(str(e))
            self.logger("The records could not be downloaded")
            return str(e)

class mongodb():
    def logger(self, ex):
        try:
            lg.basicConfig(filename=os.getcwd() + '\\' + 'mongo_files.log', level=lg.INFO, format='%(asctime)s %(message)s')
            lg.info(ex)
        except Exception as ex1:
            print('There is an error in logging of type: ', ex1)

    def establish_connection(self):
        DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
        client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
        print("The connection is established!!")
        self.logger("The connection is established!!")
        return client

    def create_table(self,client,database_name,table_name):
        try:
            database = client[database_name]
            database[table_name]
            print("Table created")
            self.logger("Table created")
        except Exception as e:
            print(str(e))
            self.logger(str(e))
            return str(e)

    def insert_bulk_data(self, client,database_name, table_name, file_path):
        try:
            database = client[database_name]
            collection = database[table_name]
            df = pd.read_csv(file_path)
            data = df.to_dict('records')
            collection.insert_many(data, ordered=False)
            print("Importing data into database done")
            self.logger("Importing data into database done")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def insert_single_data(self,client,database_name, table_name,record):
        try:
            database = client[database_name]
            collection = database[table_name]
            collection.insert_one(record)
            print("Single record inserted")
            self.logger("Single record inserted")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def insert_multiple_data(self,client,database_name, table_name,multiple_record):
        try:
            database = client[database_name]
            collection = database[table_name]
            collection.insert_many(multiple_record)
            print("Multiple records inserted")
            self.logger("Multiple records inserted")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def update_data(self, client ,database_name, table_name, present_data,new_data):
        try:
            database = client[database_name]
            collection = database[table_name]
            collection.update_one(present_data, new_data)
            print("Record updated")
            self.logger("Record updated")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def delete_data(self,client, database_name, table_name, query_to_delete):
        try:
            database = client[database_name]
            collection = database[table_name]
            collection.delete_one(query_to_delete)
            print("Record deleted from the table")
            self.logger("Record deleted from the table")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def download_data(self, client, database_name, table_name, file_path, delimiter):
        try:
            # E:\\Full_Stack_data_science\\Python\\flask\\passengers_details_mdb.csv
            database = client[database_name]
            collection = database[table_name]
            cursor = collection.find()
            mongo_docs = list(cursor)
            docs = pd.DataFrame(columns=[])
            for doc in mongo_docs:
                doc["_id"] = str(doc["_id"])
                doc_id = doc["_id"]
                series_obj = pd.Series(doc, name=doc_id)
                docs = docs.append(series_obj)
            docs.to_csv(file_path, delimiter)
            print("Downloaded records into csv file")
            self.logger("Downloaded records into csv file")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

class cassandra():
    def logger(self, ex):
        try:
            lg.basicConfig(filename=os.getcwd() + '\\' + 'cass_files.log', level=lg.INFO, format='%(asctime)s %(message)s')
            lg.info(ex)
        except Exception as ex1:
            print('There is an error in logging of type: ', ex1)

    def establish_connection(self):
        cluster = Cluster()
        session = cluster.connect()
        row = session.execute("SELECT release_version from system.local").one()  # Check connection

        if row:
            self.logger("Connection established..."+row[0])
            print("Connection established..."+row[0])
        else:
            self.logger("An Error has Occured during establishing a connection")
            print("An Error has Occured")
        return session

    def create_table(self, session, database_name, table_name,query_info):
        try:
            session.execute("USE {}".format(database_name))
            query = "CREATE TABLE {}.{}({});".format(database_name,table_name,query_info)
            session.execute(query).one()
            print("Table created")
            self.logger("Table created")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def insert_bulk_data(self, session, database_name, table_name, file_path,delimiter,column_names,column_places):
        try:
            with open(file_path, 'r') as data:
                next(data)
                data_csv = csv.reader(data, delimiter=delimiter)

                for value in data_csv:
                    list_query = [int(value[0]), str(value[1]), str(value[2]), str(value[3]), str(value[4]),int(value[5]), str(value[6]), int(value[7])]
                    query = "insert into {}.{}({}) values({})".format(database_name, table_name, column_names,column_places)
                    session.execute(query,list_query)
            print("Done inserting bulk data inside the table")
            self.logger("Done inserting bulk data inside the table")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def insert_single_data(self, session, database_name, table_name, column_names,column_values):
        try:
            query = "INSERT INTO {}.{}({}) values({});".format(database_name,table_name,column_names,column_values)
            session.execute(query)
            print("Single Data inserted")
            self.logger("Single Data inserted")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def update_data(self, session, database_name, table_name, set_query, where_query):
        try:
            query = "UPDATE {}.{} SET {} WHERE {};".format(database_name, table_name, set_query,where_query)
            session.execute(query)
            print("Data updated inside the table")
            self.logger("Data updated inside the table")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def delete_data(self,session, database_name, table_name, where_query):
        try:
            query = "DELETE FROM {}.{} WHERE {};".format(database_name, table_name, where_query)
            session.execute(query)
            print("Data deleted from the table")
            self.logger("Data deleted from the table")
        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)

    def download_data(self,session, database_name, table_name, file_path,file_name):
        try:
            table_all = session.execute("select * from {}.{};".format(database_name, table_name))
            file = open(file_path + "\\" + file_name + ".csv", "w", newline="")
            writer = csv.writer(file)
            for i in table_all:
                writer.writerow(i)
            print("Done downloading the table data")
            self.logger("Done downloading the table data")
            file.close()

        except Exception as e:
            print(str(e))
            self.logger((str(e)))
            return str(e)



