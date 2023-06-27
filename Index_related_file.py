import pymongo
from pymongo import MongoClient
from faker import Faker

# creating a connection string
def create_connection(connection_string):
    try:
        client = MongoClient(connection_string)
        print('connection_success')
        return client
    except Exception:
        print("Failed to connect")
    
connection_string = 'mongodb://localhost:27017/'
client = create_connection(connection_string)

#1. creating a database
def create_database(client):
    try:
        mydb = client["my_database"]
        print("Database is created")
        return mydb
    except Exception as e:
        return e
mydb = create_database(client)

#2 creating a collection
def create_collection(coll):
    try:
        coll= mydb["person_data"]
        print("collection is created")
        return coll
    except Exception as e:
        return e
coll = create_collection(mydb)

#using faker to insert dummy data
fake = Faker()
data = []
for _ in range(100):
    item = {
        "name": fake.name(),
        "age": fake.random_int(min=18, max=65),
        "country": fake.country(),
        "tags": fake.words(nb=3)
    }
    data.append(item)

coll.insert_many(data)

#creating the indexes 
def create_compound_index(coll):
    try:
        coll.create_index([("name", 1), ("age",1)])
        print("Index created")
    except Exception as e:
        return e
create_compound_index(coll)

def create_index(coll):
    try:
        coll.create_index("tags")
        print("Index created")
    except Exception as e:
        return e
create_index(coll)



#getting the indexes
indexes = coll.list_indexes()
for i in indexes:
    print(i)

#1. performing queries after creating index for age field 
def data_search_after_indexing(coll):
    try:
        value = coll.find({"age":{'$gt': 20}})
        for i in value:
            print(i)
    except Exception as Err:
        print(Err)

data_search_after_indexing(coll)

#2. query
def data_search_after_indexing(coll):
    try:
        value = coll.find({"age": 30})
        for i in value:
            print(i)
    except Exception as Err:
        print(Err)

data_search_after_indexing(coll)

#3. query
def data_search_after_indexing(coll):
    try:
        value = coll.find({"age": {"$gte" : 30, "$lte": 50}})
        for i in value:
            print(i)
    except Exception as Err:
        print(Err)

data_search_after_indexing(coll)