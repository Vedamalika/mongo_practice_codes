import pymongo

from pymongo import MongoClient

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
        mydatabase = client["Collage"]
        print("Database is created")
        return mydatabase
    except Exception as e:
        return e
mydatabase = create_database(client)


#  2. to list the databases 
def list_databases(client):
    try:
        res = client.list_database_names()
        return res
    except Exception as err:
        return err
res = list_databases(client) 
print(res)       


#3. creating a collection named student 
def create_collection(mydatabase):
    try:
        collection1 = mydatabase["student"]
        print("collection is created")
        return collection1
    except Exception as e:
        return e
collection1 = create_collection(mydatabase)


#4.inserting one document in the collection1
def insert_document(collection1,document):
    try:
        record = collection1.insert_one(document)
        print("Document is inserted")
        return record
    except Exception as e:
        return e

document = {
    "name": "Har",
    "Roll_no": "10",
    "Branch":"I"
}
record = insert_document(collection1,document)


#5. Inserting many documents in the collection1

def insert_many(collection1,document1):
    try:
        records = collection1.insert_many(document1)
        print("many documents are inserted")
        return records
    except Exception as e:
        return e 
document1 = [
    {"name": "John","Roll_no": "107","Branch":'ECE'},
    {"name": "Jack","Roll_no": "108","Branch":'Civil'},
    {"name": "Jill","Roll_no": "109","Branch":'MNT'},
]

records = insert_many(collection1,document1)

#6. Printing all the documents in a collection
def get_all_collections(collection1):
    try:
        for doc in collection1.find():
            print(doc)
    except Exception as e:
        return e
get_all_collections(collection1)


#7. to find the first document in a collection 
def get_first_document(collection1):
    try:
        first = collection1.find_one()
        print(first)
    except Exception as e:
        return e 
get_first_document(collection1)
    

#8. to print a document when a particular condition is satisfied
def get_desired_document(collection1):
    try:
        y = collection1.find_one({"name":"Harsha"})
        print(y)
    except Exception as e:
        return e 
get_desired_document(collection1)

def get_desired_document(collection1):
    try:
        y = collection1.find_one({"Roll_no":"105"})
        print(y)
    except Exception as e:
        return e 
get_desired_document(collection1)

#9. for counting the number of Documents in a collection
def get_document_count(collection1):
    try:
        y = collection1.count_documents({})
        print(y)
    except Exception as e:
        return e 
get_document_count(collection1)


#10. Updating a document based on a condition
def update_document(collection1):
    try:
        filter = {"name":"Jill"}
        updation_value = {"$set":{"Branch": "MECH"}}
        res = collection1.update_one(filter,updation_value)
        print("document is updated")
        return res
    except Exception as e:
        return e 
result = update_document(collection1)



#11. to delete any document from a collection
def delete_document(collection1):
    try:
        filter = {"name":"Har"}
        collection1.delete_one(filter)
        print("document is deleted")
    except Exception as e:
        return e 
delete_document(collection1)


#12. Aggregations 
#match and count functions
def get_the_count(collection1):
    try:
        agg_res = collection1.aggregate( [ 
            { "$match": {"Roll_no" : "101"} },
            { "$count": "Total students with roll_no 101"}])

        return agg_res
    except Exception as e:
        return e 
        
agg_res = get_the_count(collection1)
for i in agg_res:
    print(i)

def get_the_count(collection1):
    try:
        agg_res = collection1.aggregate( [ 
            { "$match": {"name" : "Jill"} },
            { "$count": "Total students with name jill"}])

        return agg_res
    except Exception as e:
        return e 
        
agg_res = get_the_count(collection1)
for i in agg_res:
    print(i)


#group function 
def get_the_match(collection1):
    try:
        agg_res = collection1.aggregate( [ 
            { "$group": {"_id" : "$name"}}])

        return agg_res
    except Exception as e:
        return e 
        
agg_res = get_the_match(collection1)
for i in agg_res:
    print(i)






