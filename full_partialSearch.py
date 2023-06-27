import pymongo
from faker import Faker
from pymongo import MongoClient
#using faker to use dummy data
fake = Faker()

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
        Sample_db= client["Student_db"]
        print("Database is created")
        return Sample_db
    except Exception as e:
        return e
Sample_db = create_database(client)

#2 creating a collection
def create_collection(Sample_db):
    try:
        collection3 = Sample_db["student_details"]
        print("collection is created")
        return collection3
    except Exception as e:
        return e
collection3 = create_collection(Sample_db)

3#. creating a function to insert documents using faker to insert dummy data
def insert_documents(collection3,num_documents):
    for _ in range(num_documents):
        student = {
            'name': fake.name(),
            'age': fake.random_int(min=18, max=25),
            'email': fake.email(),
            'phone_number': fake.phone_number(),
        }
        try:
            collection3.insert_one(student)
        except Exception as err:
            return err
        
# Calling the insert_documents function to insert 10,000 documents
insert_documents(collection3,10000)


#4. creating text index and perform full-text search on the data
def create_text_index(collection3):
    try:
        collection3.create_index([("name","text")])
        print("Success")
        # finding all the names which contain baker in them 
        desired_value = collection3.find({"$text" : {"$search" : "Baker"}})
        for i in desired_value:
            print(i)
    except Exception as e:
        return e
create_text_index(collection3)


#5. To get more than one word like (or opeartion)(either this or that or both) 
# will be covered in the result
def searching_data(collection3):
    try:
        # finding all the names which contain both alex or Schroeder or both too  
        desired_value = collection3.find({"$text" : {"$search" : "Alex Schroeder"}})
        for j in desired_value:
            print(j)
    except Exception as e:
        return e
searching_data(collection3)

#6. searching for the exact match for a particular word we need to display in the result 
def searching_exact_match(collection3):
    try:
        # finding all the names which contain both alex or Schroeder or both too  
        desired_value = collection3.find({"$text" : {"$search" : "\"Angela Bryant\"", "$caseSensitive": True}})
        for k in desired_value:
            print(k)
    except Exception as e:
        return e
searching_exact_match(collection3)

#7 query using regex operator
query = {"name":{"$regex": "A?ex"}}
result = collection3.find(query)
for doc in result:
    print(doc)


#8. Searching for documents where the name starts with a specific prefix
def search_by_prefix(prefix):
    query = {"name": {"$regex": f"^{prefix}"}}
    result = collection3.find(query)
    for document in result:
        print(document)

search_by_prefix("B")

#9. Searching for documents where the name starts with a specific suffix
def search_by_suffix(suffix):
    query = {"name": {"$regex": f"{suffix}$"}}
    result = collection3.find(query)
    for document in result:
        print(document)

search_by_suffix("ge")

#10. Searching the documents where the name matches a specific pattern
def search_by_pattern(pattern):
    query = {"name": {"$regex": pattern}}
    result = collection3.find(query)
    for document in result:
        print(document)
search_by_pattern("Jo.*i")


#11. Searching the documents where the name contains a specific substring
def search_by_substring(substring):
    query = {"name": {"$regex": f".*{substring}.*"}}
    result = collection3.find(query)
    for document in result:
        print(document)

search_by_substring("ander")


