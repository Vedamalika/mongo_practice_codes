from faker import Faker
from pymongo import MongoClient

fake = Faker()

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
        mydb2 = client["geospatial_db"]
        print("Database is created")
        return mydb2
    except Exception as e:
        return e
mydb2 = create_database(client)

#2 creating a collection
def create_collection(mydb2):
    try:
        collection4= mydb2["geospatial_collection"]
        print("collection is created")
        return collection4
    except Exception as e:
        return e
collection4= create_collection(mydb2)

#3. inserting the sample geospatial data using faker 
def inserting_sample_data(collection4,num_samples):
        for _ in range(num_samples):
            location = {
                'name': fake.name(),
                'address': fake.address(),
                'coordinates': [float(fake.longitude()), float(fake.latitude())],
            }
            try:
                  collection4.insert_one(location)
            except Exception as e:
                  return e

        print("sample geospatial data inserted successfully")

inserting_sample_data(collection4,50)

#4. creating index for 
def create_2dsphere_index():
    try:
        # Creating the 2dsphere index
        result = collection4.create_index([("coordinates", "2dsphere")])
        return result

    except Exception as e:
        return e

# Calling the function
index_name = create_2dsphere_index()

#printing the index name:
print(index_name)



#5. finding the nearby locations using near query operator 
def geospatial_query(latitude, longitude):
    try:
        query = {
            'coordinates': {
                '$near': {
                    '$geometry': {
                        'type': 'Point',
                        'coordinates': [longitude, latitude]
                    }
                }
            }
        }
        result = collection4.find(query)
        for document in result:
            print(document)
    
    except Exception as e:
        return e

# Calling the function with the desired latitude and longitude
geospatial_query(-51.6420625, -67.119402)


#6. second query with near operator and specifying the min(nearest) and max(farthest)
def geospatial_query(longitude,latitude):
    try:
        query = {
            'coordinates':{
                '$near': {
                    '$geometry':{
                        'type':'Point',
                        'coordinates':[longitude,latitude]
                    },
                    "$minDistance": 30,
                    "$maxDistance": 500
                    
                }
            }
        }
        result = collection4.find(query)
        return result
    except Exception as e:
        return e
#making a function call    
res = geospatial_query(-133.949699,-3.9672815)
for doc in res:
    print(doc)



