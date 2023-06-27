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
        coll= mydb["Places"]
        print("collection is created")
        return coll
    except Exception as e:
        return e
coll = create_collection(mydb)

# # inserting the documents 
def insert_places_data(Places):
    try:
        # Insert the data into the collection
        result = coll.insert_many(Places)
        return list(result)

    except Exception as e:
        return e
    
places_data = [
    {
        "name": "Restaurant 1",
        "location": {
            "type": "Point",
            "coordinates": [78.446915, 17.4037156]
        }
    },
    {
        "name": "Restaurant 2",
        "location": {
            "type": "Point",
            "coordinates": [40, 5]
        }
    }
]

# # Calling the function
inserted_ids = insert_places_data(places_data)

# creating index to the location 
def create_2dsphere_index():
    try:
        # Creating the 2dsphere index
        result = coll.create_index([("location", "2dsphere")])
        return result

    except Exception as e:
        return e

# # Calling the function
index_name = create_2dsphere_index()

# # Printing the created index name
print(index_name)


#finding neraby places for a given particular location
def find_nearby_places(coordinates, min_distance, max_distance):
    try:
        query = {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": coordinates
                    },
                    "$minDistance": min_distance,
                    "$maxDistance": max_distance
                }
            }
        }

        # Execute the query
        result = coll.find(query)

        # Return the result as a list of dictionaries
        return list(result)

    except Exception as e:
        return e

# Define the coordinates, min_distance, and max_distance
coordinates = [78.446915, 17.4037156]
min_distance = 1000
max_distance = 5000

# Calling the function
nearby_places = find_nearby_places(coordinates, min_distance, max_distance)

# Print the matching documents
for place in nearby_places:
    print(place)


