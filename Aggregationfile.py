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
        mydb1= client["Student_db"]
        print("Database is created")
        return mydb1
    except Exception as e:
        return e
mydb1 = create_database(client)

#2 creating a collection
def create_collection(mydb1):
    try:
        col= mydb1["Pizza_store"]
        print("collection is created")
        return col
    except Exception as e:
        return e
col = create_collection(mydb1)

#3. Inserting many documents in the col

def insert_many(col,orders):
    try:
        record = col.insert_many(orders)
        print("many documents are inserted")
        return record
    except Exception as e:
        return e 

orders = [
        { "_id": 0, "name": "Pepperoni", "size": "small", "price": 19, "quantity": 10 },
        { "_id": 1, "name": "Pepperoni", "size": "medium", "price": 20, "quantity": 20 },
        { "_id": 2, "name": "Pepperoni", "size": "large", "price": 21, "quantity": 30},
        { "_id": 3, "name": "Cheese", "size": "small", "price": 12, "quantity": 15 },
        { "_id": 4, "name": "Cheese", "size": "medium", "price": 13, "quantity": 50},
        { "_id": 6, "name": "Vegan", "size": "small", "price": 17, "quantity": 10 },
        { "_id": 7, "name": "Vegan", "size": "medium", "price": 18, "quantity": 10}
    ]

# Insert the orders into the collection
record = insert_many(col,orders)


#4. getting the desired size pizzas using filter condition find
def find_pizza_by_size(size):
    try:
        query = {"size": size}
        result = col.find(query)
        return list(result)
    
    except Exception as e:
        return e
pizzas = find_pizza_by_size("medium")
for pizza in pizzas:
    print(pizza)


#5. Now doing using aggregation on the pizzas using match filter(query operator)
def aggregate_pizzas_by_size(size):
    try:
        # Defining the aggregation pipeline
        pipeline = [
            {"$match": {"size": size}}
        ]
        # Executing the aggregation query
        result = col.aggregate(pipeline)
        return list(result)

    except Exception as e:
        return e

# Calling the function
pizzas = aggregate_pizzas_by_size("medium")

# Printing the documents
for pizza in pizzas:
    print(pizza)


#6. Grouping aggregation 
def aggregate_pizzas_by_size1(size):
    try:
        # Define the aggregation pipeline
        pipeline = [
            {"$match": {"size": size}},
            {"$group": {"_id": "$name", "total": {"$sum": 1}}}
        ]
        # Execute the aggregation query
        result = col.aggregate(pipeline)
        return list(result)

    except Exception as e:
        return e

# Calling the function to aggregate pizzas with size "small"
pizzas = aggregate_pizzas_by_size1("small")

# Print the documents
for pizza in pizzas:
    print(pizza)


def aggregate_pizzas_by_size2(size):
    try:
        # Define the aggregation pipeline
        pipeline = [
            {"$match": {"size": size}},
            {"$group": {"_id": "$name", "total": {"$sum": 1}}}
        ]
        # Execute the aggregation query
        result = col.aggregate(pipeline)
        return list(result)

    except Exception as e:
        return e

# Calling the function
pizzas = aggregate_pizzas_by_size2("medium")

# Print the documents
for pizza in pizzas:
    print(pizza)

#7. getting the quantity of size medium pizza 
def aggregate_pizzas_by_size3(size):
    try:
        # aggregation pipeline
        pipeline = [
            {"$match": {"size": size}},
            {"$group": {"_id": {"pizza": "$name"}, "total": {"$sum": 1}, "total_quantity": {"$sum": "$quantity"}}}
        ]

        # Execute the aggregation query
        result = col.aggregate(pipeline)

        return list(result)

    except Exception as e:
        return e
    
pizzas = aggregate_pizzas_by_size3("medium")

# Print the aggregated documents
for pizza in pizzas:
    print(pizza)


