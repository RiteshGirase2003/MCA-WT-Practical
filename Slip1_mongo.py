from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# MCA_WT is Database name 
db = client["MCA_WT"]

# Collections
owners_collection = db["owners"]  # Collection for owners' information
properties_collection = db["properties"]  # Collection for property information

# Clear collections for a fresh start (optional)
owners_collection.delete_many({})  # Deletes all documents in the owners collection
properties_collection.delete_many({})  # Deletes all documents in the properties collection

# Sample data insertion
# Insert owners
# Inserting sample documents into the owners collection, each document represents an owner with an ID, name, contact, and associated property IDs.
owners = [
    {"_id": 1, "name": "Mr. Patil", "contact": "1234567890", "properties": [1, 2]},
    {"_id": 2, "name": "Ms. Sharma", "contact": "0987654321", "properties": [3]},
    {"_id": 3, "name": "Mr. Singh", "contact": "1122334455", "properties": [4, 5]}
]
# Insert many -> it inserts many document/records 
owners_collection.insert_many(owners)  # Insert list of documents into the owners collection

# Insert properties
# Inserting sample documents into the properties collection, each document represents a property with an ID, address, rate, area, and owner ID.
properties = [
    {"_id": 1, "address": {"area": "Nashik Road", "city": "Nashik"}, "rate": 75000, "area": 500, "owner_id": 1},
    {"_id": 2, "address": {"area": "Gangapur Road", "city": "Nashik"}, "rate": 150000, "area": 800, "owner_id": 1},
    {"_id": 3, "address": {"area": "MG Road", "city": "Mumbai"}, "rate": 200000, "area": 1000, "owner_id": 2},
    {"_id": 4, "address": {"area": "Makhmalabad", "city": "Nashik"}, "rate": 90000, "area": 700, "owner_id": 3},
    {"_id": 5, "address": {"area": "Dadar", "city": "Mumbai"}, "rate": 85000, "area": 650, "owner_id": 3}
]
properties_collection.insert_many(properties)  # Insert list of documents into the properties collection

# Queries

# a. Display area-wise property details
print("a. Area-wise property details:")
# Using aggregate to group properties by area within the address field

# "$group": { "_id": "$address.area" }: Groups documents by the area field inside the address subdocument.
# "properties": { "$push": "$$ROOT" }: Pushes the entire document ($$ROOT) for each group into an array called properties.
# $push : Collects values and puts them into an array.
# $$ROOT : The entire document currently being processed.
# $$ Prefix: Indicates that it’s a special variable in MongoDB’s aggregation.
pipeline = [
    
    {
        "$group": {
            "_id": "$address.area",  # Group by the 'area' field inside the 'address' field
            "properties": {"$push": "$$ROOT"}  # Collect all documents for each area into a list
        }
    }
]
area_wise_properties = properties_collection.aggregate(pipeline)

# Iterate and print each group
for area in area_wise_properties:
    print(area)

# b. Display property owned by 'Mr. Patil' having minimum rate
print("\nb. Property owned by 'Mr. Patil' with minimum rate:")
# Finding Mr. Patil's document in the owners collection
mr_patil = owners_collection.find_one({"name": "Mr. Patil"})
if mr_patil:
    # Finding properties with Mr. Patil's ID and sorting by 'rate' in ascending order to get the minimum rate
    min_rate_property = properties_collection.find({"owner_id": mr_patil["_id"]}).sort("rate", 1).limit(1)
    for property_ in min_rate_property:
        print(property_)
else:
    print("Mr. Patil not found in database.")

# c. Details of owner whose property is at “Nashik”
print("\nc. Details of owner with property in Nashik:")
# Finding one property in Nashik city
property_in_nashik = properties_collection.find_one({"address.city": "Nashik"})
if property_in_nashik:
    # Using the 'owner_id' from the found property to retrieve the corresponding owner's information
    owner_in_nashik = owners_collection.find_one({"_id": property_in_nashik["owner_id"]})
    print(owner_in_nashik)
else:
    print("No property found in Nashik.")

# d. Display area of property with rate less than 100000
print("\nd. Area of property with rate less than 100000:")
# Query to find properties where rate is less than 100000
properties_less_than_100000 = properties_collection.find(
    {"rate": {"$lt": 100000}},  # Filter condition for properties with rate < 100000
    {"address.area": 1, "_id": 0}  # Project only the 'area' field inside 'address'; exclude '_id' from output
)
# Iterate and print the areas of each property found
for property_ in properties_less_than_100000:
    print(property_)
