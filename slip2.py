from pymongo import MongoClient
from bson.objectid import ObjectId

# Connect to MongoDB (replace with your connection string if necessary)
client = MongoClient("mongodb://localhost:27017/")
db = client["newspaper_database"]

# Collections
newspapers = db["newspapers"]
publishers = db["publishers"]
cities = db["cities"]

# Sample Data Insertion

# Insert cities data
cities.insert_many([
    {"_id": ObjectId("nashik_city_id"), "name": "Nashik", "state": "Maharashtra"},
    {"_id": ObjectId("pune_city_id"), "name": "Pune", "state": "Maharashtra"},
    {"_id": ObjectId("mumbai_city_id"), "name": "Mumbai", "state": "Maharashtra"},
    {"_id": ObjectId("ahmedabad_city_id"), "name": "Ahmedabad", "state": "Gujarat"},
    {"_id": ObjectId("vadodara_city_id"), "name": "Vadodara", "state": "Gujarat"}
])

# Insert publishers data
publishers.insert_many([
    {"_id": ObjectId(), "name": "ABC Publications", "state": "Maharashtra"},
    {"_id": ObjectId(), "name": "XYZ Media", "state": "Gujarat"},
    {"_id": ObjectId(), "name": "PQR News", "state": "Maharashtra"},
    {"_id": ObjectId(), "name": "LMN Publishers", "state": "Gujarat"},
    {"_id": ObjectId(), "name": "RST Media", "state": "Maharashtra"}
])

# Insert newspapers data
newspapers.insert_many([
    {"name": "Times of Nashik", "language": "English", "cityId": ObjectId("nashik_city_id"), "sales": 1000},
    {"name": "Marathi Manoos", "language": "Marathi", "cityId": ObjectId("pune_city_id"), "sales": 1500},
    {"name": "Mumbai Mirror", "language": "English", "cityId": ObjectId("mumbai_city_id"), "sales": 2000},
    {"name": "Ahmedabad Express", "language": "Gujarati", "cityId": ObjectId("ahmedabad_city_id"), "sales": 1200},
    {"name": "Vadodara Chronicle", "language": "Gujarati", "cityId": ObjectId("vadodara_city_id"), "sales": 800},
    {"name": "Deshdoot", "language": "Marathi", "cityId": ObjectId("nashik_city_id"), "sales": 1800},
    {"name": "Lokmat", "language": "Marathi", "cityId": ObjectId("pune_city_id"), "sales": 1600}
])

# Queries

# a. List all newspapers available in "Nashik" city
print("Newspapers available in Nashik:")
for newspaper in newspapers.find({"cityId": ObjectId("nashik_city_id")}):
    print(newspaper["name"])

# b. List all newspapers of "Marathi" language
print("\nMarathi language newspapers:")
for newspaper in newspapers.find({"language": "Marathi"}):
    print(newspaper["name"])

# c. Count the number of publishers in "Gujarat" state
gujarat_publishers_count = publishers.count_documents({"state": "Gujarat"})
print(f"\nNumber of publishers in Gujarat: {gujarat_publishers_count}")

# d. Cursor to show newspapers with the highest sale in Maharashtra State
print("\nNewspapers with highest sales in Maharashtra:")
cursor = newspapers.aggregate([
    {
        "$lookup": {
            "from": "cities",
            "localField": "cityId",
            "foreignField": "_id",
            "as": "city_info"
        }
    },
    {
        "$unwind": "$city_info"
    },
    {
        "$match": {"city_info.state": "Maharashtra"}
    },
    {
        "$sort": {"sales": -1}
    }
])

for doc in cursor:
    print(f"Newspaper: {doc['name']}, Sales: {doc['sales']}")
