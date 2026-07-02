from pymongo import MongoClient
db = MongoClient("mongodb://localhost:27017")["rt_analytics"]

# uzmi jedan film i pogledaj strukturu
doc = db.movies_with_stats.find_one({"reviewCount": {"$gt": 50}})
print("Polja:", list(doc.keys()))
print("reviewCount:", doc.get("reviewCount"))
print("freshCount:", doc.get("freshCount"))
print("rottenCount:", doc.get("rottenCount"))
print("avgReviewScore:", doc.get("avgReviewScore"))
print("Broj ugnjezdenih recenzija:", len(doc.get("reviews", [])))