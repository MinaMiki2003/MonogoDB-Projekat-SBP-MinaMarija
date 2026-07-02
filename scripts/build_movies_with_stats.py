"""
Izgradnja optimizovane, denormalizovane kolekcije `movies_with_stats` iz
postojećih `movies` + `reviews` kolekcija
Koristi $merge da rezultat agregacionog pipeline-a upiše direktno u novu kolekciju .

"""
import argparse
from pymongo import MongoClient

# Koliko najnovijih recenzija ugnježdavamo po filmu 
EMBEDDED_REVIEWS_LIMIT = 50


def build_pipeline(embedded_limit: int) -> list[dict]:
    return [
        {"$lookup": {
            "from": "reviews",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "allReviews",
        }},
        {"$addFields": {
            "reviewCount": {"$size": "$allReviews"},
            "freshCount": {
                "$size": {
                    "$filter": {
                        "input": "$allReviews",
                        "cond": {"$eq": ["$$this.review_state", "fresh"]},
                    }
                }
            },
            "rottenCount": {
                "$size": {
                    "$filter": {
                        "input": "$allReviews",
                        "cond": {"$eq": ["$$this.review_state", "rotten"]},
                    }
                }
            },
            "topCriticReviewCount": {
                "$size": {
                    "$filter": {
                        "input": "$allReviews",
                        "cond": {"$eq": ["$$this.top_critic", True]},
                    }
                }
            },
            "avgReviewScore": {"$avg": "$allReviews.review_score"},
            # Ugnježdavamo samo poslednjih N recenzija (sortirano po datumu)
            # da ne bismo probili 16MB limit za blockbuster filmove.
            "reviews": {
                "$sortArray": {
                    "input": "$allReviews",
                    "sortBy": {"review_date": -1},
                }
            },
        }},
        {"$project": {
            "allReviews": 0,  # uklanjamo privremeno polje - ceo skup ostaje u `reviews` kolekciji
            "reviews.ratingKey": 0,
            "reviews._id": 0,
        }},
        {"$merge": {
            "into": "movies_with_stats",
            "on": "ratingKey",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


def build_movies_with_stats(db, embedded_limit: int) -> None:
    print(f"Pravim movies_with_stats (embedded_limit={embedded_limit} recenzija po filmu)...")
    db.movies.aggregate(build_pipeline(embedded_limit))
    count = db.movies_with_stats.count_documents({})
    print(f"Završeno. movies_with_stats sada sadrži {count:,} dokumenata.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="rt_analytics")
    parser.add_argument("--embedded-limit", type=int, default=EMBEDDED_REVIEWS_LIMIT)
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    build_movies_with_stats(client[args.db], args.embedded_limit)
