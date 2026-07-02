"""
Izgradnja optimizovane, denormalizovane kolekcije `movies_with_stats` iz
postojecih `movies` + `reviews` kolekcija.

Za svaki film se preko $lookup-a spajaju sve njegove recenzije, pa se u istom
dokumentu pred-izracunavaju agregatne vrednosti (broj recenzija, fresh/rotten,
broj top-kriticara, prosecna ocena) i ugnjezdava ceo niz recenzija. Rezultat
se preko $merge upisuje u novu kolekciju `movies_with_stats`.

Ovim sam omogucila da  upiti nad ovom kolekcijom ne moraju vise da rade $lookup ka reviews - sve
sto im treba je vec u dokumentu filma (denormalizacija).

"""

import argparse
from pymongo import MongoClient


def build_pipeline() -> list[dict]:
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
            # Ugnjezdavamo sve recenzije filma, sortirane po datumu (najnovije
            # prve). Nijedan film nema dovoljno recenzija da bi se dokument
            # priblizio MongoDB 16MB limitu (najveci ima oko 600 recenzija),
            # pa limitiranje broja ugnjezdenih recenzija nije potrebno.
            "reviews": {
                "$sortArray": {
                    "input": "$allReviews",
                    "sortBy": {"review_date": -1},
                }
            },
        }},
        {"$project": {
            "allReviews": 0,        # uklanjamo privremeno polje sa lookup-a
            "reviews.ratingKey": 0,  # suvisno u ugnjezdenim recenzijama (isto kao film)
            "reviews._id": 0,
        }},
        {"$merge": {
            "into": "movies_with_stats",
            "on": "ratingKey",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }},
    ]


def build_movies_with_stats(db) -> None:
    print("Pravim movies_with_stats (denormalizacija: ugnjezdene recenzije + agregati)...")
    db.movies.aggregate(build_pipeline())
    count = db.movies_with_stats.count_documents({})
    print(f"Zavrseno. movies_with_stats sada sadrzi {count:,} dokumenata.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="rt_analytics")
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    build_movies_with_stats(client[args.db])