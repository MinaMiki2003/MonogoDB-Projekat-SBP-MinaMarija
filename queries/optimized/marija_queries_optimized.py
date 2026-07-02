"""
Agregacioni upiti - MARIJA OLIĆ (uloga: analitičar podataka)
Šema: OPTIMIZOVANA (movies_with_stats, denormalizovana + indeksi)

RAZNOVRSNOST OPTIMIZACIJA - svaki upit demonstrira DRUGAČIJI koncept:
  Q1 - COMPOUND indeks (ESR pravilo): {studio:1, reviewCount:-1}
  Q2 - SINGLE-FIELD indeks: {rating:1}
  Q3 - $lookup + indeks na foreignField (reviews.ratingKey) - demo spajanja
  Q4 - MULTIKEY indeks nad UGNJEŽDENIM poljem: {reviews.scoreSentiment:1}
  Q5 - PARTIAL indeks (indeks samo nad podskupom) + $bucket
"""

from datetime import datetime


def q1_studios_activity_quality():
    """
    OPTIMIZACIJA: COMPOUND indeks {studio:1, reviewCount:-1} (ESR pravilo).
    $match {studio != Unknown, reviewCount > 0} PRVA faza -> IXSCAN preko
    compound indeksa. E (equality) = studio, R (range) = reviewCount.
    Grupiše ugnježdene recenzije po studiju.
    """
    pipeline = [
        {"$match": {"studio": {"$ne": "Unknown"}, "reviewCount": {"$gt": 0}}},
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": "$studio",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$reviews.review_score"},
            "avgTomatoMeter": {"$avg": "$tomatoMeter"},
        }},
        {"$match": {"totalReviews": {"$gte": 100}}},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Marija_Q1_OPT_studiji_aktivnost_kvalitet", pipeline, "movies_with_stats"


def q2_reception_by_mpaa_rating():
    """
    OPTIMIZACIJA: SINGLE-FIELD indeks {rating:1}.
    $match {rating != null} PRVA faza -> IXSCAN preko rating indeksa.
    Grupiše po MPAA rating-u iz ugnježdenih recenzija.
    """
    pipeline = [
        {"$match": {"rating": {"$ne": None}}},
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": "$rating",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$reviews.review_score"},
            "avgTomatoMeter": {"$avg": "$tomatoMeter"},
            "avgAudienceScore": {"$avg": "$audienceScore"},
        }},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Marija_Q2_OPT_prijem_po_mpaa_ratingu", pipeline, "movies_with_stats"


def q3_strictest_critics():
    """
    OPTIMIZACIJA: $lookup + indeks na foreignField (reviews.ratingKey).
    NAMERNO zadržava $lookup kao demonstraciju spajanja kolekcija. Na glavnoj
    kolekciji je COLLSCAN (svesna odluka), ali $lookup interno koristi indeks
    na reviews.ratingKey - vidi se po totalKeysExamined > 0.
    """
    pipeline = [
        {"$lookup": {
            "from": "reviews",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "joinedReviews",
        }},
        {"$unwind": "$joinedReviews"},
        {"$group": {
            "_id": "$joinedReviews.critic_name",
            "reviewCount": {"$sum": 1},
            "avgReviewScore": {"$avg": "$joinedReviews.review_score"},
            "avgMovieTomato": {"$avg": "$tomatoMeter"},
        }},
        {"$match": {"reviewCount": {"$gte": 50}}},
        {"$sort": {"avgReviewScore": 1}},
    ]
    return "Marija_Q3_OPT_najstroziji_kriticari_LOOKUP", pipeline, "movies_with_stats"


def q4_sentiment_vs_score():
    """
    OPTIMIZACIJA: MULTIKEY indeks nad UGNJEŽDENIM poljem {reviews.scoreSentiment:1}.
    $match {reviews.scoreSentiment postoji} PRVA faza -> IXSCAN preko multikey
    indeksa nad nizom recenzija. Grupiše po scoreSentiment oznaci.
    """
    pipeline = [
        {"$match": {"reviews.scoreSentiment": {"$exists": True}}},
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": "$reviews.scoreSentiment",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$reviews.review_score"},
            "avgTomatoMeter": {"$avg": "$tomatoMeter"},
        }},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Marija_Q4_OPT_sentiment_vs_ocena", pipeline, "movies_with_stats"


def q5_quality_distribution_bucket():
    """
    OPTIMIZACIJA: PARTIAL indeks {tomatoMeter:1} (indeks samo nad dokumentima
    sa reviewCount >= 10) + $bucket. Partial indeks je manji (indeksira samo
    filmove sa dovoljno recenzija), pa je i brži. $match {reviewCount >= 10}
    -> IXSCAN preko partial indeksa, potom $bucket po opsezima tomatoMeter-a.
    """
    pipeline = [
        {"$match": {"reviewCount": {"$gte": 10}}},
        {"$bucket": {
            "groupBy": "$tomatoMeter",
            "boundaries": [0, 20, 40, 60, 80, 101],
            "default": "Ostalo",
            "output": {
                "movieCount": {"$sum": 1},
                "avgAudienceScore": {"$avg": "$audienceScore"},
                "avgReviewCount": {"$avg": "$reviewCount"},
            },
        }},
    ]
    return "Marija_Q5_OPT_distribucija_kvaliteta", pipeline, "movies_with_stats"


ALL_QUERIES = [
    q1_studios_activity_quality,
    q2_reception_by_mpaa_rating,
    q3_strictest_critics,
    q4_sentiment_vs_score,
    q5_quality_distribution_bucket,
]