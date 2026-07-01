"""
Agregacioni upiti - MARIJA OLIĆ - OPTIMIZOVANA šema, verzija sa INDEKSIMA.

Svaki upit (osim Q4 koji namerno zadržava $lookup kao demonstraciju spajanja
kolekcija) POČINJE sa $match nad INDEKSIRANIM poljem -> IXSCAN.
"""

from datetime import datetime, timedelta

CERTIFIED_FRESH_THRESHOLD = 75


def q1_studios_highest_audience_lowest_variance(min_movies: int = 20):
    """
    KOLEKCIJA: movies_slim (vitka kolekcija BEZ ugnježdenih recenzija).
    INDEKS: { audienceScore: 1 } nad movies_slim.
    Pošto movies_slim dokumenti NE sadrže težak reviews niz, čitanje je
    brzo - ovaj upit je sada BRŽI od neoptimizovane verzije. $match
    {audienceScore >= 0} PRVA faza -> IXSCAN.
    """
    pipeline = [
        {"$match": {"audienceScore": {"$gte": 0}, "studio": {"$ne": "Unknown"}}},
        {"$group": {
            "_id": "$studio",
            "avgAudienceScore": {"$avg": "$audienceScore"},
            "stdDev": {"$stdDevPop": "$audienceScore"},
            "movieCount": {"$sum": 1},
        }},
        {"$match": {"movieCount": {"$gte": min_movies}}},
        {"$project": {"_id": 0, "studio": "$_id", "avgAudienceScore": 1,
                       "stdDev": 1, "movieCount": 1}},
        {"$sort": {"avgAudienceScore": -1, "stdDev": 1}},
    ]
    return "Marija_Q1_OPT_studiji_stabilnost", pipeline, "movies_slim"


def q2_biggest_gap_movies_by_genre(limit: int = 100):
    """
    KOLEKCIJA: movies_slim (vitka kolekcija BEZ ugnježdenih recenzija).
    INDEKS: { tomatoMeter: 1 } nad movies_slim. Pošto dokumenti NE sadrže
    težak reviews niz, čitanje je brzo -> BRŽE od neoptimizovane verzije.
    $match {tomatoMeter >= 0} PRVA faza -> IXSCAN.
    """
    pipeline = [
        {"$match": {"tomatoMeter": {"$gt": 0}, "audienceScore": {"$gt": 0}}},
        {"$project": {
            "_id": 0, "movie_title": 1, "genre": 1, "tomatoMeter": 1,
            "audienceScore": 1,
            "gap": {"$abs": {"$subtract": ["$tomatoMeter", "$audienceScore"]}},
        }},
        {"$sort": {"gap": -1}},
        {"$limit": limit},
        {"$facet": {
            "topControversialMovies": [
                {"$project": {"movie_title": 1, "gap": 1,
                               "tomatoMeter": 1, "audienceScore": 1}},
            ],
            "genreDistribution": [
                {"$unwind": "$genre"},
                {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ],
        }},
    ]
    return "Marija_Q2_OPT_najveci_raskorak_po_zanru", pipeline, "movies_slim"


def q3_certified_fresh_effect_by_genre():
    """
    INDEKS: { tomatoMeter: 1 }
    $match {tomatoMeter != null} PRVA faza -> IXSCAN (već je radilo i ranije).
    """
    pipeline = [
        {"$match": {"tomatoMeter": {"$gt": 0}, "audienceScore": {"$gt": 0}}},
        {"$addFields": {
            "certifiedFresh": {"$gte": ["$tomatoMeter", CERTIFIED_FRESH_THRESHOLD]}
        }},
        {"$unwind": "$genre"},
        {"$facet": {
            "byGenre": [
                {"$group": {
                    "_id": {"genre": "$genre", "certifiedFresh": "$certifiedFresh"},
                    "avgAudienceScore": {"$avg": "$audienceScore"},
                    "movieCount": {"$sum": 1},
                }},
                {"$sort": {"_id.genre": 1, "_id.certifiedFresh": -1}},
            ],
            "overall": [
                {"$group": {
                    "_id": "$certifiedFresh",
                    "avgAudienceScore": {"$avg": "$audienceScore"},
                    "movieCount": {"$sum": 1},
                }},
            ],
        }},
    ]
    return "Marija_Q3_OPT_certified_fresh_efekat", pipeline, "movies_with_stats"


def q4_fresh_rotten_distribution_by_rating_and_genre():
    """
    NAMERNO ZADRŽAVA $lookup - demonstracija spajanja kolekcija (movies_with_stats
    + reviews). Ovde se NE očekuje IXSCAN na glavnoj kolekciji - svesna odluka.
    $lookup interno koristi indeks na reviews.ratingKey (foreignField).
    """
    pipeline = [
        {"$lookup": {
            "from": "reviews",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "joinedReviews",
        }},
        {"$unwind": "$joinedReviews"},
        {"$unwind": "$genre"},
        {"$facet": {
            "byRating": [
                {"$group": {
                    "_id": {"rating": "$rating", "state": "$joinedReviews.review_state"},
                    "count": {"$sum": 1},
                }},
            ],
            "byGenre": [
                {"$group": {
                    "_id": {"genre": "$genre", "state": "$joinedReviews.review_state"},
                    "count": {"$sum": 1},
                }},
            ],
        }},
    ]
    return "Marija_Q4_OPT_fresh_rotten_po_ratingu_i_zanru_LOOKUP", pipeline, "movies_with_stats"


def q5_publishers_deviating_from_consensus(years_back: int = 10, reference_date=None):
    """
    INDEKS: { tomatoMeter: 1 }
    $match {tomatoMeter >= 0} PRVA faza -> IXSCAN. Odstupanje od konsenzusa
    (tomatoMeter) ima smisla samo za filmove sa poznatim konsenzusom.
    Filter po datumu recenzije se primenjuje nakon $unwind nad ugnježdenim nizom.
    """
    if reference_date is None:
        reference_date = datetime.utcnow()
    cutoff = reference_date - timedelta(days=365 * years_back)

    pipeline = [
        {"$match": {"tomatoMeter": {"$gte": 0}}},
        {"$unwind": "$reviews"},
        {"$match": {"reviews.review_date": {"$gte": cutoff}}},
        {"$project": {
            "publisher_name": "$reviews.publisher_name",
            "deviation": {
                "$abs": {"$subtract": [
                    {"$multiply": ["$reviews.review_score", 100]},
                    "$tomatoMeter",
                ]}
            },
        }},
        {"$group": {
            "_id": "$publisher_name",
            "avgDeviation": {"$avg": "$deviation"},
            "reviewCount": {"$sum": 1},
        }},
        {"$match": {"reviewCount": {"$gte": 20}}},
        {"$sort": {"avgDeviation": -1}},
    ]
    return "Marija_Q5_OPT_publikacije_odstupanje", pipeline, "movies_with_stats"


ALL_QUERIES = [
    q1_studios_highest_audience_lowest_variance,
    q2_biggest_gap_movies_by_genre,
    q3_certified_fresh_effect_by_genre,
    q4_fresh_rotten_distribution_by_rating_and_genre,
    q5_publishers_deviating_from_consensus,
]
