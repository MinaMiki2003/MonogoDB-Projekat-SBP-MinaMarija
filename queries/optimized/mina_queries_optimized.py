
from datetime import datetime, timedelta


def q1_genres_best_tomatometer_min50_reviews():
    """
    INDEKS: { reviewCount: 1 }
    $match {reviewCount >= 50} PRVA faza -> IXSCAN na reviewCount.
    """
    pipeline = [
        {"$match": {"reviewCount": {"$gte": 50}}},
        {"$unwind": "$genre"},
        {"$group": {
            "_id": "$genre",
            "avgTomatoMeter": {"$avg": "$tomatoMeter"},
            "movieCount": {"$sum": 1},
        }},
        {"$match": {"movieCount": {"$gte": 10}}},
        {"$project": {"_id": 0, "genre": "$_id", "avgTomatoMeter": 1, "movieCount": 1}},
        {"$sort": {"avgTomatoMeter": -1}},
    ]
    return "Mina_Q1_OPT_zanrovi_najvisi_tomatometer", pipeline, "movies_with_stats"
#zakoment verzija bez filter
# def q1_genres_best_tomatometer_min50_reviews():
#     """
#     INDEKS: { reviewCount: 1 }
#     $match {reviewCount >= 50} PRVA faza -> IXSCAN na reviewCount.
#     """
#     pipeline = [
#         {"$match": {"reviewCount": {"$gte": 50}}},
#         {"$unwind": "$genre"},
#         {"$group": {
#             "_id": "$genre",
#             "avgTomatoMeter": {"$avg": "$tomatoMeter"},
#             "movieCount": {"$sum": 1},
#         }},
#         {"$project": {"_id": 0, "genre": "$_id", "avgTomatoMeter": 1, "movieCount": 1}},
#         {"$sort": {"avgTomatoMeter": -1}},
#     ]
#     return "Mina_Q1_OPT_zanrovi_najvisi_tomatometer", pipeline, "movies_with_stats"

# def q2_publications_activity():
#     """
#     OPTIMIZOVANI Q2: nad movies_with_stats. $unwind ugnježdenih recenzija pa
#     grupisanje po publikaciji - bez $lookup-a.
#     """
#     pipeline = [
#         {"$unwind": "$reviews"},
#         {"$group": {
#             "_id": "$reviews.publicatioName",
#             "totalReviews": {"$sum": 1},
#             "avgReviewScore": {"$avg": "$reviews.review_score"},
#         }},
#         {"$match": {"totalReviews": {"$gte": 50}}},
#         {"$sort": {"totalReviews": -1}},
#     ]
#     return "Mina_Q2_OPT_publikacije_aktivnost", pipeline, "movies_with_stats"

def q2_publications_activity():
    """
    OPTIMIZOVANI Q2: nad movies_with_stats (ugnježdene recenzije). $unwind
    reviews pa grupisanje po publikaciji - bez $lookup-a.
    """
    pipeline = [
        {"$match": {"reviewCount": {"$gt": 0}}},
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": "$reviews.publicatioName",
            "totalReviews": {"$sum": 1},
           # "avgReviewScore": {"$avg": "$reviews.review_score"},
           # "avgMovieTomato": {"$avg": "$tomatoMeter"},
        }},
        {"$match": {"totalReviews": {"$gte": 50}}},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Mina_Q2_OPT_publikacije_aktivnost", pipeline, "movies_with_stats"


def q3_top_critics_vs_regular_deviation():
    """
    OPTIMIZOVANI Q3: koristi ugnježdene recenzije umesto $lookup-a.
    $match filtrira valjane review_score (0-1) pre računanja odstupanja.
    """
    pipeline = [
        {"$unwind": "$reviews"},
        {"$match": {"reviews.review_score": {"$gte": 0, "$lte": 1}}},
        {"$group": {
            "_id": "$reviews.top_critic",
            "avgDeviation": {
                "$avg": {
                    "$abs": {"$subtract": [
                        {"$multiply": ["$reviews.review_score", 100]},
                        "$audienceScore",
                    ]}
                }
            },
            "reviewCount": {"$sum": 1},
        }},
        {"$sort": {"avgDeviation": -1}},
    ]
    return "Mina_Q3_OPT_top_kriticari_odstupanje", pipeline, "movies_with_stats"

# def q3_top_critics_vs_regular_deviation():
#     """
#   # Stara verzija sa $lookup-om - demonstrira indeksiranje na spajanju preko
#     reviews.ratingKey, ali je bila ~100s pa sam presla na denormalizaciju gore.


#     pipeline = [
#         {"$match": {"reviewCount": {"$gt": 0}}},
#         {"$unwind": "$reviews"},
#         {"$match": {"reviews.review_score": {"$gte": 0, "$lte": 1}}},
#         {"$project": {
#             "top_critic": "$reviews.top_critic",
#             "deviation": {
#                 "$abs": {"$subtract": [
#                     {"$multiply": ["$reviews.review_score", 100]},
#                     "$audienceScore",
#                 ]}
#             },
#         }},
#         {"$group": {
#             "_id": "$top_critic",
#             "avgDeviation": {"$avg": "$deviation"},
#             "reviewCount": {"$sum": 1},
#         }},
#         {"$sort": {"avgDeviation": -1}},
#     ]
#     return "Mina_Q3_OPT_top_kriticari_odstupanje", pipeline, "movies_with_stats"

#jako sporo izvrsava zbog lookapa nedovoljna optimizacjia
# def q3_top_critics_vs_regular_deviation():
#     """
#     NAMERNO ZADRŽAVA $lookup - demonstracija spajanja kolekcija (movies_with_stats
#     + reviews). Ovde se NE očekuje IXSCAN na glavnoj kolekciji - svesna odluka.
#     $lookup interno koristi indeks na reviews.ratingKey (foreignField).
#     """
#     pipeline = [
#         {"$lookup": {
#             "from": "reviews",
#             "localField": "ratingKey",
#             "foreignField": "ratingKey",
#             "as": "joinedReviews",
#         }},
#         {"$unwind": "$joinedReviews"},
#         {"$project": {
#             "top_critic": "$joinedReviews.top_critic",
#             "deviation": {
#                 "$abs": {"$subtract": [
#                     {"$multiply": ["$joinedReviews.review_score", 100]},
#                     "$audienceScore",
#                 ]}
#             },
#         }},
#         {"$group": {
#             "_id": "$top_critic",
#             "avgDeviation": {"$avg": "$deviation"},
#             "reviewCount": {"$sum": 1},
#         }},
#         {"$sort": {"avgDeviation": -1}},
#     ]
#     return "Mina_Q3_OPT_top_kriticari_odstupanje_LOOKUP", pipeline, "movies_with_stats"


def q4_review_scores_by_decade():
    """
    OPTIMIZOVANI Q4: nad movies_with_stats. $unwind reviews, grupisanje po
    deceniji izlaska - bez $lookup-a.
    """
    pipeline = [
        {"$match": {"releaseYear": {"$gt": 0}}},
        {"$unwind": "$reviews"},
        {"$group": {
            "_id": {"$subtract": ["$releaseYear", {"$mod": ["$releaseYear", 10]}]},
            "avgReviewScore": {"$avg": "$reviews.review_score"},
            "totalReviews": {"$sum": 1},
            "topCriticReviews": {"$sum": {"$cond": ["$reviews.top_critic", 1, 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]
    return "Mina_Q4_OPT_ocene_po_deceniji", pipeline, "movies_with_stats"

def q5_critic_audience_divergence():
    """
    OPTIMIZOVANI Q5: nad movies_with_stats. Oba polja (tomatoMeter, audienceScore)
    i reviewCount su VEĆ u dokumentu - nema $lookup-a, nema $unwind-a. Samo $match
    (koristi indeks) + $project + $sort. 
    čita se po jedan dokument po filmu, sve potrebne vrednosti su denormalizovane.
    """
    pipeline = [
        {"$match": {
            "tomatoMeter": {"$ne": None},
            "audienceScore": {"$ne": None},
            "reviewCount": {"$gte": 20},
        }},
        {"$project": {
            "_id": 0,
            "movie_title": 1,
            "tomatoMeter": 1,
            "audienceScore": 1,
            "reviewCount": 1,
            "divergence": {"$abs": {"$subtract": ["$tomatoMeter", "$audienceScore"]}},
        }},
        {"$sort": {"divergence": -1}},
        {"$limit": 20},
    ]
    return "Mina_Q5_OPT_raskorak_kritika_publika", pipeline, "movies_with_stats"

# def q5_critic_activity_by_month():
#     """
#     OPTIMIZOVANI Q5: nad movies_with_stats (ugnježdene recenzije), bez $lookup-a.
#     avgTomatoMeterOfMovies je ovde PONDERISAN po broju recenzija (prosek po
#     recenziji, ne po distinktnom filmu) - to je svesna razlika u odnosu na
#     neoptimizovanu verziju, prihvaćena radi performansi (jednostruko grupisanje).
#     """
#     pipeline = [
#         {"$match": {"reviewCount": {"$gt": 0}}},
#         {"$unwind": "$reviews"},
#         {"$project": {
#             "tomatoMeter": 1,
#             "month": {"$month": "$reviews.review_date"},
#         }},
#         {"$group": {
#             "_id": "$month",
#             "reviewCount": {"$sum": 1},
#             "avgTomatoMeterOfMovies": {"$avg": "$tomatoMeter"},
#         }},
#         {"$project": {"_id": 0, "month": "$_id", "reviewCount": 1, "avgTomatoMeterOfMovies": 1}},
#         {"$sort": {"reviewCount": -1}},
#     ]
#     return "Mina_Q5_OPT_aktivnost_kriticara_po_mesecu", pipeline, "movies_with_stats"

ALL_QUERIES = [
    q1_genres_best_tomatometer_min50_reviews,
    q2_publications_activity,
    q3_top_critics_vs_regular_deviation,
    q4_review_scores_by_decade,
    q5_critic_audience_divergence,
]