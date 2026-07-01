"""
Agregacioni upiti - MINA VOJNOVIĆ (uloga: menadžer filmske produkcije)
Šema: NEOPTIMIZOVANA (movies + reviews kolekcije, $lookup za spajanje)

Svaka funkcija vraća (naziv, pipeline, collection_name) - pipeline se izvršava
nad navedenom kolekcijom. Pokretanje i merenje performansi vrši se kroz
scripts/run_performance_analysis.py

NAPOMENA O MERENJU (tačka 6 - uporedna analiza):
Ovi upiti se mere nad NEINDEKSIRANOM bazom (samo podrazumevani _id indeks).
Tek nakon merenja se pokreće create_indexes.py + build_movies_with_stats.py,
pa se mere optimizovane verzije. Time se dobija poštena "pre/posle" slika.
"""

from datetime import datetime

#verzija sa filterom da bi uzeli u obzir samo žanrove koji imaju bar 10 filmova
def q1_genres_best_tomatometer_min50_reviews():
    pipeline = [
        {"$lookup": {
            "from": "reviews",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "movieReviews",
        }},
        {"$project": {
            "genre": 1,
            "tomatoMeter": 1,
            "reviewCount": {"$size": "$movieReviews"},
        }},
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
    return "Mina_Q1_zanrovi_najvisi_tomatometer", pipeline, "movies"

#verzija Q1 bez $addfields bez filtera za >10

# def q1_genres_best_tomatometer_min50_reviews():
#     pipeline = [
#         {"$lookup": {
#             "from": "reviews",
#             "localField": "ratingKey",
#             "foreignField": "ratingKey",
#             "as": "movieReviews",
#         }},
#         {"$project": {
#             "genre": 1,
#             "tomatoMeter": 1,
#             "reviewCount": {"$size": "$movieReviews"},
#         }},
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
#     return "Mina_Q1_zanrovi_najvisi_tomatometer", pipeline, "movies"

# def q1_genres_best_tomatometer_min50_reviews():
#     """
#     PITANJE (uloga: menadžer produkcije): Koji žanrovi filmova imaju najviši
#     prosečan TomatoMeter među filmovima koji imaju najmanje 50 kritičarskih
#     recenzija? (Na koje žanrove se isplati fokusirati produkciju.)

#     USKO GRLO: $lookup nad reviews kolekcijom (1.4M dokumenata) BEZ indeksa na
#     reviews.ratingKey -> za svaki film COLLSCAN cele reviews kolekcije.
#     """
#     pipeline = [
#         {"$lookup": {
#             "from": "reviews",
#             "localField": "ratingKey",
#             "foreignField": "ratingKey",
#             "as": "movieReviews",
#         }},
#         {"$addFields": {"reviewCount": {"$size": "$movieReviews"}}},
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
#     return "Mina_Q1_zanrovi_najvisi_tomatometer", pipeline, "movies"


def q2_publications_activity():
    """
    PITANJE (uloga: menadžer produkcije): Koje publikacije su najaktivnije
    (najviše recenzija) i kakav je prosečan TomatoMeter filmova koje
    recenziraju? (Gde usmeriti PR/marketing budžet.)

    USKO GRLO: $lookup iz reviews (1.4M) ka movies BEZ indeksa na
    movies.ratingKey -> za svaku recenziju COLLSCAN movies kolekcije.
    """
    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "m",
        }},
        {"$unwind": "$m"},
        {"$group": {
            "_id": "$publicatioName",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$review_score"},
            "avgMovieTomato": {"$avg": "$m.tomatoMeter"},
        }},
        {"$match": {"totalReviews": {"$gte": 50}}},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Mina_Q2_publikacije_aktivnost", pipeline, "reviews"


def q3_top_critics_vs_regular_deviation():
    """
    PITANJE (uloga: menadžer produkcije): Da li top kritičari više odstupaju
    od mišljenja publike nego regularni kritičari? (Koliko verovati
    kritičarskim ocenama pri proceni tržišnog prijema filma.)

    USKO GRLO: $lookup iz reviews (1.4M) ka movies BEZ indeksa.
    """
    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "movieInfo",
        }},
        {"$unwind": "$movieInfo"},
        {"$project": {
            "top_critic": 1,
            "deviation": {
                "$abs": {
                    "$subtract": [
                        {"$multiply": ["$review_score", 100]},
                        "$movieInfo.audienceScore",
                    ]
                }
            },
        }},
        {"$group": {
            "_id": "$top_critic",
            "avgDeviation": {"$avg": "$deviation"},
            "reviewCount": {"$sum": 1},
        }},
        {"$sort": {"avgDeviation": -1}},
    ]
    return "Mina_Q3_top_kriticari_odstupanje", pipeline, "reviews"


def q4_review_scores_by_decade():
    """
    PITANJE (uloga: menadžer produkcije): Kako se prosečna ocena recenzija i
    aktivnost kritičara menjaju po decenijama izlaska filma? (Da li su stariji
    filmovi bolje/lošije ocenjeni i koliko ih kritičari uopšte prate.)

    USKO GRLO: $lookup iz reviews (1.4M) ka movies BEZ indeksa.
    """
    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "m",
        }},
        {"$unwind": "$m"},
        {"$match": {"m.releaseYear": {"$gt": 0}}},
        {"$group": {
            "_id": {"$subtract": ["$m.releaseYear", {"$mod": ["$m.releaseYear", 10]}]},
            "avgReviewScore": {"$avg": "$review_score"},
            "totalReviews": {"$sum": 1},
            "topCriticReviews": {"$sum": {"$cond": ["$top_critic", 1, 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]
    return "Mina_Q4_ocene_po_deceniji", pipeline, "reviews"

def q5_critic_audience_divergence():
    """
    PITANJE : Koji filmovi najviše dele kritiku i
    publiku - najveći raskorak između tomatoMeter (kritičari) i audienceScore
    (publika)? Korisno za procenu marketinškog rizika.

    """
    pipeline = [
        {"$lookup": {
            "from": "reviews",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "movieReviews",
        }},
        {"$addFields": {"reviewCount": {"$size": "$movieReviews"}}},
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
    return "Mina_Q5_raskorak_kritika_publika", pipeline, "movies"

#OVA VERZIJA Q5 UPITA JE SLOZENIJA ALI MALA OPTIMIZACIJA SA 13 SEK NA 8 NE MOZE BOLJE
# def q5_critic_activity_by_month():
#     """
#     PITANJE (uloga: menadžer produkcije): Koji meseci beleže najveću aktivnost
#     kritičara i kakav je prosečan TomatoMeter filmova recenziranih tih meseci?
#     (Kad je medijska pažnja najveća - kada planirati izlazak filma.)

#     FAZE PIPELINE-A:
#       1. $project - izdvoji mesec iz review_date ($month operator)
#       2. $group   - grupiši recenzije po mesecu: broj recenzija (aktivnost) +
#                     skup JEDINSTVENIH ratingKey vrednosti filmova recenziranih
#                     tog meseca ($addToSet)
#       3. $lookup  - spoji sa `movies` kolekcijom da dobiješ tomatoMeter za sve
#                     filmove recenzirane u datom mesecu (localField je NIZ
#                     ratingKey vrednosti - "array to array" $lookup spajanje)
#       4. $project - prosečan tomatoMeter DISTINKTNIH filmova tog meseca
#       5. $sort    - opadajuće po broju recenzija (aktivnosti)

#     SLOŽENOST / USKO GRLO: array-to-array $lookup (foreignField se poredi sa
#     SVAKIM elementom niza iz localField) je skuplji od 1:1 lookup-a - MongoDB
#     radi $in upit za svaki od 12 grupisanih dokumenata (meseci), nad celom
#     `movies` kolekcijom. BEZ indeksa na movies.ratingKey -> 12 COLLSCAN-ova.
#     """
#     pipeline = [
#         {"$project": {
#             "ratingKey": 1,
#             "month": {"$month": "$review_date"},
#         }},
#         {"$group": {
#             "_id": "$month",
#             "reviewCount": {"$sum": 1},
#             "ratingKeys": {"$addToSet": "$ratingKey"},
#         }},
#         {"$lookup": {
#             "from": "movies",
#             "localField": "ratingKeys",
#             "foreignField": "ratingKey",
#             "as": "moviesThatMonth",
#         }},
#         {"$project": {
#             "_id": 0,
#             "month": "$_id",
#             "reviewCount": 1,
#             "avgTomatoMeterOfMovies": {"$avg": "$moviesThatMonth.tomatoMeter"},
#         }},
#         {"$sort": {"reviewCount": -1}},
#     ]
#     return "Mina_Q5_aktivnost_kriticara_po_mesecu", pipeline, "reviews"


ALL_QUERIES = [
    q1_genres_best_tomatometer_min50_reviews,
    q2_publications_activity,
    q3_top_critics_vs_regular_deviation,
    q4_review_scores_by_decade,
    q5_critic_audience_divergence,
]