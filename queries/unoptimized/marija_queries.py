"""
Agregacioni upiti - MARIJA OLIĆ (uloga: analitičar podataka)
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


def q1_studios_activity_quality():
    """
    PITANJE (uloga: analitičar podataka): Koji studiji imaju najviše
    kritičarskih recenzija i kakav im je prosečan review_score i TomatoMeter?
    (Analiza izvora - koji studio dominira u kritičarskoj pažnji i kvalitetu.)

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
            "_id": "$m.studio",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$review_score"},
            "avgTomatoMeter": {"$avg": "$m.tomatoMeter"},
        }},
        {"$match": {"totalReviews": {"$gte": 100}}},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Marija_Q1_studiji_aktivnost_kvalitet", pipeline, "reviews"


def q2_reception_by_mpaa_rating():
    """
    PITANJE (uloga: analitičar podataka): Kako se broj recenzija i prosečne
    ocene (review_score, TomatoMeter, audienceScore) razlikuju po MPAA
    rating-u filma (G, PG, PG-13, R)? (Segmentacija prijema po uzrasnoj
    kategoriji.)

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
        {"$group": {
            "_id": "$m.rating",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$review_score"},
            "avgTomatoMeter": {"$avg": "$m.tomatoMeter"},
            "avgAudienceScore": {"$avg": "$m.audienceScore"},
        }},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Marija_Q2_prijem_po_mpaa_ratingu", pipeline, "reviews"


def q3_strictest_critics():
    """
    PITANJE (uloga: analitičar podataka): Koji kritičari su "najstroži"
    (najniži prosečan review_score) među onima sa bar 50 recenzija, i kakav
    je prosečan TomatoMeter filmova koje recenziraju? (Profilisanje izvora -
    identifikacija sistematski strogih/blagih kritičara.)

    USKO GRLO: $lookup iz reviews (1.4M) ka movies BEZ indeksa.
    NAPOMENA: u optimizovanoj verziji ovaj upit NAMERNO zadržava $lookup kao
    demonstraciju spajanja kolekcija.
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
            "_id": "$critic_name",
            "reviewCount": {"$sum": 1},
            "avgReviewScore": {"$avg": "$review_score"},
            "avgMovieTomato": {"$avg": "$m.tomatoMeter"},
        }},
        {"$match": {"reviewCount": {"$gte": 50}}},
        {"$sort": {"avgReviewScore": 1}},
    ]
    return "Marija_Q3_najstroziji_kriticari", pipeline, "reviews"


def q4_sentiment_vs_score():
    """
    PITANJE (uloga: analitičar podataka): Koliko se oznaka scoreSentiment
    (POSITIVE/NEGATIVE) poklapa sa stvarnim review_score-om i TomatoMeter-om
    filmova? (Kontrola kvaliteta podataka - da li sentiment oznaka odgovara
    numeričkoj oceni.)

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
        {"$group": {
            "_id": "$scoreSentiment",
            "totalReviews": {"$sum": 1},
            "avgReviewScore": {"$avg": "$review_score"},
            "avgTomatoMeter": {"$avg": "$m.tomatoMeter"},
        }},
        {"$sort": {"totalReviews": -1}},
    ]
    return "Marija_Q4_sentiment_vs_ocena", pipeline, "reviews"


def q5_quality_distribution_bucket():
    """
    PITANJE (uloga: analitičar podataka): Kako su filmovi raspoređeni po
    opsezima TomatoMeter-a (0-20, 20-40, 40-60, 60-80, 80-100), i kakav je
    prosečan audienceScore i broj recenzija u svakom opsegu? (Statistička
    distribucija kvaliteta - $bucket analiza.)

    USKO GRLO: $lookup iz movies (143k) ka reviews (1.4M) radi brojanja
    recenzija po filmu BEZ indeksa na reviews.ratingKey.
    """
    pipeline = [
        {"$lookup": {
            "from": "reviews",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "movieReviews",
        }},
        {"$addFields": {"reviewCount": {"$size": "$movieReviews"}}},
        {"$match": {"reviewCount": {"$gte": 10}, "tomatoMeter": {"$ne": None}}},
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
    return "Marija_Q5_distribucija_kvaliteta", pipeline, "movies"


ALL_QUERIES = [
    q1_studios_activity_quality,
    q2_reception_by_mpaa_rating,
    q3_strictest_critics,
    q4_sentiment_vs_score,
    q5_quality_distribution_bucket,
]