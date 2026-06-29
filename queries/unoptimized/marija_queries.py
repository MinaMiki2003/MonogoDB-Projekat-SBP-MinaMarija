"""
Agregacioni upiti - MARIJA OLIĆ (uloga: analitičar podataka)
Šema: NEOPTIMIZOVANA (movies + reviews kolekcije, $lookup za spajanje)
"""

from datetime import datetime, timedelta

# Pretpostavka korišćena u Q3: Rotten Tomatoes na sajtu dodeljuje status
# "Certified Fresh" filmovima sa tomatoMeter >= 75 koji imaju dovoljan broj
# recenzija. Pošto dataset ne sadrži eksplicitnu kolonu certifiedFresh,
# status se IZVODI iz tomatoMeter vrednosti - ovo je dokumentovana
# pretpostavka, ne greška u podacima.
CERTIFIED_FRESH_THRESHOLD = 75


def q1_studios_highest_audience_lowest_variance(min_movies: int = 20):
    """
    PITANJE: Koji filmski studiji imaju najviše prosečne AudienceScore
    vrednosti i najmanju varijaciju rezultata (minimum 20 filmova)?

    FAZE PIPELINE-A:
      1. $group  - grupiši filmove po studiju (`studio`): prosek
                   audienceScore, $stdDevPop (populaciona standardna
                   devijacija - koristimo Pop varijantu jer radimo nad
                   CELOM populacijom filmova tog studija u datasetu, ne
                   nad uzorkom), broj filmova
      2. $match  - zadrži samo studije sa >= min_movies filmova (mali
                   studiji sa 1-2 filma bi imali varijansu 0 i lažno
                   "pobedili")
      3. $sort   - sortiraj po kombinaciji visok prosek + niska varijansa

    OČEKIVANI REZULTAT: lista {studio, avgAudienceScore, stdDev, movieCount}.

    SLOŽENOST: Jednostavan $group nad `movies` (143k dok.) - O(M). Bez
    indeksa na `studio`, $group i dalje mora pročitati sve dokumente
    (agregacija po definiciji prolazi kroz sve kandidate iz prethodne
    faze), pa indeks na `studio` ovde NE smanjuje totalDocsExamined u
    samom $group, ali POMAŽE ako se ispred njega doda $match na studio
    u optimizovanoj verziji upita.
    """
    pipeline = [
        {"$group": {
            "_id": "$studio",
            "avgAudienceScore": {"$avg": "$audienceScore"},
            "stdDev": {"$stdDevPop": "$audienceScore"},
            "movieCount": {"$sum": 1},
        }},
        {"$match": {"movieCount": {"$gte": min_movies}}},
        {"$project": {
            "_id": 0, "studio": "$_id", "avgAudienceScore": 1,
            "stdDev": 1, "movieCount": 1,
        }},
        {"$sort": {"avgAudienceScore": -1, "stdDev": 1}},
    ]
    return "Marija_Q1_studiji_stabilnost", pipeline, "movies"


def q2_biggest_gap_movies_by_genre(limit: int = 100):
    """
    PITANJE: Koji filmovi imaju najveći raskorak između TomatoMeter i
    AudienceScore i kojim žanrovima najčešće pripadaju?

    FAZE PIPELINE-A:
      1. $project - izračunaj `gap` = |tomatoMeter - audienceScore|
      2. $sort    - sortiraj opadajuće po gap
      3. $limit   - zadrži samo TOP N najkontroverznijih filmova
      4. $group   - (nakon $unwind genre niza) prebroj učestalost žanra
                    među tih N filmova - identifikacija žanra koji
                    "polarizuje" kritiku i publiku

    OČEKIVANI REZULTAT: (a) top N kontroverznih filmova sa gap vrednošću i
    (b) raspodela žanrova među njima.

    SLOŽENOST: $sort BEZ indeksa na izračunato polje `gap` zahteva sortiranje
    SVIH 143k dokumenata u memoriji (blockingSort, limit na MongoDB
    32MB sort buffer-u) pre $limit faze - ovo je čest uzrok upozorenja
    "Sort exceeded memory limit" na velikim kolekcijama. $limit se ne može
    "pomeriti ispred" $sort jer bi promenio semantiku upita.
    """
    pipeline = [
        {"$project": {
            "movie_title": 1,
            "genre": 1,
            "tomatoMeter": 1,
            "audienceScore": 1,
            "gap": {"$abs": {"$subtract": ["$tomatoMeter", "$audienceScore"]}},
        }},
        {"$sort": {"gap": -1}},
        {"$limit": limit},
        {"$facet": {
            "topControversialMovies": [
                {"$project": {"_id": 0, "movie_title": 1, "gap": 1,
                               "tomatoMeter": 1, "audienceScore": 1}},
            ],
            "genreDistribution": [
                {"$unwind": "$genre"},
                {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ],
        }},
    ]
    return "Marija_Q2_najveci_raskorak_po_zanru", pipeline, "movies"


def q3_certified_fresh_effect_by_genre():
    """
    PITANJE: Da li Certified Fresh status ima isti efekat na AudienceScore
    u svim žanrovima?

    FAZE PIPELINE-A:
      1. $match   - zadrži filmove sa definisanim tomatoMeter i genre
      2. $addFields - izvedi boolean `certifiedFresh` (tomatoMeter >= 75)
      3. $unwind  - razloži genre niz
      4. $facet   - paralelno izračunaj: (a) avg audienceScore za
                    certifiedFresh=true/false PO ŽANRU i (b) isto, ali
                    GLOBALNO (svi žanrovi zajedno) - radi poređenja da li
                    se neki žanr značajno razlikuje od globalnog efekta
      5. $group   - (unutar facet-a) agregacija po (genre, certifiedFresh)
                    odnosno samo po certifiedFresh za globalni pogled

    OČEKIVANI REZULTAT: { byGenre: [{genre, certifiedFresh, avgAudience}],
    overall: [{certifiedFresh, avgAudience}] } - poređenjem byGenre reda sa
    overall redom uočava se da li žanr odstupa od opšteg efekta.

    SLOŽENOST: $facet izvršava OBE pod-pipeline grane nad ISTIM ulaznim
    skupom dokumenata (nakon $unwind) - to znači da se rad $unwind faze NE
    duplira, ali svaka grana i dalje radi svoj $group, pa je ukupni trošak
    CPU-a približno zbir troškova obe grane (memorijski se ulazni skup
    čuva samo jednom).
    """
    pipeline = [
        {"$match": {"tomatoMeter": {"$ne": None}, "genre": {"$ne": []}}},
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
    return "Marija_Q3_certified_fresh_efekat", pipeline, "movies"


def q4_fresh_rotten_distribution_by_rating_and_genre():
    """
    PITANJE: Kako se distribucija Fresh i Rotten recenzija razlikuje po
    uzrasnim kategorijama (MPAA rating) i žanrovima filmova?

    FAZE PIPELINE-A (nad `reviews`):
      1. $lookup  - spoji svaku recenziju sa filmom da dobiješ `rating`
                    (MPAA) i `genre` niz
      2. $unwind  - razloži genre niz filma
      3. $facet   - paralelno: (a) distribucija fresh/rotten po MPAA rating
                    kategoriji i (b) distribucija fresh/rotten po žanru
      4. $group   - (unutar svake facet grane) prebroj fresh vs rotten

    OČEKIVANI REZULTAT: { byRating: [{rating, review_state, count}],
    byGenre: [{genre, review_state, count}] }

    SLOŽENOST: Najskuplji upit u celom setu - $lookup nad 1.4M+ recenzija
    (svaka mora pronaći svoj film), zatim $unwind koji dodatno umnožava
    broj dokumenata (faktor ~2x za prosečan broj žanrova po filmu), pa
    obe $facet grane prolaze kroz taj umnoženi skup. Indeks na
    movies.ratingKey je OBAVEZAN da bi $lookup faza koristila IXSCAN
    umesto COLLSCAN za svaku od 1.4M+ recenzija.
    """
    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "movieInfo",
        }},
        {"$unwind": "$movieInfo"},
        {"$unwind": "$movieInfo.genre"},
        {"$facet": {
            "byRating": [
                {"$group": {
                    "_id": {"rating": "$movieInfo.rating", "state": "$review_state"},
                    "count": {"$sum": 1},
                }},
            ],
            "byGenre": [
                {"$group": {
                    "_id": {"genre": "$movieInfo.genre", "state": "$review_state"},
                    "count": {"$sum": 1},
                }},
            ],
        }},
    ]
    return "Marija_Q4_fresh_rotten_po_ratingu_i_zanru", pipeline, "reviews"


def q5_publishers_deviating_from_consensus(years_back: int = 10, reference_date=None):
    """
    PITANJE: Koje publikacije najviše odstupaju od ukupnog kritičarskog
    konsenzusa u poslednjih 10 godina?

    FAZE PIPELINE-A (nad `reviews`):
      1. $match   - zadrži recenzije iz poslednjih `years_back` godina
                    (review_date >= cutoff)
      2. $lookup  - spoji sa filmom da dobiješ tomatoMeter (konsenzus
                    svih kritičara - "ground truth" za poređenje)
      3. $project - izračunaj odstupanje recenzije od konsenzusa filma
      4. $group   - grupiši po publisher_name: prosečno odstupanje, broj
                    recenzija
      5. $sort    - opadajuće po prosečnom odstupanju

    OČEKIVANI REZULTAT: lista {publisher_name, avgDeviation, reviewCount}
    sortirana od publikacije sa najizraženijim "kontrarijanskim" stilom.

    SLOŽENOST: $match na review_date PRE $lookup-a je ključna optimizacija
    - smanjuje broj dokumenata koji ulaze u skupu $lookup fazu. Bez
    compound indeksa koji uključuje review_date, ovaj $match i dalje radi
    COLLSCAN nad celom `reviews` kolekcijom (videti poglavlje 7 - indeksi).
    """
    if reference_date is None:
        reference_date = datetime.utcnow()
    cutoff = reference_date - timedelta(days=365 * years_back)

    pipeline = [
        {"$match": {"review_date": {"$gte": cutoff}}},
        {"$lookup": {
            "from": "movies",
            "localField": "ratingKey",
            "foreignField": "ratingKey",
            "as": "movieInfo",
        }},
        {"$unwind": "$movieInfo"},
        {"$project": {
            "publisher_name": 1,
            "deviation": {
                "$abs": {
                    "$subtract": [
                        {"$multiply": ["$review_score", 100]},
                        "$movieInfo.tomatoMeter",
                    ]
                }
            },
        }},
        {"$group": {
            "_id": "$publisher_name",
            "avgDeviation": {"$avg": "$deviation"},
            "reviewCount": {"$sum": 1},
        }},
        {"$match": {"reviewCount": {"$gte": 20}}},  # statistička relevantnost
        {"$sort": {"avgDeviation": -1}},
    ]
    return "Marija_Q5_publikacije_odstupanje", pipeline, "reviews"


ALL_QUERIES = [
    q1_studios_highest_audience_lowest_variance,
    q2_biggest_gap_movies_by_genre,
    q3_certified_fresh_effect_by_genre,
    q4_fresh_rotten_distribution_by_rating_and_genre,
    q5_publishers_deviating_from_consensus,
]
