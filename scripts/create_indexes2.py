"""
Kreiranje indeksa za NEOPTIMIZOVANU šemu (movies + reviews) i za
OPTIMIZOVANU šemu (movies_with_stats).

Pokretanje:
    python scripts/create_indexes.py --mongo-uri mongodb://localhost:27017 --db rt_analytics

Aktuelni upiti koje indeksi podržavaju:
  MINA:   Q1 žanr, Q2 publikacija, Q3 top_critic ($lookup demo), Q4 decenija, Q5 raskorak
  MARIJA: Q1 studio, Q2 MPAA rating, Q3 critic_name ($lookup demo), Q4 sentiment, Q5 $bucket
"""

import argparse
from pymongo import MongoClient, ASCENDING, DESCENDING


def create_indexes(db) -> None:
    print("=" * 70)
    print("KREIRANJE INDEKSA - movies kolekcija")
    print("=" * 70)

    # --- Single field indeksi ---
    idx = db.movies.create_index([("ratingKey", ASCENDING)], unique=True)
    print(f"[movies] {idx} - unique indeks; ratingKey je prirodni ključ filma, "
          f"koristi se u SVAKOM $lookup-u iz reviews kolekcije (foreignField) i "
          f"pri izgradnji movies_with_stats. Unique sprečava duplikate filmova.")

    idx = db.movies.create_index([("genre", ASCENDING)])
    print(f"[movies] {idx} - genre je multikey indeks (polje je niz); koristi se "
          f"u Mina Q1 za $unwind/grupisanje po žanru.")

    idx = db.movies.create_index([("studio", ASCENDING)])
    print(f"[movies] {idx} - koristi se u Marija Q1 (analiza aktivnosti i "
          f"kvaliteta po studiju).")

    idx = db.movies.create_index([("tomatoMeter", DESCENDING)])
    print(f"[movies] {idx} - range upiti i sortiranje po tomatoMeter; koristi se "
          f"u Mina Q5 (raskorak kritika/publika) i Marija Q5 ($bucket po "
          f"opsezima tomatoMeter-a).")

    # --- Compound indeks (ESR pravilo) ---
    # Demonstracija ESR za upit oblika: find({genre: X}).sort({tomatoMeter: -1})
    #   E (equality) -> genre
    #   S (sort)     -> tomatoMeter
    idx = db.movies.create_index([("genre", ASCENDING), ("tomatoMeter", DESCENDING)])
    print(f"[movies] {idx} - COMPOUND (ESR: E=genre, S=tomatoMeter). "
          f"Pokriva upite koji filtriraju po žanru i sortiraju po tomatoMeter-u.")

    print("\n" + "=" * 70)
    print("KREIRANJE INDEKSA - reviews kolekcija")
    print("=" * 70)

    idx = db.reviews.create_index([("ratingKey", ASCENDING)])
    print(f"[reviews] {idx} - NAJVAŽNIJI indeks u celom projektu. Ovo je "
          f"foreignField u SVAKOM $lookup-u iz movies kolekcije (i u Mina Q3 i "
          f"Marija Q3 koji namerno zadržavaju $lookup). Bez njega svaki $lookup "
          f"radi COLLSCAN nad 1.4M+ recenzija.")

    idx = db.reviews.create_index([("critic_name", ASCENDING)])
    print(f"[reviews] {idx} - koristi se u Marija Q3 ($group po critic_name - "
          f"profilisanje najstrožih/najblažih kritičara).")

    idx = db.reviews.create_index([("publicatioName", ASCENDING)])
    print(f"[reviews] {idx} - koristi se u Mina Q2 (grupisanje po publikaciji - "
          f"polje publicatioName).")

    idx = db.reviews.create_index([("top_critic", ASCENDING)])
    print(f"[reviews] {idx} - koristi se u Mina Q3 ($group po top_critic). "
          f"Niska selektivnost (2 vrednosti) - sam za sebe slab za filtriranje, "
          f"ali koristan kao deo compound indeksa (videti ispod).")

    idx = db.reviews.create_index([("review_date", DESCENDING)])
    print(f"[reviews] {idx} - koristi se za analizu po datumu recenzije "
          f"(izvlačenje meseca/godine iz review_date).")

    idx = db.reviews.create_index([("scoreSentiment", ASCENDING)])
    print(f"[reviews] {idx} - koristi se u Marija Q4 ($group po scoreSentiment - "
          f"kontrola poklapanja sentiment oznake sa ocenom).")

    # --- Compound indeks (ESR pravilo) ---
    # E (equality) -> top_critic, R (range) -> review_date
    idx = db.reviews.create_index([("top_critic", ASCENDING), ("review_date", DESCENDING)])
    print(f"[reviews] {idx} - COMPOUND (ESR: E=top_critic, R=review_date). "
          f"Demonstracija compound indeksa za upite tipa 'top kritičari u "
          f"određenom vremenskom opsegu'.")

    # --- Compound indeks (ESR) ---
    # ratingKey (equality, foreignField za $lookup) + review_state (equality).
    # ratingKey ima visoku selektivnost (skoro unique) pa ide prvo;
    # review_state (fresh/rotten) ima nisku selektivnost.
    idx = db.reviews.create_index([("ratingKey", ASCENDING), ("review_state", ASCENDING)])
    print(f"[reviews] {idx} - COMPOUND (E=ratingKey [visoka selektivnost], "
          f"E=review_state [niska selektivnost]). ratingKey je foreignField za "
          f"$lookup, a review_state se koristi pri izgradnji movies_with_stats "
          f"(brojanje freshCount/rottenCount).")

    print("\nSvi indeksi (neoptimizovana šema) kreirani. Pregled:")
    for coll_name in ("movies", "reviews"):
        print(f"\n-- db.{coll_name}.getIndexes() --")
        for index_info in db[coll_name].list_indexes():
            print(f"  {index_info['name']}: {index_info['key']}")


def create_indexes_optimized(db) -> None:
    """Indeksi za optimizovanu (denormalizovanu) movies_with_stats kolekciju."""
    print("\n" + "=" * 70)
    print("KREIRANJE INDEKSA - movies_with_stats kolekcija (optimizovana šema)")
    print("=" * 70)

    idx = db.movies_with_stats.create_index([("ratingKey", ASCENDING)], unique=True)
    print(f"[movies_with_stats] {idx} - prirodni ključ. U Mina Q3 i Marija Q3 "
          f"služi kao localField za $lookup ka reviews kolekciji; inače za "
          f"direktan pristup filmu po ključu.")

    idx = db.movies_with_stats.create_index([("genre", ASCENDING)])
    print(f"[movies_with_stats] {idx} - multikey indeks (genre je niz); "
          f"podržava Mina Q1 ($unwind po žanru).")

    # ESR compound: E=genre, R=reviewCount (Mina Q1 filtrira reviewCount BEZ $lookup-a)
    idx = db.movies_with_stats.create_index([("genre", ASCENDING), ("reviewCount", DESCENDING)])
    print(f"[movies_with_stats] {idx} - COMPOUND (ESR: E=genre, R=reviewCount). "
          f"Podržava Mina Q1 koji filtrira reviewCount >= 50 i potom $unwind po "
          f"žanru, bez $lookup-a.")

    idx = db.movies_with_stats.create_index([("studio", ASCENDING)])
    print(f"[movies_with_stats] {idx} - Marija Q1 ($group po studiju).")

    idx = db.movies_with_stats.create_index([("tomatoMeter", DESCENDING)])
    print(f"[movies_with_stats] {idx} - Marija Q5 ($bucket po opsezima "
          f"tomatoMeter-a) i Mina Q5 (raskorak kritika/publika).")

    idx = db.movies_with_stats.create_index([("reviews.top_critic", ASCENDING)])
    print(f"[movies_with_stats] {idx} - multikey indeks nad ugnježdenim nizom "
          f"recenzija; podržava upite koji rade $unwind reviews.")

    # --- INDEKSI ZA $match NA POČETKU PIPELINE-A (uslov za IXSCAN) ---
    # Svaki optimizovani upit (osim Q3) počinje sa $match nad jednim od ovih
    # polja, što tera MongoDB da koristi IXSCAN umesto COLLSCAN.
    idx = db.movies_with_stats.create_index([("reviewCount", ASCENDING)])
    print(f"[movies_with_stats] {idx} - KLJUČNI za IXSCAN. Najkorišćeniji indeks: "
          f"Mina Q1 (reviewCount>=50), Mina Q2/Q5 (reviewCount>0) i Marija "
          f"Q1/Q2/Q4 (reviewCount>0) i Q5 (reviewCount>=10) počinju $match-om "
          f"nad ovim poljem. Jedan indeks - više upita.")

    idx = db.movies_with_stats.create_index([("releaseYear", ASCENDING)])
    print(f"[movies_with_stats] {idx} - KLJUČNI za IXSCAN. Mina Q4 počinje "
          f"$match-om {{releaseYear > 0}} (grupisanje po deceniji).")

    idx = db.movies_with_stats.create_index([("audienceScore", ASCENDING)])
    print(f"[movies_with_stats] {idx} - podržava analizu audienceScore "
          f"(Marija Q2 i Q5 računaju prosečan audienceScore po grupi).")


      # ========================================================================
    # RAZNOVRSNI INDEKSI ZA MARIJA UPITE (svaki drugačiji koncept)
    # ========================================================================

    # Marija Q1 - COMPOUND indeks (ESR: E=studio, R=reviewCount)
    idx = db.movies_with_stats.create_index(
        [("studio", ASCENDING), ("reviewCount", DESCENDING)])
    print(f"[movies_with_stats] {idx} - COMPOUND (ESR: E=studio, R=reviewCount) "
          f"za Marija Q1. $match filtrira po studio (equality) i reviewCount "
          f"(range) u jednoj IXSCAN fazi.")

    # Marija Q2 - SINGLE-FIELD indeks na MPAA rating
    idx = db.movies_with_stats.create_index([("rating", ASCENDING)])
    print(f"[movies_with_stats] {idx} - SINGLE-FIELD za Marija Q2. $match "
          f"{{rating != null}} -> IXSCAN, grupisanje po uzrasnoj kategoriji.")

    # Marija Q4 - MULTIKEY indeks nad UGNJEŽDENIM poljem
    idx = db.movies_with_stats.create_index([("reviews.scoreSentiment", ASCENDING)])
    print(f"[movies_with_stats] {idx} - MULTIKEY nad ugnježdenim nizom za "
          f"Marija Q4. Indeks nad poljem UNUTAR niza reviews[] (reviews."
          f"scoreSentiment); MongoDB pravi indeksni ulaz za svaki element niza.")

    # Marija Q5 - PARTIAL indeks (indeksira samo podskup dokumenata)
    idx = db.movies_with_stats.create_index(
        [("tomatoMeter", ASCENDING)],
        partialFilterExpression={"reviewCount": {"$gte": 10}},
        name="tomatoMeter_partial_reviewCount10")
    print(f"[movies_with_stats] {idx} - PARTIAL indeks za Marija Q5. Indeksira "
          f"SAMO filmove sa reviewCount >= 10 (manji indeks = brži i štedi "
          f"prostor). $bucket radi nad ovim podskupom.")


    print("\n-- db.movies_with_stats.getIndexes() --")
    for index_info in db.movies_with_stats.list_indexes():
        print(f"  {index_info['name']}: {index_info['key']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="rt_analytics")
    parser.add_argument("--optimized-only", action="store_true",
                         help="Kreiraj samo indekse za movies_with_stats (preskoči movies/reviews)")
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    database = client[args.db]

    if not args.optimized_only:
        create_indexes(database)
    create_indexes_optimized(database)