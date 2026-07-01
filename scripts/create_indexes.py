"""
Kreiranje indeksa za NEOPTIMIZOVANU šemu (movies + reviews).

Pokretanje:
    python scripts/create_indexes.py --mongo-uri mongodb://localhost:27017 --db rt_analytics
"""

import argparse
from pymongo import MongoClient, ASCENDING, DESCENDING


def create_indexes(db) -> None:
    print("=" * 70)
    print("KREIRANJE INDEKSA - movies kolekcija")
    print("=" * 70)

    # --- Single field indeksi (traženi u zadatku projekta) ---
    idx = db.movies.create_index([("ratingKey", ASCENDING)], unique=True)
    print(f"[movies] {idx} - unique indeks; ratingKey je prirodni ključ filma, "
          f"koristi se u SVAKOM $lookup-u iz reviews kolekcije (foreignField). "
          f"Unique constraint sprečava duplikate filmova.")

    idx = db.movies.create_index([("genre", ASCENDING)])
    print(f"[movies] {idx} - genre je multikey indeks (polje je niz); koristi se "
          f"u Mina Q1 za $unwind po žanru.")

    idx = db.movies.create_index([("studio", ASCENDING)])
    print(f"[movies] {idx} - koristi se u Marija Q1 ($group po studiju); "
          f"korisno ako se ispred $group dodaje $match po konkretnom studiju.")

    idx = db.movies.create_index([("tomatoMeter", DESCENDING)])
    print(f"[movies] {idx} - koristi se za range upite i sortiranje po "
          f"tomatoMeter (npr. Marija Q3 - $match tomatoMeter postoji, "
          f"filtriranje >= 75 za certifiedFresh).")

    # --- Compound indeks (ESR pravilo) ---
    # Demonstracija ESR za upit oblika: find({genre: X}).sort({tomatoMeter: -1})
    #   E (equality)  -> genre
    #   S (sort)      -> tomatoMeter
    idx = db.movies.create_index([("genre", ASCENDING), ("tomatoMeter", DESCENDING)])
    print(f"[movies] {idx} - COMPOUND (ESR: E=genre, S=tomatoMeter). "
          f"Pokriva upite koji filtriraju po žanru i sortiraju/agregiraju po "
          f"tomatoMeter-u.")

    print("\n" + "=" * 70)
    print("KREIRANJE INDEKSA - reviews kolekcija")
    print("=" * 70)

    idx = db.reviews.create_index([("ratingKey", ASCENDING)])
    print(f"[reviews] {idx} - NAJVAŽNIJI indeks u celom projektu. Ovo je "
          f"foreignField u SVAKOM $lookup-u iz movies kolekcije. Bez njega, "
          f"svaki $lookup poziv radi COLLSCAN nad 1.4M+ recenzija. "
          f"Koristi ga i Mina Q3 (optimizovani, namerno zadržan $lookup na reviews) "
          f"i Mina Q2/Q4/Q5 (optimizovani, $lookup iz reviews ka movies_with_stats "
          f"-> ovaj indeks ubrzava skeniranje reviews kolekcije).")

    idx = db.reviews.create_index([("critic_name", ASCENDING)])
    print(f"[reviews] {idx} - tražen u specifikaciji projekta; koristan za "
          f"buduće upite tipa 'sve recenzije kritičara X'.")

    idx = db.reviews.create_index([("publicatioName", ASCENDING)])
    print(f"[reviews] {idx} - koristi se u Mina Q2 ($group po publicatioName). "
          f"NAPOMENA: naziv polja je 'publicatioName' (sa greškom u kucanju iz "
          f"originalnog CSV-a) - potvrđeno direktnom proverom baze.")

    idx = db.reviews.create_index([("top_critic", ASCENDING)])
    print(f"[reviews] {idx} - koristi se u Mina Q3 ($group po top_critic). "
          f"Niska selektivnost (samo 2 vrednosti) - sam za sebe nije veoma "
          f"koristan za FILTRIRANJE velike kolekcije, ali koristan kao deo "
          f"compound indeksa (videti ispod).")

    idx = db.reviews.create_index([("review_date", DESCENDING)])
    print(f"[reviews] {idx} - koristi se u Mina Q5 ($project $month iz review_date) "
          f"i Marija Q5 ($match review_date >= cutoff).")

    # --- Compound indeks (ESR pravilo) ---
    #   E (equality) -> top_critic
    #   R (range)    -> review_date
    idx = db.reviews.create_index([("top_critic", ASCENDING), ("review_date", DESCENDING)])
    print(f"[reviews] {idx} - COMPOUND (ESR: E=top_critic, R=review_date). "
          f"Pokriva upite koji filtriraju po tipu kritičara i potom po "
          f"opsegu datuma (npr. 'top kritičari u poslednjih 10 godina').")

    # --- Compound indeks (ESR) za $lookup + $match kombinaciju ---
    # ratingKey (equality, koristi se kao foreignField) + review_state
    # (equality, koristi se u $group). Oba su equality => redosled po
    # selektivnosti: ratingKey je skoro unique (visoka selektivnost),
    # review_state ima samo 2 vrednosti (niska selektivnost) => ratingKey ide
    # prvo, što takođe odgovara ESR jer je ratingKey "pravo" equality polje
    # za spajanje, dok je review_state equality polje za naknadni $group.
    idx = db.reviews.create_index([("ratingKey", ASCENDING), ("review_state", ASCENDING)])
    print(f"[reviews] {idx} - COMPOUND (E=ratingKey [visoka selektivnost], "
          f"E=review_state [niska selektivnost]). Ubrzava $lookup + naknadnu "
          f"$group po review_state (Marija Q4) jer MongoDB može koristiti "
          f"indeks i za spoj i za grupisanje u istoj IXSCAN fazi.")

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
    print(f"[movies_with_stats] {idx} - prirodni ključ. Ujedno foreignField za "
          f"$lookup u Mina Q2/Q4/Q5 (optimizovani upiti spajaju reviews ka "
          f"movies_with_stats po ovom polju), pa je indeks tu KLJUČAN.")

    idx = db.movies_with_stats.create_index([("genre", ASCENDING)])
    print(f"[movies_with_stats] {idx} - isti razlog kao u neoptimizovanoj šemi.")

    # ESR compound: E=genre, R=reviewCount (Mina Q1 sada filtrira DIREKTNO
    # po denormalizovanom reviewCount, bez $lookup+$size)
    idx = db.movies_with_stats.create_index([("genre", ASCENDING), ("reviewCount", DESCENDING)])
    print(f"[movies_with_stats] {idx} - COMPOUND (ESR: E=genre, R=reviewCount). "
          f"Mina Q1 u optimizovanoj verziji filtrira reviewCount >= 50 BEZ "
          f"$lookup-a - ovaj indeks pokriva i $match i $unwind genre fazu.")

    idx = db.movies_with_stats.create_index([("studio", ASCENDING)])
    print(f"[movies_with_stats] {idx} - Marija Q1.")

    idx = db.movies_with_stats.create_index([("tomatoMeter", DESCENDING)])
    print(f"[movies_with_stats] {idx} - Marija Q3 (certifiedFresh threshold).")

    idx = db.movies_with_stats.create_index([("reviews.top_critic", ASCENDING)])
    print(f"[movies_with_stats] {idx} - multikey indeks nad ugnježdenim nizom; "
          f"podržava upite nakon $unwind reviews.")

    # --- INDEKS ZA $match NA POČETKU PIPELINE-A (uslov za IXSCAN) ---
    # Mina Q1 počinje sa $match {reviewCount >= 50}, što je SELEKTIVAN filter
    # (odbacuje većinu filmova koji imaju < 50 recenzija) -> IXSCAN je koristan.
    idx = db.movies_with_stats.create_index([("reviewCount", ASCENDING)])
    print(f"[movies_with_stats] {idx} - KLJUČNI za IXSCAN u Mina Q1 "
          f"($match {{reviewCount >= 50}} kao prva faza). Selektivan filter, "
          f"indeks značajno smanjuje broj pregledanih dokumenata.")

    idx = db.movies_with_stats.create_index([("releaseYear", ASCENDING)])
    print(f"[movies_with_stats] {idx} - koristan za upite koji filtriraju po "
          f"godini/deceniji izlaska. NAPOMENA: Mina Q4 (optimizovani) radi nad "
          f"reviews kolekcijom i filtrira po m.releaseYear TEK nakon $lookup-a, "
          f"pa za Q4 ovaj indeks NIJE u prvoj fazi; indeks ostaje koristan za "
          f"direktne upite nad movies_with_stats po releaseYear.")

    idx = db.movies_with_stats.create_index([("audienceScore", ASCENDING)])
    print(f"[movies_with_stats] {idx} - KLJUČNI za IXSCAN. Marija Q1 počinje "
          f"$match-om {{audienceScore >= 0}}.")

    # NAPOMENA: tomatoMeter indeks je već kreiran gore (za Marija Q3 threshold),
    # i sada ga dodatno koriste Marija Q2 i Q5 koji počinju $match-om
    # {tomatoMeter >= 0}. Jedan indeks - više upita.

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