"""
Izvršava svih 10 agregacionih upita (optimizovana verzija, nad
movies_with_stats) i LEPO ISPISUJE STVARNE REZULTATE u terminal -
odgovore na poslovna pitanja, NE performanse.

Svaki upit je ograničen na top N redova pri ispisu (DISPLAY_LIMIT) da
terminal ostane čitljiv - sama agregacija i dalje obrađuje ceo dataset,
ali se prikazuje samo vrh rezultata.

Pokretanje:
    python scripts/show_query_results.py --db rt_analytics
    python scripts/show_query_results.py --db rt_analytics --limit 20
    python scripts/show_query_results.py --db rt_analytics --unoptimized   (koristi movies+reviews verziju)
"""

import argparse
import sys
from pathlib import Path

from pymongo import MongoClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "queries" / "unoptimized"))
sys.path.insert(0, str(PROJECT_ROOT / "queries" / "optimized"))

DISPLAY_LIMIT = 15  # koliko redova rezultata ispisati po upitu

# Kratak, čitljiv naslov + poslovno pitanje za svaki upit (po imenu funkcije)
QUERY_TITLES = {
    "Mina_Q1": "MINA Q1: Žanrovi sa najvišim prosečnim TomatoMeter (min. 50 recenzija)",
    "Mina_Q2": "MINA Q2: TomatoMeter vs AudienceScore kroz decenije po žanrovima",
    "Mina_Q3": "MINA Q3: Da li top kritičari više odstupaju od publike nego regularni",
    "Mina_Q4": "MINA Q4: Reditelji sa najboljim balansom kritika/publika (min. 5 filmova)",
    "Mina_Q5": "MINA Q5: Meseci sa najvećom aktivnošću kritičara",
    "Marija_Q1": "MARIJA Q1: Studiji - najviši AudienceScore + najmanja varijacija (min. 20 filmova)",
    "Marija_Q2": "MARIJA Q2: Filmovi sa najvećim raskorakom TomatoMeter/AudienceScore",
    "Marija_Q3": "MARIJA Q3: Da li Certified Fresh jednako utiče na sve žanrove",
    "Marija_Q4": "MARIJA Q4: Distribucija Fresh/Rotten po uzrastu (MPAA) i žanru",
    "Marija_Q5": "MARIJA Q5: Publikacije koje najviše odstupaju od konsenzusa (10 god.)",
}


def short_key(query_name: str) -> str:
    """'Mina_Q1_OPT_zanrovi...' -> 'Mina_Q1'"""
    parts = query_name.split("_")
    return f"{parts[0]}_{parts[1]}"


def format_value(value):
    """Zaokruži float-ove radi čitljivosti, skrati duge liste."""
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, list):
        if len(value) > 3:
            return value[:3] + [f"... (+{len(value) - 3})"]
        return value
    return value


def print_results(title: str, results: list, limit: int) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)

    if not results:
        print("  (nema rezultata)")
        return

    shown = results[:limit]
    for i, doc in enumerate(shown, 1):
        # Ukloni _id ako je None/prazan radi čistijeg prikaza
        clean = {k: format_value(v) for k, v in doc.items()}
        print(f"  {i:2d}. {clean}")

    if len(results) > limit:
        print(f"  ... (prikazano {limit} od {len(results)} rezultata)")
    else:
        print(f"  (ukupno {len(results)} rezultata)")


def run_all(db, query_modules, limit: int) -> None:
    for module in query_modules:
        for query_fn in module.ALL_QUERIES:
            name, pipeline, collection_name = query_fn()
            title = QUERY_TITLES.get(short_key(name), name)
            try:
                results = list(db[collection_name].aggregate(pipeline, allowDiskUse=True))
            except Exception as exc:
                print(f"\n!! GREŠKA u {name}: {exc}")
                continue
            print_results(title, results, limit)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="rt_analytics")
    parser.add_argument("--limit", type=int, default=DISPLAY_LIMIT,
                         help="Broj redova rezultata po upitu (default 15)")
    parser.add_argument("--unoptimized", action="store_true",
                         help="Koristi neoptimizovanu verziju upita (movies+reviews, sporije)")
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    if args.unoptimized:
        import mina_queries, marija_queries
        modules = [mina_queries, marija_queries]
        print(">>> Prikazujem rezultate NEOPTIMIZOVANE verzije (movies + reviews) <<<")
    else:
        import mina_queries_optimized, marija_queries_optimized
        modules = [mina_queries_optimized, marija_queries_optimized]
        print(">>> Prikazujem rezultate OPTIMIZOVANE verzije (movies_with_stats) <<<")

    run_all(db, modules, args.limit)
    print("\n" + "=" * 78)
    print("ZAVRŠENO - svih 10 upita izvršeno.")
    print("=" * 78)


if __name__ == "__main__":
    main()
