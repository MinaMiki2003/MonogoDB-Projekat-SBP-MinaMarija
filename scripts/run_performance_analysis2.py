"""
Automatizovana analiza performansi - pokreće SVIH 10 pitanja u OBE verzije
(neoptimizovana šema sa $lookup-om i optimizovana movies_with_stats šema),
za svaki upit poziva explain("executionStats"), izdvaja ključne metrike i
generiše uporednu tabelu (poglavlje 9 specifikacije projekta).

VAŽNO: Ovaj skript treba pokrenuti NA RAČUNARU GDE JE MONGODB POKRENUT i
GDE SU PODACI VEĆ UVEZENI (etl.py) i GDE JE movies_with_stats VEĆ
IZGRAĐENA (build_movies_with_stats.py) i indeksi kreirani
(create_indexes.py). Brojevi koje ovaj skript ispisuje su REALNI rezultati
nad TVOJIM podacima - ne izmišljene vrednosti.

Pokretanje:
    python scripts/run_performance_analysis.py --mongo-uri mongodb://localhost:27017 --db rt_analytics

Izlaz:
    results/explain_unoptimized/<naziv_upita>.json   (sirovi explain() izlaz)
    results/explain_optimized/<naziv_upita>.json
    results/performance_comparison.md   (markdown tabela, poglavlje 9)
    results/performance_comparison.csv
"""

import argparse
import json
import sys
from pathlib import Path

from pymongo import MongoClient

# Da bi import-i query modula radili nezavisno od trenutnog radnog direktorijuma
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "queries" / "unoptimized"))
sys.path.insert(0, str(PROJECT_ROOT / "queries" / "optimized"))

import mina_queries as mina_unopt
import marija_queries as marija_unopt
import mina_queries_optimized as mina_opt
import marija_queries_optimized as marija_opt


def extract_stats(explain_output: dict) -> dict:
    """
    Izdvaja ključne metrike iz explain odziva sa verbosity="executionStats".

    Struktura explain izlaza varira po tipu pipeline-a:
      - jednostavan pipeline: top-level `executionStats` sa
        executionTimeMillis/totalDocsExamined/totalKeysExamined
      - pipeline sa $lookup/$group/$facet: metrike su raspoređene po
        `stages` nizu, unutar `$cursor.executionStats`, i po ugnježdenim
        `executionStages` čvorovima
    Zbog toga radimo rekurzivni obilazak i uzimamo MAKSIMALNU vrednost vremena
    (ukupno trajanje upita) i ZBIR pregledanih dokumenata/ključeva.
    """
    total_docs_examined = 0
    total_keys_examined = 0
    execution_times = []  # skupljamo sve nađene vrednosti, uzimamo max
    stage_types_seen = set()

    def walk(node):
        nonlocal total_docs_examined, total_keys_examined
        if isinstance(node, dict):
            if "totalDocsExamined" in node and isinstance(node["totalDocsExamined"], (int, float)):
                total_docs_examined += node["totalDocsExamined"]
            if "totalKeysExamined" in node and isinstance(node["totalKeysExamined"], (int, float)):
                total_keys_examined += node["totalKeysExamined"]
            if "docsExamined" in node and isinstance(node["docsExamined"], (int, float)):
                total_docs_examined += node["docsExamined"]
            if "keysExamined" in node and isinstance(node["keysExamined"], (int, float)):
                total_keys_examined += node["keysExamined"]
            # Vreme: i executionTimeMillis (na nivou upita) i
            # executionTimeMillisEstimate (po fazi agregacije)
            for time_key in ("executionTimeMillis", "executionTimeMillisEstimate"):
                if time_key in node and isinstance(node[time_key], (int, float)):
                    execution_times.append(node[time_key])
            if "stage" in node and isinstance(node["stage"], str):
                stage_types_seen.add(node["stage"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(explain_output)

    # Ukupno vreme upita = najveća izmerena vrednost (faze se izvršavaju
    # sekvencijalno pa je trajanje cele agregacije >= trajanja najduže faze;
    # top-level executionTimeMillis, ako postoji, biće upravo taj maksimum).
    execution_time_ms = max(execution_times) if execution_times else 0

    return {
        "executionTimeMillis": execution_time_ms,
        "totalDocsExamined": total_docs_examined,
        "totalKeysExamined": total_keys_examined,
        "stagesUsed": sorted(stage_types_seen),
        "usedCOLLSCAN": "COLLSCAN" in stage_types_seen,
        "usedIXSCAN": "IXSCAN" in stage_types_seen,
    }


def run_and_explain(db, collection_name: str, pipeline: list[dict]) -> dict:
    # Da bismo dobili STVARNE statistike izvršavanja (executionTimeMillis,
    # totalDocsExamined...), a ne samo plan, koristi se 'explain' komanda sa
    # verbosity="executionStats". Poziv preko db.command("aggregate", ...,
    # explain=True) vraća SAMO queryPlanner (bez stvarnih brojeva), zato se
    # umesto toga eksplicitno poziva 'explain' komanda koja obavija
    # 'aggregate' potkomandu.
    explain_command = {
        "explain": {
            "aggregate": collection_name,
            "pipeline": pipeline,
            "cursor": {},
        },
        "verbosity": "executionStats",
    }
    return db.command(explain_command)


def run_suite(db, query_modules, label: str, output_dir: Path) -> list[dict]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for module in query_modules:
        for query_fn in module.ALL_QUERIES:
            name, pipeline, collection_name = query_fn()
            print(f"[{label}] Izvršavam: {name} (nad '{collection_name}')...")
            try:
                explain_output = run_and_explain(db, collection_name, pipeline)
            except Exception as exc:
                print(f"  GREŠKA: {exc}")
                continue

            stats = extract_stats(explain_output)
            stats["queryName"] = name
            stats["collection"] = collection_name
            results.append(stats)

            with open(output_dir / f"{name}.json", "w", encoding="utf-8") as f:
                json.dump(explain_output, f, default=str, indent=2)

            print(f"  -> executionTimeMillis={stats['executionTimeMillis']}, "
                  f"totalDocsExamined={stats['totalDocsExamined']}, "
                  f"totalKeysExamined={stats['totalKeysExamined']}, "
                  f"COLLSCAN={stats['usedCOLLSCAN']}, IXSCAN={stats['usedIXSCAN']}")
    return results


def build_comparison_table(unopt_results: list[dict], opt_results: list[dict]) -> str:
    """Poveže upite po logičkom imenu (bez _OPT sufiksa) i izračuna % poboljšanja."""

    def base_name(name: str) -> str:
        return name.replace("_OPT", "").replace("_LOOKUP", "")

    opt_by_base = {base_name(r["queryName"]): r for r in opt_results}

    lines = [
        "| Upit | Pre optimizacije (ms) | Posle optimizacije (ms) | Poboljšanje (%) | "
        "DocsExamined pre | DocsExamined posle |",
        "|---|---|---|---|---|---|",
    ]

    csv_lines = ["upit,vreme_pre_ms,vreme_posle_ms,poboljsanje_pct,docs_pre,docs_posle"]

    for r in unopt_results:
        base = base_name(r["queryName"])
        opt = opt_by_base.get(base)
        if not opt:
            continue
        before = r["executionTimeMillis"] or 0
        after = opt["executionTimeMillis"] or 0
        improvement = ((before - after) / before * 100) if before else 0
        lines.append(
            f"| {base} | {before} | {after} | {improvement:.1f}% | "
            f"{r['totalDocsExamined']} | {opt['totalDocsExamined']} |"
        )
        csv_lines.append(f"{base},{before},{after},{improvement:.1f},"
                          f"{r['totalDocsExamined']},{opt['totalDocsExamined']}")

    return "\n".join(lines), "\n".join(csv_lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="rt_analytics")
    parser.add_argument("--samo-mina", action="store_true",
                         help="Pokreni samo Mina upite (bez Marija)")
    parser.add_argument("--samo-marija", action="store_true",
                         help="Pokreni samo Marija upite (bez Mina)")
    args = parser.parse_args()

    # Biranje kojih upita - samo Mina ili svi
    if args.samo_mina:
        unopt_modules = [mina_unopt]
        opt_modules = [mina_opt]
        print(">>> Pokrecem SAMO MINA upite <<<")
    elif args.samo_marija:
        unopt_modules = [marija_unopt]
        opt_modules = [marija_opt]
        print(">>> Pokrecem SAMO MARIJA upite <<<")
    else:
        unopt_modules = [mina_unopt, marija_unopt]
        opt_modules = [mina_opt, marija_opt]

    results_dir = PROJECT_ROOT / "results"
    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    print("\n" + "#" * 70)
    print("# DEO 1/2: NEOPTIMIZOVANA ŠEMA (movies + reviews, sa $lookup)")
    print("#" * 70)
    unopt_results = run_suite(
        db, unopt_modules, "NEOPTIMIZOVANO",
        results_dir / "explain_unoptimized",
    )

    print("\n" + "#" * 70)
    print("# DEO 2/2: OPTIMIZOVANA ŠEMA (movies_with_stats)")
    print("#" * 70)
    
    opt_results = run_suite(
        db, opt_modules, "OPTIMIZOVANO",
        results_dir / "explain_optimized",
    )

    table_md, table_csv = build_comparison_table(unopt_results, opt_results)

    comparison_md_path = results_dir / "performance_comparison.md"
    with open(comparison_md_path, "w", encoding="utf-8") as f:
        f.write("# Uporedna analiza performansi - pre i posle optimizacije\n\n")
        f.write(table_md)
        f.write("\n")

    with open(results_dir / "performance_comparison.csv", "w", encoding="utf-8") as f:
        f.write(table_csv)

    print("\n" + "=" * 70)
    print("ZAVRŠENO. Uporedna tabela:")
    print("=" * 70)
    print(table_md)
    print(f"\nSnimljeno u: {comparison_md_path}")


if __name__ == "__main__":
    main()
