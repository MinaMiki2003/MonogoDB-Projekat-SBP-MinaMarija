"""
ETL proces za projekat "Analiza filmova i kritičarskih recenzija korišćenjem MongoDB baze"

Ulaz:  data/raw/rotten_tomatoes_movies.csv
       data/raw/rotten_tomatoes_movie_reviews.csv
Izlaz: MongoDB baza `rt_analytics`, kolekcije `movies` i `reviews`

Pokretanje:
    python scripts/etl.py --movies data/raw/rotten_tomatoes_movies.csv \
                           --reviews data/raw/rotten_tomatoes_movie_reviews.csv \
                           --mongo-uri mongodb://localhost:27017 \
                           --db rt_analytics

Napomena: dataset ima ~143k filmova i ~1.4M recenzija. Učitavanje csv-a se radi
u chunk-ovima (reviews fajl) da se ne premaši dostupna memorija, a insert u
MongoDB se radi u batch-evima radi performansi.
"""

import argparse
import re
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from pymongo import MongoClient, UpdateOne

# ---------------------------------------------------------------------------
# Pomoćne konstante
# ---------------------------------------------------------------------------

# Mapiranje slovnih ocena u numeričku skalu 0-1 (koristi se pri parsiranju
# heterogenog polja originalScore u reviews datasetu)
LETTER_GRADE_MAP = {
    "A+": 1.00, "A": 0.95, "A-": 0.90,
    "B+": 0.85, "B": 0.80, "B-": 0.75,
    "C+": 0.70, "C": 0.65, "C-": 0.60,
    "D+": 0.55, "D": 0.50, "D-": 0.45,
    "F": 0.20,
}

CHUNK_SIZE = 50_000      # za čitanje velikog reviews CSV-a
INSERT_BATCH_SIZE = 5_000  # za insert_many u MongoDB


# ---------------------------------------------------------------------------
# KORAK 1: Analiza kvaliteta podataka
# ---------------------------------------------------------------------------

def analyze_quality(df: pd.DataFrame, name: str) -> None:
    """Štampa izveštaj o kvalitetu podataka za dati DataFrame."""
    print(f"\n{'=' * 70}")
    print(f"ANALIZA KVALITETA PODATAKA: {name}")
    print(f"{'=' * 70}")
    print(f"Broj redova: {len(df):,}")
    print(f"Broj kolona: {df.shape[1]}")

    print("\n-- Tipovi podataka --")
    print(df.dtypes)

    print("\n-- NULL vrednosti po koloni --")
    null_report = pd.DataFrame({
        "null_count": df.isnull().sum(),
        "null_percent": (df.isnull().sum() / len(df) * 100).round(2),
    }).sort_values("null_percent", ascending=False)
    print(null_report)

    df_for_dup_check = df.copy()
    for col in df_for_dup_check.columns:
        if df_for_dup_check[col].apply(lambda x: isinstance(x, list)).any():
            df_for_dup_check[col] = df_for_dup_check[col].astype(str)
    dup_count = df_for_dup_check.duplicated().sum()
    print(f"\n-- Potpuni duplikati redova: {dup_count:,} "
          f"({dup_count / len(df) * 100:.2f}%) --")

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        print("\n-- Statistika numeričkih kolona --")
        print(df[numeric_cols].describe())


# ---------------------------------------------------------------------------
# KORAK 2: Analiza upotrebljivosti kolona
# ---------------------------------------------------------------------------

# Pragovi za odluku o koloni (dokumentovano obrazloženje, ne magic numbers)
HIGH_NULL_THRESHOLD = 60.0   # iznad ovog % NULL -> kandidat za uklanjanje/transformaciju
MEDIUM_NULL_THRESHOLD = 15.0  # iznad ovog % -> kandidat za dopunu

COLUMN_DECISIONS_MOVIES = {
    # kolona: (odluka, obrazloženje)
    "soundMix": ("ukloniti", "Tehnički podatak, nije relevantan za nijedno od 10 agregacionih pitanja."),
    "reviewUrl": ("ukloniti", "Nije deo movies fajla, ali se navodi za review fajl - nije relevantan za analitiku."),
    "ratingContents": ("zadržati", "Korisno za buduća proširenja (analiza po sadržaju klasifikacije); mala količina podataka."),
    "boxOffice": ("transformisati", "Tekstualni format ($111.3M) -> parsirati u numeričku vrednost u USD; NULL ostaje NULL (nepoznata zarada != 0)."),
    "audienceScore": ("zadržati uz dopunu medianom po žanru", "Koristi se u 4 od 10 pitanja; potpuno uklanjanje filmova bez ove vrednosti bi obrisalo previše podataka."),
    "tomatoMeter": ("zadržati, prevalidirati", "Centralna kolona projekta; vrednost se i preračunava iz reviews kolekcije radi provere konzistentnosti."),
    "genre": ("transformisati", "Comma-separated string -> niz (array) stringova, obavezno za $unwind operacije u pitanjima 2,3,4."),
    "director": ("transformisati i preimenovati", "-> directors (niz); koristi se za pitanje Mina #4."),
    "cast": ("transformisati i preimenovati", "-> actors (niz); nije direktno korišćeno u 10 pitanja, ali zadržano radi opšte analitičke vrednosti i budućih upita."),
    "writer": ("transformisati i preimenovati, zadržati", "-> authors (niz); zadržano radi kompletnosti šeme iz specifikacije projekta."),
}

COLUMN_DECISIONS_REVIEWS = {
    "reviewUrl": ("ukloniti", "URL recenzije nije analitički relevantan i značajno povećava veličinu dokumenta bez koristi."),
    "originalScore": ("transformisati", "Heterogeni format (3/5, B+, 85/100...) -> normalizovana review_score vrednost 0-1; ako se ne može parsirati, NULL (ne 0, jer 0 bi lažno predstavljalo najgoru ocenu)."),
    "scoreSentiment": ("zadržati", "Koristan kao sekundarna provera u odnosu na reviewState."),
    "isTopCritic": ("zadržati, preimenovati", "-> top_critic; centralna kolona za pitanja Mina #3 i Marija #4."),
}


def report_usability_decisions() -> None:
    print("\n" + "=" * 70)
    print("ANALIZA UPOTREBLJIVOSTI KOLONA - ODLUKE")
    print("=" * 70)
    for dataset_name, decisions in (
        ("movies", COLUMN_DECISIONS_MOVIES),
        ("reviews", COLUMN_DECISIONS_REVIEWS),
    ):
        print(f"\n-- {dataset_name} --")
        for col, (decision, reason) in decisions.items():
            print(f"  {col:20s} -> {decision:35s} | {reason}")


# ---------------------------------------------------------------------------
# KORAK 3: Čišćenje podataka
# ---------------------------------------------------------------------------

def clean_movies(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # Uklanjanje potpunih duplikata
    df = df.drop_duplicates()

    # id i title su ključne kolone - bez njih dokument nema smisla
    df = df.dropna(subset=["id", "title"])

    # Uklanjanje duplikata po prirodnom ključu (id) - zadržati prvi zapis
    df = df.drop_duplicates(subset=["id"], keep="first")

    # Standardizacija tekstualnih vrednosti - trim whitespace
    text_cols = ["title", "genre", "director", "writer", "cast",
                 "rating", "originalLanguage", "distributor"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Nekonzistentni zapisi: runtimeMinutes <= 0 nema smisla
    if "runtimeMinutes" in df.columns:
        df.loc[df["runtimeMinutes"] <= 0, "runtimeMinutes"] = np.nan

    after = len(df)
    print(f"[clean_movies] {before:,} -> {after:,} redova "
          f"(uklonjeno {before - after:,})")
    return df


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    df = df.drop_duplicates()

    # id (FK na film) i reviewId su ključne kolone
    df = df.dropna(subset=["id"])
    if "reviewId" in df.columns:
        df = df.drop_duplicates(subset=["reviewId"], keep="first")

    text_cols = ["criticName", "publicationName", "reviewText", "reviewState"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    after = len(df)
    print(f"[clean_reviews] {before:,} -> {after:,} redova "
          f"(uklonjeno {before - after:,})")
    return df


# ---------------------------------------------------------------------------
# KORAK 4: Dopuna podataka (imputation)
# ---------------------------------------------------------------------------

def parse_box_office(value) -> float | None:
    """'$111.3M' -> 111300000.0 ; '$950K' -> 950000.0 ; NaN -> None"""
    if pd.isna(value):
        return None
    value = str(value).strip().replace("$", "").replace(",", "")
    match = re.match(r"^([\d.]+)\s*([MK]?)$", value, re.IGNORECASE)
    if not match:
        return None
    number, suffix = match.groups()
    number = float(number)
    multiplier = {"M": 1_000_000, "K": 1_000}.get(suffix.upper(), 1)
    return number * multiplier


def parse_original_score(value) -> float | None:
    """
    Normalizuje heterogene formate ocena kritičara na skalu 0.0-1.0:
      '3/5'    -> 0.6
      '85/100' -> 0.85
      'B+'     -> 0.85 (mapa slovnih ocena)
      '4 stars' / '4/5 stars' -> pokušaj parsiranja razlomka
    Ako format nije prepoznat, vraća None (NE nulu - nepoznato != najgore).
    """
    if pd.isna(value):
        return None
    value = str(value).strip()

    # Format "x/y"
    fraction_match = re.match(r"^(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)", value)
    if fraction_match:
        numerator, denominator = map(float, fraction_match.groups())
        if denominator > 0:
            return round(numerator / denominator, 4)

    # Format slovne ocene (A+, B-, C, ...)
    upper_val = value.upper().replace(" ", "")
    if upper_val in LETTER_GRADE_MAP:
        return LETTER_GRADE_MAP[upper_val]

    # Format čistog broja 0-100 (pretpostavka: procenat)
    if re.match(r"^\d+(\.\d+)?$", value):
        number = float(value)
        return round(number / 100, 4) if number > 1 else number

    return None


def split_to_list(value) -> list[str]:
    """'Comedy, Family' -> ['Comedy', 'Family']; NaN/'' -> []"""
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def impute_movies(df: pd.DataFrame) -> pd.DataFrame:
    # Tekstualne kolone -> "Unknown"
    for col in ["rating", "originalLanguage", "distributor"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Numeričke kolone: audienceScore/tomatoMeter - median po žanru bi bio
    # idealan, ali pre toga genre mora biti transformisan u listu (korak 6).
    # Ovde primenjujemo globalni median kao razuman kompromis za fazu pre
    # transformacije; obrazloženje: ekstremno odstupanje (skew) nije
    # očekivano za score kolone u rasponu 0-100.
    for col in ["audienceScore", "tomatoMeter"]:
        if col in df.columns:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            print(f"[impute_movies] {col}: NULL popunjen medianom = {median_val}")

    # boxOffice ostaje NULL ako nepoznat (parsira se kasnije u transform_movies)
    return df


def impute_reviews(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["criticName", "publicationName"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    if "isTopCritic" in df.columns:
        df["isTopCritic"] = df["isTopCritic"].fillna(False)

    # reviewText: NULL -> "" (prazan string), ne "Unknown" (nije kategorička vrednost)
    if "reviewText" in df.columns:
        df["reviewText"] = df["reviewText"].fillna("")

    return df


# ---------------------------------------------------------------------------
# KORAK 5: Redukcija skupa podataka
# ---------------------------------------------------------------------------

MOVIES_DROP_COLUMNS = ["soundMix"]
REVIEWS_DROP_COLUMNS = ["reviewUrl"]


def reduce_movies(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [c for c in MOVIES_DROP_COLUMNS if c in df.columns]
    print(f"[reduce_movies] Uklanjam kolone: {cols_to_drop} "
          f"(nisu relevantne za 10 agregacionih pitanja)")
    return df.drop(columns=cols_to_drop)


def reduce_reviews(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [c for c in REVIEWS_DROP_COLUMNS if c in df.columns]
    print(f"[reduce_reviews] Uklanjam kolone: {cols_to_drop} "
          f"(URL recenzije nije analitički relevantan)")
    return df.drop(columns=cols_to_drop)


# ---------------------------------------------------------------------------
# KORAK 6: Transformacija podataka
# ---------------------------------------------------------------------------

def transform_movies(df: pd.DataFrame) -> pd.DataFrame:
    # Normalizacija naziva kolona (stvarni CSV nazivi -> ciljna MongoDB šema)
    df = df.rename(columns={
        "id": "ratingKey",
        "title": "movie_title",
        "director": "directors",
        "writer": "authors",
        "cast": "actors",
        "distributor": "studio",
    })

    # Comma-separated stringovi -> nizovi
    for col in ["genre", "directors", "authors", "actors"]:
        if col in df.columns:
            df[col] = df[col].apply(split_to_list)

    # Konverzija datuma
    for col in ["releaseDateTheaters", "releaseDateStreaming"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Izdvajanje releaseYear (koristi se u nekoliko upita za grupisanje po deceniji)
    if "releaseDateTheaters" in df.columns:
        df["releaseYear"] = df["releaseDateTheaters"].dt.year

    # Parsiranje boxOffice u numeričku vrednost
    if "boxOffice" in df.columns:
        df["boxOffice"] = df["boxOffice"].apply(parse_box_office)

    # Tipizacija score kolona
    for col in ["tomatoMeter", "audienceScore", "runtimeMinutes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def transform_reviews(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "id": "ratingKey",
        "criticName": "critic_name",
        "publicationName": "publisher_name",
        "isTopCritic": "top_critic",
        "creationDate": "review_date",
        "reviewText": "review_content",
        "reviewState": "review_state",
    })

    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")

    if "top_critic" in df.columns:
        df["top_critic"] = df["top_critic"].astype(bool)

    if "originalScore" in df.columns:
        df["review_score"] = df["originalScore"].apply(parse_original_score)
        df = df.drop(columns=["originalScore"])

    if "review_state" in df.columns:
        df["review_state"] = df["review_state"].str.lower()

    return df


# ---------------------------------------------------------------------------
# KORAK 7 + 8: Kreiranje finalnih dokumenata i import u MongoDB
# ---------------------------------------------------------------------------

def dataframe_to_documents(df: pd.DataFrame) -> list[dict]:
    """Konvertuje DataFrame u listu dict-ova spremnih za MongoDB insert,
    uz čišćenje NaT/NaN vrednosti (pymongo ne ume da serijalizuje NaN/NaT)."""
    df = df.replace({np.nan: None, pd.NaT: None})
    records = df.to_dict(orient="records")

    # pandas Timestamp -> python datetime (pymongo zahteva native tip)
    for record in records:
        for key, value in record.items():
            if isinstance(value, pd.Timestamp):
                record[key] = value.to_pydatetime()
    return records


def bulk_insert(collection, documents: list[dict], label: str) -> None:
    total = len(documents)
    inserted = 0
    for start in range(0, total, INSERT_BATCH_SIZE):
        batch = documents[start:start + INSERT_BATCH_SIZE]
        collection.insert_many(batch, ordered=False)
        inserted += len(batch)
        print(f"[{label}] uneto {inserted:,}/{total:,}", end="\r")
    print(f"[{label}] uneto {inserted:,}/{total:,} - ZAVRŠENO")


# ---------------------------------------------------------------------------
# Glavni ETL tok
# ---------------------------------------------------------------------------

def run_etl(movies_csv: str, reviews_csv: str, mongo_uri: str, db_name: str) -> None:
    client = MongoClient(mongo_uri)
    db = client[db_name]

    # ---- MOVIES ----
    print("\n### Učitavanje movies.csv ###")
    movies_df = pd.read_csv(movies_csv, low_memory=False)

    analyze_quality(movies_df, "movies (sirovi podaci)")
    report_usability_decisions()

    movies_df = clean_movies(movies_df)
    movies_df = impute_movies(movies_df)
    movies_df = reduce_movies(movies_df)
    movies_df = transform_movies(movies_df)

    analyze_quality(movies_df, "movies (nakon ETL-a)")

    db.movies.drop()
    bulk_insert(db.movies, dataframe_to_documents(movies_df), "movies")

    # ---- REVIEWS (čitanje u chunk-ovima zbog veličine fajla) ----
    print("\n### Učitavanje reviews.csv (chunked) ###")
    db.reviews.drop()

    total_inserted = 0
    first_chunk = True
    for chunk in pd.read_csv(reviews_csv, chunksize=CHUNK_SIZE, low_memory=False):
        if first_chunk:
            analyze_quality(chunk, "reviews (prvi chunk - ilustracija)")
            first_chunk = False

        chunk = clean_reviews(chunk)
        chunk = impute_reviews(chunk)
        chunk = reduce_reviews(chunk)
        chunk = transform_reviews(chunk)

        documents = dataframe_to_documents(chunk)
        if documents:
            db.reviews.insert_many(documents, ordered=False)
            total_inserted += len(documents)
            print(f"[reviews] ukupno uneto: {total_inserted:,}", end="\r")

    print(f"[reviews] ukupno uneto: {total_inserted:,} - ZAVRŠENO")
    print("\nETL proces završen.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL proces: CSV -> MongoDB")
    parser.add_argument("--movies", required=True, help="Putanja do rotten_tomatoes_movies.csv")
    parser.add_argument("--reviews", required=True, help="Putanja do rotten_tomatoes_movie_reviews.csv")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="rt_analytics")
    args = parser.parse_args()

    run_etl(args.movies, args.reviews, args.mongo_uri, args.db)
