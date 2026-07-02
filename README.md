# Analiza filmova i kritičarskih recenzija u MongoDB

Projekat iz predmeta Sistemi baza podataka. Tema je analiza velikog skupa
podataka o filmovima i recenzijama kritičara sa Rotten Tomatoes-a, sa fokusom
na optimizaciju agregacionih upita kroz restruktuiranje šeme i indeksiranje.

**Tim:**
- Mina Vojnović — uloga: menadžer filmske produkcije
- Marija Olić — uloga: analitičar podataka

**Dataset:** [Massive Rotten Tomatoes Movies & Reviews](https://www.kaggle.com/datasets/andrezaza/clapper-massive-rotten-tomatoes-movies-and-reviews)
(~143.000 filmova, ~1.4M recenzija, preko 100MB)

Svaka od nas je postavila po 5 pitanja iz perspektive svoje uloge. Svako pitanje
je prvo rešeno nad polaznom (normalizovanom) šemom sa dve kolekcije, pa
optimizovano i izmereno pre/posle. Pitanja su malo izmenjena u odnosu na inicijalni predlog koje smo imale na prezentovanju ideje o projektu.

---

## Struktura projekta
```
project/
├── data/raw/            CSV fajlovi sa Kaggle-a (NISU na git-u, vidi .gitignore)
├── scripts/
│   ├── etl.py                       ucitavanje i ciscenje CSV -> MongoDB
│   ├── build_movies_with_stats.py   pravljenje denormalizovane kolekcije
│   ├── create_indexes.py            kreiranje indeksa
│   ├── run_performance_analysis.py  merenje pre/posle + uporedna tabela
│   ├── make_diagram.py              dijagram pre/posle od izmerene tabele
│   └── show_query_results.py        ispis rezultata upita
├── queries/
│   ├── unoptimized/     pitanja nad movies + reviews (sa $lookup-om)
│   └── optimized/       ista pitanja nad movies_with_stats
├── results/             izmerena tabela, dijagram (Explain u obliku JSON)
|                
└── README.md

---
```

## Šeme

Polazna, **normalizovana** šema ima dve kolekcije:

- `movies` — podaci o filmu: `ratingKey`, `movie_title`, `tomatoMeter`,
  `audienceScore`, `genre` (niz), `releaseYear`, `directors`, `studio`...
- `reviews` — pojedinačne recenzije: `ratingKey` (referenca na film),
  `critic_name`, `publicatioName`, `top_critic`, `review_state`,
  `review_score`, `review_date`...

Veza je jedan-prema-više preko `ratingKey`. Da bi se film povezao sa svojim
recenzijama, neoptimizovani upiti koriste `$lookup`, što je nad 1.4M recenzija
bez efikasnog pristupa jako sporo — to je usko grlo koje optimizacija rešava.

Kao optimizaciju napravila sam **denormalizovanu** kolekciju
`movies_with_stats`: za svaki film su u isti dokument ugnježdene njegove
recenzije (`reviews` niz) i pred-izračunate agregatne vrednosti (`reviewCount`,
`freshCount`, `rottenCount`, `topCriticReviewCount`, `avgReviewScore`). Tako
većina upita više ne mora `$lookup` — sve što im treba je u dokumentu filma.

---

## Pokretanje

Treba da budu instalirani MongoDB i Python 3.10+, pa:

```bash
pip install -r requirements.txt
```

Sve komande se pokreću iz korena projekta. Redosled je bitan jer svaki korak
zavisi od prethodnog.

### 1. Ucitavanje podataka (ETL)

CSV fajlove sa Kaggle-a prvo staviti u `data/raw/`, pa:

```bash
python scripts/etl.py --movies data/raw/rotten_tomatoes_movies.csv --reviews data/raw/rotten_tomatoes_movie_reviews.csv --db rt_analytics
```

Skripta ispisuje analizu kvaliteta podataka (broj zapisa, null vrednosti,
duplikati, odluke o kolonama) i puni kolekcije `movies` i `reviews`. Reviews
fajl se cita u chunk-ovima da ne bi pretrpao memoriju. Traje nekoliko minuta
zbog velicine.

### 2. Izgradnja optimizovane kolekcije

```bash
python scripts/build_movies_with_stats.py --db rt_analytics
```

Pravi `movies_with_stats` ($lookup + $merge na strani servera). Ovo je
najsporiji korak jer spaja sve recenzije sa filmovima.

### 3. Kreiranje indeksa

```bash
python scripts/create_indexes.py --db rt_analytics
```

Kreira indekse za sve tri kolekcije: single-field, multikey (nad nizovima
poput `genre` i ugnježdenih recenzija), compound po ESR pravilu i unique na
`ratingKey`. Pokrenuti bez dodatnih flegova da bi se napravili i indeksi za
polaznu šemu (potrebni za $lookup).

### 4. Merenje performansi

```bash
python scripts/run_performance_analysis.py --db rt_analytics --samo-mina
```

Za svaki upit poziva `explain("executionStats")`, izdvaja vreme izvršavanja i
broj pregledanih dokumenata, i pravi uporednu tabelu pre/posle u
`results/performance_comparison.md` i `.csv`. Po defaultu ne dira indekse (meri
nad postojecim stanjem). `--samo-mina` pokrece samo moje upite; bez tog flega
idu i Marijinih 5.

> Napomena: neoptimizovani upiti su namerno spori (COLLSCAN + `$lookup` nad
> milionima recenzija bez efikasnog pristupa). Zato ovaj korak ume da traje i
> vise od 15 minuta — upravo ta sporost je usko grlo koje optimizacija resava.

### 5. Dijagram (opciono)

```bash
python scripts/make_diagram.py
```

Od `results/performance_comparison.csv` pravi bar chart pre/posle
(`results/performance_diagram.png`). Koristi logaritamsku skalu jer se vremena
razlikuju za redove velicine (neopt ide u stotine hiljada ms, opt u stotine).

---

## Rezultati

Uporedna tabela se generise automatski. Kod svih upita merim i vreme (ms) i
broj pregledanih dokumenata (`totalDocsExamined`), jer broj pregledanih
dokumenata najjasnije pokazuje prelazak sa COLLSCAN na IXSCAN.

Apsolutno vreme varira izmedju pokretanja zbog kesiranja i opterecenja sistema,
pa se kao stabilan pokazatelj optimizacije oslanjam na `totalDocsExamined` i
tip skeniranja (COLLSCAN vs IXSCAN), koji su konzistentni.

Optimizacija je kombinacija dve tehnike koje predmet trazi:

- **restruktuiranje šeme** (denormalizacija u `movies_with_stats`) — uklanja
  `$lookup` kod vecine upita
- **indeksiranje** — kod upita koji zadrzava `$lookup` indeks na
  `reviews.ratingKey` ubrzava spajanje, a kod ostalih indeksi ubrzavaju pocetni
  `$match` (npr. Q1 koristi `reviewCount` indeks i radi IXSCAN)

---

## Metabase

Vizualizacije rezultata upita radjene su u Metabase-u, povezanom na
`rt_analytics` bazu, nad denormalizovanom kolekcijom `movies_with_stats`.

Metabase se pokrece iz `metabase` foldera (nije deo git-a zbog velicine .jar
fajla):

​```bash
cd metabase
$env:MB_JETTY_PORT=3004; java -jar metabase.jar
​```

Nakon pokretanja, Metabase je dostupan na `http://localhost:3004`. Grafikoni
su radjeni preko native query opcije (JSON pipeline nad `movies_with_stats`).


## Napomena o podacima

CSV fajlovi sa Kaggle-a su preko 100MB i nisu deo repozitorijuma (GitHub odbija
fajlove te velicine). Treba ih preuzeti sa linka na vrhu i staviti u `data/raw/`
pre pokretanja ETL-a. 
Kao GUI klijent za pregled baze i `explain` analizu koriscen je MongoDB Compass.