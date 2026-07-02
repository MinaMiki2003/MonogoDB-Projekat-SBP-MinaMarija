"""
dijagram uporedne analize performansi (pre/posle optimizacije).
 results/performance_comparison.csv -> bar chart.



"""

import csv
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter

# Putanja do CSV-a (podesi ako ti je drugacija)
CSV_PATH = Path("results/performance_comparison.csv")
OUTPUT_PATH = Path("results/performance_diagram.png")

# Citanje CSV-a
labels = []
pre = []
posle = []

with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # skracujemo ime upita za citljivost (Mina_Q1_zanrovi... -> Q1)
        naziv = row["upit"]
        kratko = naziv.split("_")[1] if "_" in naziv else naziv  # Q1, Q2...
        labels.append(kratko)
        pre.append(float(row["vreme_pre_ms"]))
        posle.append(float(row["vreme_posle_ms"]))

x = np.arange(len(labels))
sirina = 0.38

fig, ax = plt.subplots(figsize=(10, 6))

bar1 = ax.bar(x - sirina/2, pre, sirina, label="Pre optimizacije",
              color="#d9534f")
bar2 = ax.bar(x + sirina/2, posle, sirina, label="Posle optimizacije",
              color="#5cb85c")

# Log skala jer su razlike ogromne (stotine hiljada vs stotine ms)
ax.set_yscale("log")
# Normalni brojevi na y-osi umesto 10^x notacije
ax.yaxis.set_major_formatter(ScalarFormatter())
ax.ticklabel_format(axis="y", style="plain")

ax.set_ylabel("Vreme izvrsavanja (ms)")
ax.set_xlabel("Upit")
ax.set_title("Uporedna analiza performansi upita - pre i posle optimizacije")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(axis="y", linestyle="--", alpha=0.4)

# Ispis vrednosti iznad svakog stubica
def oznaci(bars):
    for b in bars:
        h = b.get_height()
        ax.annotate(f"{int(h)}",
                    xy=(b.get_x() + b.get_width()/2, h),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=8)

oznaci(bar1)
oznaci(bar2)

plt.tight_layout()
plt.savefig(OUTPUT_PATH, dpi=150)
print(f"Dijagram sacuvan u: {OUTPUT_PATH}")