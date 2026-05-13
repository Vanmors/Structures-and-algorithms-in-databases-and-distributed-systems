"""
Генерация детальных графиков:
  1. LSH: влияние nbits
  2. HNSW: влияние efSearch при фиксированных M и efConstruction
  3. HNSW: влияние M при фиксированных efConstruction и efSearch
  4. IVF+PQ: влияние nprobe при фиксированных nlist и m_pq
  5. IVF+PQ: влияние m_pq при фиксированных nlist и nprobe
  6. Pareto-front: recall vs QPS для лучших конфигураций каждого алгоритма
"""

import json
import os

import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
RESULTS_PATH = os.path.join(RESULTS_DIR, "benchmark_results.json")

with open(RESULTS_PATH, "r") as f:
    data = json.load(f)

lsh = [d for d in data if d["algorithm"] == "LSH"]
hnsw = [d for d in data if d["algorithm"] == "HNSW"]
ivfpq = [d for d in data if d["algorithm"] == "IVF+PQ"]

plt.rcParams.update({"figure.figsize": (10, 5), "font.size": 12})


# 1. LSH: влияние nbits
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
nbits = [d["params"]["nbits"] for d in lsh]
recall = [d["recall"] for d in lsh]
qps = [d["search_qps"] for d in lsh]
size = [d["index_size_mb"] for d in lsh]

axes[0].plot(nbits, recall, "o-", color="#e74c3c", linewidth=2, markersize=8)
axes[0].set_xlabel("nbits")
axes[0].set_ylabel("Recall@100")
axes[0].set_title("Recall vs nbits")
axes[0].grid(True, alpha=0.3)

axes[1].plot(nbits, qps, "s-", color="#3498db", linewidth=2, markersize=8)
axes[1].set_xlabel("nbits")
axes[1].set_ylabel("QPS")
axes[1].set_title("QPS vs nbits")
axes[1].grid(True, alpha=0.3)

axes[2].plot(nbits, size, "^-", color="#2ecc71", linewidth=2, markersize=8)
axes[2].set_xlabel("nbits")
axes[2].set_ylabel("Index Size (MB)")
axes[2].set_title("Index Size vs nbits")
axes[2].grid(True, alpha=0.3)

fig.suptitle("LSH: влияние параметра nbits", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "lsh_nbits.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("lsh_nbits.png saved")


# 2. HNSW: влияние efSearch (фикс M=32, efConstruction=200)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
subset = [d for d in hnsw if d["params"]["M"] == 32 and d["params"]["efConstruction"] == 200]
efS = [d["params"]["efSearch"] for d in subset]
recall_h = [d["recall"] for d in subset]
qps_h = [d["search_qps"] for d in subset]

ax1.plot(efS, recall_h, "o-", color="#e74c3c", linewidth=2, markersize=8)
ax1.set_xlabel("efSearch")
ax1.set_ylabel("Recall@100")
ax1.set_title("Recall vs efSearch")
ax1.grid(True, alpha=0.3)

ax2.plot(efS, qps_h, "s-", color="#3498db", linewidth=2, markersize=8)
ax2.set_xlabel("efSearch")
ax2.set_ylabel("QPS")
ax2.set_title("QPS vs efSearch")
ax2.grid(True, alpha=0.3)

fig.suptitle("HNSW: влияние efSearch (M=32, efConstruction=200)", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "hnsw_efsearch.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("hnsw_efsearch.png saved")


# 3. HNSW: влияние M (фикс efConstruction=200, efSearch=64)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
subset = [d for d in hnsw if d["params"]["efConstruction"] == 200 and d["params"]["efSearch"] == 64]
M_vals = [d["params"]["M"] for d in subset]
recall_m = [d["recall"] for d in subset]
size_m = [d["index_size_mb"] for d in subset]

ax1.plot(M_vals, recall_m, "o-", color="#e74c3c", linewidth=2, markersize=8)
ax1.set_xlabel("M")
ax1.set_ylabel("Recall@100")
ax1.set_title("Recall vs M")
ax1.grid(True, alpha=0.3)

ax2.plot(M_vals, size_m, "^-", color="#2ecc71", linewidth=2, markersize=8)
ax2.set_xlabel("M")
ax2.set_ylabel("Index Size (MB)")
ax2.set_title("Index Size vs M")
ax2.grid(True, alpha=0.3)

fig.suptitle("HNSW: влияние M (efConstruction=200, efSearch=64)", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "hnsw_m.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("hnsw_m.png saved")


# 4. HNSW: влияние efConstruction (фикс M=32, efSearch=128)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
subset = [d for d in hnsw if d["params"]["M"] == 32 and d["params"]["efSearch"] == 128]
efC_vals = [d["params"]["efConstruction"] for d in subset]
recall_c = [d["recall"] for d in subset]
build_c = [d["index_time_s"] for d in subset]

ax1.plot(efC_vals, recall_c, "o-", color="#e74c3c", linewidth=2, markersize=8)
ax1.set_xlabel("efConstruction")
ax1.set_ylabel("Recall@100")
ax1.set_title("Recall vs efConstruction")
ax1.grid(True, alpha=0.3)

ax2.plot(efC_vals, build_c, "D-", color="#9b59b6", linewidth=2, markersize=8)
ax2.set_xlabel("efConstruction")
ax2.set_ylabel("Build Time (s)")
ax2.set_title("Build Time vs efConstruction")
ax2.grid(True, alpha=0.3)

fig.suptitle("HNSW: влияние efConstruction (M=32, efSearch=128)", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "hnsw_efconstruction.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("hnsw_efconstruction.png saved")


# 5. IVF+PQ: влияние nprobe (фикс nlist=1024, m_pq=32)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
subset = [d for d in ivfpq if d["params"]["nlist"] == 1024 and d["params"]["m_pq"] == 32]
nprobe_vals = [d["params"]["nprobe"] for d in subset]
recall_p = [d["recall"] for d in subset]
qps_p = [d["search_qps"] for d in subset]

ax1.plot(nprobe_vals, recall_p, "o-", color="#e74c3c", linewidth=2, markersize=8)
ax1.set_xlabel("nprobe")
ax1.set_ylabel("Recall@100")
ax1.set_title("Recall vs nprobe")
ax1.grid(True, alpha=0.3)

ax2.plot(nprobe_vals, qps_p, "s-", color="#3498db", linewidth=2, markersize=8)
ax2.set_xlabel("nprobe")
ax2.set_ylabel("QPS")
ax2.set_title("QPS vs nprobe")
ax2.grid(True, alpha=0.3)

fig.suptitle("IVF+PQ: влияние nprobe (nlist=1024, m_pq=32)", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "ivfpq_nprobe.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("ivfpq_nprobe.png saved")


# 6. IVF+PQ: влияние m_pq (фикс nlist=256, nprobe=10)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
subset = [d for d in ivfpq if d["params"]["nlist"] == 256 and d["params"]["nprobe"] == 10]
mpq_vals = [d["params"]["m_pq"] for d in subset]
recall_mpq = [d["recall"] for d in subset]
size_mpq = [d["index_size_mb"] for d in subset]

ax1.plot(mpq_vals, recall_mpq, "o-", color="#e74c3c", linewidth=2, markersize=8)
ax1.set_xlabel("m_pq")
ax1.set_ylabel("Recall@100")
ax1.set_title("Recall vs m_pq")
ax1.grid(True, alpha=0.3)

ax2.plot(mpq_vals, size_mpq, "^-", color="#2ecc71", linewidth=2, markersize=8)
ax2.set_xlabel("m_pq")
ax2.set_ylabel("Index Size (MB)")
ax2.set_title("Index Size vs m_pq")
ax2.grid(True, alpha=0.3)

fig.suptitle("IVF+PQ: влияние m_pq (nlist=256, nprobe=10)", fontsize=14, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "ivfpq_mpq.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("ivfpq_mpq.png saved")


# 7. Общее сравнение: Recall vs QPS (Pareto-стиль)
fig, ax = plt.subplots(figsize=(12, 7))

for algo, marker, color, label in [
    (lsh, "s", "#e74c3c", "LSH"),
    (hnsw, "o", "#3498db", "HNSW"),
    (ivfpq, "^", "#2ecc71", "IVF+PQ"),
]:
    r = [d["recall"] for d in algo]
    q = [d["search_qps"] for d in algo]
    ax.scatter(r, q, marker=marker, color=color, label=label, alpha=0.6, s=50)

ax.set_xlabel("Recall@100", fontsize=13)
ax.set_ylabel("QPS (log scale)", fontsize=13)
ax.set_yscale("log")
ax.set_title("Сравнение алгоритмов: Recall vs QPS", fontsize=14, fontweight="bold")
ax.legend(fontsize=12)
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "comparison_recall_qps.png"), dpi=150, bbox_inches="tight")
plt.close(fig)
print("comparison_recall_qps.png saved")

print("\nAll plots saved to", RESULTS_DIR)
