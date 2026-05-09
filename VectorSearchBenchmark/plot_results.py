# Визуализация результатов бенчмарка ANN-алгоритмов

import json
import os

import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
RESULTS_PATH = os.path.join(RESULTS_DIR, "benchmark_results.json")

ALGO_STYLE = {
    "LSH":    {"color": "tab:red",   "marker": "s"},
    "HNSW":   {"color": "tab:blue",  "marker": "o"},
    "IVF+PQ": {"color": "tab:green", "marker": "^"},
}


def load_results() -> list[dict]:
    with open(RESULTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def split_by_algo(results: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for r in results:
        groups.setdefault(r["algorithm"], []).append(r)
    return groups


# ---------------------------------------------------------------------------
# Графики
# ---------------------------------------------------------------------------

def plot_recall_vs_qps(groups: dict[str, list[dict]]):
    """График 1: Recall@100 vs QPS (лог-шкала по Y)."""
    plt.figure(figsize=(10, 6))
    for algo, entries in groups.items():
        style = ALGO_STYLE.get(algo, {"color": "gray", "marker": "x"})
        recalls = [e["recall"] for e in entries]
        qps = [e["search_qps"] for e in entries]
        plt.scatter(recalls, qps, label=algo, alpha=0.6, **style)

    plt.xlabel("Recall@100")
    plt.ylabel("Queries per Second (QPS)")
    plt.yscale("log")
    plt.title("Recall vs QPS")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, "recall_vs_qps.png"), dpi=150)
    plt.close()
    print("Сохранён recall_vs_qps.png")


def plot_recall_vs_size(groups: dict[str, list[dict]]):
    """График 2: Recall@100 vs размер индекса (MB)."""
    plt.figure(figsize=(10, 6))
    for algo, entries in groups.items():
        style = ALGO_STYLE.get(algo, {"color": "gray", "marker": "x"})
        recalls = [e["recall"] for e in entries]
        sizes = [e["index_size_mb"] for e in entries]
        plt.scatter(recalls, sizes, label=algo, alpha=0.6, **style)

    plt.xlabel("Recall@100")
    plt.ylabel("Размер индекса (MB)")
    plt.title("Recall vs Index Size")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, "recall_vs_size.png"), dpi=150)
    plt.close()
    print("Сохранён recall_vs_size.png")


def plot_recall_vs_build_time(groups: dict[str, list[dict]]):
    """График 3: Recall@100 vs время индексации (сек)."""
    plt.figure(figsize=(10, 6))
    for algo, entries in groups.items():
        style = ALGO_STYLE.get(algo, {"color": "gray", "marker": "x"})
        recalls = [e["recall"] for e in entries]
        times = [e["index_time_s"] for e in entries]
        plt.scatter(recalls, times, label=algo, alpha=0.6, **style)

    plt.xlabel("Recall@100")
    plt.ylabel("Время индексации (сек)")
    plt.title("Recall vs Indexing Time")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, "recall_vs_build_time.png"), dpi=150)
    plt.close()
    print("Сохранён recall_vs_build_time.png")


# ---------------------------------------------------------------------------
# Таблица лучших конфигураций
# ---------------------------------------------------------------------------

def print_best_configs(groups: dict[str, list[dict]]):
    """Для каждого алгоритма: лучшая конфигурация с recall >= 0.9 и макс. QPS."""
    print("\n" + "=" * 90)
    print("ЛУЧШИЕ КОНФИГУРАЦИИ (recall >= 0.9, максимальный QPS)")
    print("=" * 90)
    header = f"{'Алгоритм':<10} {'Recall':>7} {'QPS':>9} {'Index MB':>10} {'Build (s)':>10}  Параметры"
    print(header)
    print("-" * 90)

    for algo, entries in groups.items():
        # Фильтруем по recall >= 0.9
        good = [e for e in entries if e["recall"] >= 0.9]
        if not good:
            # Если ни одна конфигурация не достигает 0.9 — берём лучшую по recall
            best = max(entries, key=lambda e: e["recall"])
            note = " (best recall < 0.9)"
        else:
            best = max(good, key=lambda e: e["search_qps"])
            note = ""

        params_str = ", ".join(f"{k}={v}" for k, v in best["params"].items())
        print(f"{algo:<10} {best['recall']:>7.4f} {best['search_qps']:>9.0f} "
              f"{best['index_size_mb']:>10.1f} {best['index_time_s']:>10.1f}  "
              f"{params_str}{note}")

    print("=" * 90)

    # Абсолютный лучший по recall
    all_entries = [e for entries in groups.values() for e in entries]
    best_recall = max(all_entries, key=lambda e: e["recall"])
    best_qps = max(all_entries, key=lambda e: e["search_qps"])

    print(f"\nАбсолютный лучший recall: {best_recall['algorithm']} "
          f"{best_recall['params']} => {best_recall['recall']:.4f}")
    print(f"Абсолютный лучший QPS:    {best_qps['algorithm']} "
          f"{best_qps['params']} => {best_qps['search_qps']:.0f}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    results = load_results()
    print(f"Загружено {len(results)} результатов из {RESULTS_PATH}")

    groups = split_by_algo(results)
    for algo, entries in groups.items():
        print(f"  {algo}: {len(entries)} конфигураций")

    plot_recall_vs_qps(groups)
    plot_recall_vs_size(groups)
    plot_recall_vs_build_time(groups)
    print_best_configs(groups)


if __name__ == "__main__":
    main()
