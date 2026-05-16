# Бенчмарк ANN-алгоритмов: LSH, HNSW, IVF+PQ
#
# Для каждой конфигурации измеряем:
#   - recall@100: доля правильных соседей (пересечение с ground truth / 100)
#   - время индексации (сек)
#   - размер индекса (MB) - через faiss.write_index во временный файл
#   - скорость поиска (QPS - queries per second)

import json
import os
import tempfile
import time

import faiss
import numpy as np

from download_dataset import load_sift

K = 100  # top-K соседей
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
GROUND_TRUTH_PATH = os.path.join(DATA_DIR, "ground_truth.npy")
RESULTS_PATH = os.path.join(RESULTS_DIR, "benchmark_results.json")


def compute_recall(predicted: np.ndarray, ground_truth: np.ndarray) -> float:
    """Recall@K: средняя доля правильных соседей по всем запросам."""
    n = predicted.shape[0]
    k = ground_truth.shape[1]
    total = 0
    for i in range(n):
        total += len(set(predicted[i]) & set(ground_truth[i]))
    return total / (n * k)


def measure_index_size(index) -> float:
    """Размер индекса в MB (сериализация во временный файл)."""
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=True) as f:
        faiss.write_index(index, f.name)
        size_bytes = os.path.getsize(f.name)
    return size_bytes / (1024 * 1024)


def search_with_timing(index, query: np.ndarray, k: int) -> tuple[np.ndarray, np.ndarray, float]:
    """Поиск k соседей с замером времени. Возвращает (D, I, qps)."""
    t0 = time.perf_counter()
    D, I = index.search(query, k)
    elapsed = time.perf_counter() - t0
    qps = query.shape[0] / elapsed
    return D, I, qps


def log_result(results: list, algo: str, params: dict,
               recall: float, index_time: float, qps: float, size_mb: float):
    """Добавляет результат в список и печатает в консоль."""
    entry = {
        "algorithm": algo,
        "params": params,
        "recall": round(recall, 4),
        "index_time_s": round(index_time, 2),
        "search_qps": round(qps, 1),
        "index_size_mb": round(size_mb, 2),
    }
    results.append(entry)
    print(f"  {algo} {params} => recall={recall:.4f}  QPS={qps:.0f}  "
          f"index={size_mb:.1f}MB  build={index_time:.1f}s")



def benchmark_lsh(base: np.ndarray, query: np.ndarray,
                  gt: np.ndarray, results: list):
    """LSH — faiss.IndexLSH с перебором nbits."""
    dim = base.shape[1]
    nbits_list = [128, 256, 512, 1024, 2048]

    print("\n=== LSH ===")
    for nbits in nbits_list:
        index = faiss.IndexLSH(dim, nbits)

        t0 = time.perf_counter()
        index.add(base)
        index_time = time.perf_counter() - t0

        _, I, qps = search_with_timing(index, query, K)
        recall = compute_recall(I, gt)
        size_mb = measure_index_size(index)

        log_result(results, "LSH", {"nbits": nbits},
                   recall, index_time, qps, size_mb)


def benchmark_hnsw(base: np.ndarray, query: np.ndarray,
                   gt: np.ndarray, results: list):
    """HNSW — faiss.IndexHNSWFlat с перебором M, efConstruction, efSearch."""
    dim = base.shape[1]
    M_list = [8, 16, 32, 64]
    efConstruction_list = [40, 100, 200, 500]
    efSearch_list = [16, 32, 64, 128, 256, 512]

    print("\n=== HNSW ===")
    for M in M_list:
        for efC in efConstruction_list:
            index = faiss.IndexHNSWFlat(dim, M)
            index.hnsw.efConstruction = efC

            # Построение графа - один раз для пары (M, efConstruction)
            t0 = time.perf_counter()
            index.add(base)
            index_time = time.perf_counter() - t0

            size_mb = measure_index_size(index)

            # Перебор efSearch без переиндексации
            for efS in efSearch_list:
                index.hnsw.efSearch = efS
                _, I, qps = search_with_timing(index, query, K)
                recall = compute_recall(I, gt)

                log_result(results, "HNSW",
                           {"M": M, "efConstruction": efC, "efSearch": efS},
                           recall, index_time, qps, size_mb)


def benchmark_ivfpq(base: np.ndarray, query: np.ndarray,
                    gt: np.ndarray, results: list):
    """IVF+PQ — faiss.IndexIVFPQ с перебором nlist, m_pq, nprobe."""
    dim = base.shape[1]
    nlist_list = [100, 256, 512, 1024]
    m_pq_list = [8, 16, 32, 64]
    nprobe_list = [1, 5, 10, 20, 50, 100, 200]

    print("\n=== IVF+PQ ===")
    for nlist in nlist_list:
        for m_pq in m_pq_list:
            quantizer = faiss.IndexFlatL2(dim)
            index = faiss.IndexIVFPQ(quantizer, dim, nlist, m_pq, 8)

            # Обучение + добавление - один раз для пары (nlist, m_pq)
            t0 = time.perf_counter()
            index.train(base)
            index.add(base)
            index_time = time.perf_counter() - t0

            size_mb = measure_index_size(index)

            # Перебор nprobe без переиндексации
            for nprobe in nprobe_list:
                if nprobe > nlist:
                    continue
                index.nprobe = nprobe
                _, I, qps = search_with_timing(index, query, K)
                recall = compute_recall(I, gt)

                log_result(results, "IVF+PQ",
                           {"nlist": nlist, "m_pq": m_pq, "nprobe": nprobe},
                           recall, index_time, qps, size_mb)


def benchmark_opq_ivfpq(base: np.ndarray, query: np.ndarray,
                        gt: np.ndarray, results: list):
    """OPQ+IVF+PQ — OPQ предвращение + IVF+PQ для лучшего recall."""
    dim = base.shape[1]
    nlist_list = [256, 1024]
    m_pq_list = [32, 64]
    nprobe_list = [10, 20, 50, 100, 200]

    print("\n=== OPQ+IVF+PQ ===")
    for nlist in nlist_list:
        for m_pq in m_pq_list:
            # OPQ вращает пространство для минимизации ошибки квантизации
            opq = faiss.OPQMatrix(dim, m_pq)
            quantizer = faiss.IndexFlatL2(dim)
            sub_index = faiss.IndexIVFPQ(quantizer, dim, nlist, m_pq, 8)
            index = faiss.IndexPreTransform(opq, sub_index)

            t0 = time.perf_counter()
            index.train(base)
            index.add(base)
            index_time = time.perf_counter() - t0

            size_mb = measure_index_size(index)

            for nprobe in nprobe_list:
                if nprobe > nlist:
                    continue
                sub_index.nprobe = nprobe
                _, I, qps = search_with_timing(index, query, K)
                recall = compute_recall(I, gt)

                log_result(results, "OPQ+IVF+PQ",
                           {"nlist": nlist, "m_pq": m_pq, "nprobe": nprobe},
                           recall, index_time, qps, size_mb)


def main():
    # Загрузка данных
    base, query = load_sift()
    print(f"Base:  {base.shape}, Query: {query.shape}")

    # Загрузка ground truth
    if not os.path.exists(GROUND_TRUTH_PATH):
        print(f"ОШИБКА: {GROUND_TRUTH_PATH} не найден. Сначала запустите compute_ground_truth.py")
        return
    gt = np.load(GROUND_TRUTH_PATH)
    print(f"Ground truth: {gt.shape}")

    results: list[dict] = []

    # Запуск бенчмарков
    benchmark_lsh(base, query, gt, results)
    benchmark_hnsw(base, query, gt, results)
    benchmark_ivfpq(base, query, gt, results)
    # benchmark_opq_ivfpq(base, query, gt, results)

    # Сохранение результатов
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nРезультаты сохранены в {RESULTS_PATH} ({len(results)} конфигураций)")


if __name__ == "__main__":
    main()
