# Вычисление ground truth — точные 100 ближайших соседей для каждого запроса
# Используется brute-force (IndexFlatL2) из faiss для получения эталонных результатов.
# Результат сохраняется в data/ground_truth.npy и переиспользуется в benchmark.py

import os
import time

import faiss
import numpy as np

from download_dataset import load_sift, read_ivecs, SIFT_DIR

K = 100  # количество ближайших соседей

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
GROUND_TRUTH_PATH = os.path.join(DATA_DIR, "ground_truth.npy")


def compute_ground_truth(base: np.ndarray, query: np.ndarray, k: int = K) -> tuple[np.ndarray, np.ndarray]:
    """Вычисляет точные k ближайших соседей для каждого запроса методом полного перебора.

    Returns:
        (distances, indices) — матрицы shape (n_queries, k)
    """
    dim = base.shape[1]
    index = faiss.IndexFlatL2(dim)

    print(f"Добавляю {base.shape[0]} векторов в индекс (dim={dim})...")
    t0 = time.perf_counter()
    index.add(base)
    add_time = time.perf_counter() - t0
    print(f"Добавлено за {add_time:.2f} сек, всего в индексе: {index.ntotal}")

    print(f"Ищу {k} ближайших соседей для {query.shape[0]} запросов...")
    t0 = time.perf_counter()
    distances, indices = index.search(query, k)
    search_time = time.perf_counter() - t0
    print(f"Поиск завершён за {search_time:.2f} сек")

    return distances, indices


def verify_against_dataset_gt(computed_indices: np.ndarray) -> None:
    """Сравнивает вычисленный ground truth с тем, что идёт в комплекте с SIFT1M."""
    gt_path = os.path.join(SIFT_DIR, "sift_groundtruth.ivecs")
    if not os.path.exists(gt_path):
        print("Файл sift_groundtruth.ivecs не найден, пропускаю верификацию")
        return

    dataset_gt = read_ivecs(gt_path)  # shape (10000, 100)
    # Сравниваем только top-K (в датасете тоже 100 соседей)
    k = min(computed_indices.shape[1], dataset_gt.shape[1])

    match_count = 0
    total = computed_indices.shape[0]
    for i in range(total):
        computed_set = set(computed_indices[i, :k])
        dataset_set = set(dataset_gt[i, :k])
        match_count += len(computed_set & dataset_set)

    recall = match_count / (total * k)
    print(f"Верификация: recall совпадения с эталоном датасета = {recall:.4f}")
    if recall > 0.99:
        print("Ground truth корректен!")
    else:
        print("ВНИМАНИЕ: расхождение с эталоном датасета — проверьте данные")


def main():
    base, query = load_sift()
    print(f"Base:  {base.shape}, dtype={base.dtype}")
    print(f"Query: {query.shape}, dtype={query.dtype}")

    distances, indices = compute_ground_truth(base, query, k=K)
    print(f"Результат: distances.shape={distances.shape}, indices.shape={indices.shape}")

    os.makedirs(DATA_DIR, exist_ok=True)
    np.save(GROUND_TRUTH_PATH, indices)
    print(f"Ground truth сохранён в {GROUND_TRUTH_PATH}")

    # Проверяем корректность, сравнив с GT из датасета
    verify_against_dataset_gt(indices)

    # Статистика
    print(f"\nСтатистика расстояний до k={K} соседей:")
    print(f"  Мин. расстояние (L2): {distances[:, 0].mean():.2f} (среднее по 1-му соседу)")
    print(f"  Макс. расстояние (L2): {distances[:, -1].mean():.2f} (среднее по {K}-му соседу)")


if __name__ == "__main__":
    main()
