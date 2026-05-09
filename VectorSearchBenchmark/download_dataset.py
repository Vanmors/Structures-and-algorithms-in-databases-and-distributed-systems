import os
import tarfile
import urllib.request
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
SIFT_DIR = os.path.join(DATA_DIR, "sift")
SIFT_URL = "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz"
SIFT_TAR = os.path.join(DATA_DIR, "sift.tar.gz")


def download_sift():
    """Скачивает и распаковывает SIFT1M, если ещё не скачан."""
    if os.path.isdir(SIFT_DIR) and os.listdir(SIFT_DIR):
        print("Датасет уже скачан:", SIFT_DIR)
        return

    os.makedirs(DATA_DIR, exist_ok=True)

    if not os.path.exists(SIFT_TAR):
        print(f"Скачиваю {SIFT_URL} ...")
        urllib.request.urlretrieve(SIFT_URL, SIFT_TAR)
        print("Скачано:", SIFT_TAR)

    print("Распаковываю...")
    with tarfile.open(SIFT_TAR, "r:gz") as tar:
        tar.extractall(DATA_DIR)
    print("Готово:", SIFT_DIR)


def read_fvecs(filename: str) -> np.ndarray:
    """Читает файл формата fvecs -> np.ndarray shape (n, dim), dtype=float32."""
    # первые 4 байта — int32 с размерностью вектора
    dim = int(np.fromfile(filename, dtype=np.int32, count=1)[0])
    data = np.fromfile(filename, dtype=np.float32)
    # каждая запись: 1 int32 (dim) + dim float32 = (dim + 1) float32
    return data.reshape(-1, dim + 1)[:, 1:]


def read_ivecs(filename: str) -> np.ndarray:
    """Читает файл формата ivecs -> np.ndarray shape (n, dim), dtype=int32."""
    dim = int(np.fromfile(filename, dtype=np.int32, count=1)[0])
    data = np.fromfile(filename, dtype=np.int32)
    return data.reshape(-1, dim + 1)[:, 1:]


def load_sift():
    """Загружает base и query векторы SIFT1M. Скачивает датасет при необходимости."""
    download_sift()
    base = read_fvecs(os.path.join(SIFT_DIR, "sift_base.fvecs"))
    query = read_fvecs(os.path.join(SIFT_DIR, "sift_query.fvecs"))
    return base, query


if __name__ == "__main__":
    base, query = load_sift()
    print(f"Base:  {base.shape}, dtype={base.dtype}")
    print(f"Query: {query.shape}, dtype={query.dtype}")
