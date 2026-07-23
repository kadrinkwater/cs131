import time
import pandas as pd

FILE = "data/clickstream-enwiki-2026-06.tsv"

start = time.time()

try:
    df = pd.read_csv(
        FILE,
        sep="\t",
        names=["prev", "curr", "type", "count"],
        header=None,
        on_bad_lines="warn",
        low_memory=False,
    )

    elapsed = time.time() - start

    print("Loaded successfully")
    print(f"Rows loaded: {len(df):,}")
    print(f"Runtime: {elapsed:.2f} seconds")
    print(
        f"DataFrame memory: "
        f"{df.memory_usage(deep=True).sum() / (1024 ** 3):.2f} GB"
    )

except MemoryError:
    elapsed = time.time() - start
    print("FAILED: MemoryError")
    print(f"Runtime: {elapsed:.2f} seconds")

except Exception as error:
    elapsed = time.time() - start
    print("FAILED")
    print(f"{type(error).__name__}: {error}")
    print(f"Runtime: {elapsed:.2f} seconds")
