import pandas as pd
import numpy as np

# Generate 1 million rows of sample employee salary data
rows = 1_000_000
data = {
    "id": np.arange(1, rows + 1),
    "name": [f"Employee_{i}" for i in range(1, rows + 1)],
    "salary": np.random.randint(50000, 200000, size=rows)
}

df = pd.DataFrame(data)

# Save as CSV
df.to_csv("large_data.csv", index=False)

# Save as Parquet (recommended for BigQuery)
try:
    df.to_parquet("large_data.parquet", engine="pyarrow")
except ImportError as e:
    print("Parquet support requires `pyarrow` or `fastparquet`. Install one via pip.")
    raise e
