import pandas as pd
import os
root = "~/pyhealth_bak"

person_df = pd.read_csv(
    os.path.join(root, "person.csv"),
    dtype={"person_id": str},
    nrows=1000,
    sep=",",
)

print(person_df.shape)
print(person_df.head())
