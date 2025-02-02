import pandas as pd
import matplotlib.pyplot as plt

plt.rc("axes", axisbelow=True)

df = pd.read_csv("results.csv")

baselinedf = pd.read_csv("s3_results.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_ylabel("Lookups per second")
ax.set_xlabel("Page size (KiB)")
ax.set_xscale("log")

ax.plot(df["page_size_kb"], df["takes_per_second"], label="parquet")
ax.plot(
    baselinedf["read_size_sectors"] * 4,
    baselinedf["iters_per_second"],
    linestyle="--",
    color="gray",
    label="baseline",
)

ax.legend()

plt.savefig("chart.png", bbox_inches="tight")
plt.close()
