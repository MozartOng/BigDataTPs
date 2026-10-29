import pandas as pd

df = pd.read_csv("data/HI-Large_Trans.csv")
print("Shape:", df.shape)
print(df.head())
print(df.describe(include="all"))