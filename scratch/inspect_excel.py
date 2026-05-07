import pandas as pd
try:
    df = pd.read_excel("d:/Users/epinedas/Downloads/Scrip Duplicados/Tarso.xlsx")
    print("Columns:", df.columns.tolist())
    print("\nHead:\n", df.head())
    print("\nDate Column Types:")
    for col in df.columns:
        if 'fecha' in col.lower():
            print(f"{col}: {df[col].dtype}")
            print(f"Sample: {df[col].dropna().head(1).tolist()}")
except Exception as e:
    print("Error:", e)
