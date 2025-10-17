import pandas as pd
import requests
from io import BytesIO
from pathlib import Path
from datetime import date

# -----------------------
# 設定
# -----------------------
BASE_URL = "https://www.usgs.gov/centers/national-minerals-information-center"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 要下載的金屬
COMMODITIES = {
    "aluminum": "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2022-alumi-ERT.xlsx",
    "nickel": "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2022-nickel-ERT.xlsx",
    "cobalt": "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2023-cobal-ERT.xlsx",
    "tungsten": "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2022-tungs-ERT.xlsx",
}

STAMP = date.today().isoformat()

def tidy_excel(xlsx_bytes, commodity):
    """整理 USGS 原始 Excel -> 長表"""
    try:
        xls = pd.ExcelFile(BytesIO(xlsx_bytes))
        df = pd.read_excel(xls, xls.sheet_names[0], header=None)
        # 嘗試找包含 'Country' 的那一列
        header_idx = df.index[df.iloc[:, 0].astype(str).str.contains("Country", case=False, na=False)].tolist()
        if not header_idx:
            return None
        header_idx = header_idx[0]
        df.columns = df.iloc[header_idx]
        df = df.drop(range(0, header_idx + 1))
        df = df.melt(id_vars=["Country"], var_name="Year", value_name="Production")
        df["Commodity"] = commodity
        df = df[df["Country"].notna() & df["Production"].notna()]
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df["Production"] = pd.to_numeric(df["Production"], errors="coerce")
        return df.dropna(subset=["Year", "Production"])
    except Exception as e:
        print(f"[WARN] {commodity} 無法解析：{e}")
        return None

def save_csv(df, basename):
    latest = OUTPUT_DIR / f"{basename}_latest.csv"
    dated  = OUTPUT_DIR / f"{basename}_{STAMP}.csv"
    df.to_csv(latest, index=False)
    df.to_csv(dated, index=False)
    print("[WRITE]", latest.name, "rows:", len(df))

def run():
    all_data = []
    for metal, url in COMMODITIES.items():
        print(f"[INFO] 抓取 {metal} → {url}")
        resp = requests.get(url)
        if resp.status_code == 200:
            df = tidy_excel(resp.content, metal)
            if df is not None and not df.empty:
                all_data.append(df)
        else:
            print(f"[WARN] 無法下載 {metal}")

    if not all_data:
        print("[ERROR] 沒有資料")
        return

    df_all = pd.concat(all_data, ignore_index=True)
    df_all["Year"] = df_all["Year"].astype(int)
    df_all = df_all.sort_values(["Commodity", "Year", "Country"])
    save_csv(df_all, "usgs_world_production_long")

if __name__ == "__main__":
    run()
