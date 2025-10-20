import re
from io import BytesIO
from pathlib import Path
from datetime import date

import pandas as pd
import requests

# =====================
# 設定
# =====================
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COMMODITIES = {
    "aluminum": "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2022-alumi-ERT.xlsx",
    "nickel":   "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2022-nickel-ERT.xlsx",
    "cobalt":   "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2023-cobal-ERT.xlsx",
    "tungsten": "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/myb1-2022-tungs-ERT.xlsx",
}
STAMP = date.today().isoformat()

# =====================
# 解析工具
# =====================
YEAR_RE = re.compile(r"^(19|20)\d{2}$")

def _is_year(x: str) -> bool:
    x = str(x).strip()
    return bool(YEAR_RE.match(x))

def tidy_excel(xlsx_bytes, commodity):
    """把 USGS ERT Excel 解析成長表：Country | Year | Production | Commodity"""
    try:
        xls = pd.ExcelFile(BytesIO(xlsx_bytes))
        print(f"[DBG] sheets: {xls.sheet_names}")

        country_keywords = ["country", "country or area", "country/area", "area", "location"]

        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
            if df.empty:
                continue

            header_idx = None
            # 嘗試找出第一列中出現任何 country_keywords 的行
            for i in range(min(len(df), 200)):
                row = df.iloc[i].astype(str).str.strip().str.lower()
                has_country = row.apply(lambda x: any(k in x for k in country_keywords)).any()
                year_like = row[row.apply(_is_year)]
                if has_country and len(year_like) >= 2:
                    header_idx = i
                    break

            if header_idx is None:
                continue

            header = df.iloc[header_idx].astype(str).str.strip().tolist()
            work = df.iloc[header_idx + 1:].copy()
            work.columns = header

            # 找出「Country」欄位（擴展定義）
            country_col = None
            for c in work.columns:
                c_str = str(c).strip().lower()
                if any(k in c_str for k in country_keywords):
                    country_col = c
                    break

            if country_col is None:
                continue

            # 找出「年份」欄位
            year_cols = [c for c in work.columns if _is_year(str(c))]
            if len(year_cols) < 2:
                continue

            use = work[[country_col] + year_cols].rename(columns={country_col: "Country"})
            use = use[use["Country"].notna() & (use["Country"].astype(str).str.strip() != "")]
            long = use.melt(id_vars=["Country"], var_name="Year", value_name="Production")
            long["Commodity"] = commodity

            long["Year"] = pd.to_numeric(long["Year"], errors="coerce")
            long["Production"] = pd.to_numeric(
                long["Production"].astype(str).str.replace(",", ""), errors="coerce"
            )
            long = long.dropna(subset=["Year", "Production"])
            if not long.empty:
                print(f"[OK] {commodity} from sheet '{sheet}' → rows: {len(long)}")
                return long

        print(f"[WARN] {commodity} 沒有偵測到標題行/有效資料")
        return None

    except Exception as e:
        print(f"[WARN] {commodity} 無法解析：{e}")
        return None


# =====================
# 輸出
# =====================
def save_csv(df, basename):
    latest = OUTPUT_DIR / f"{basename}_latest.csv"
    dated  = OUTPUT_DIR / f"{basename}_{STAMP}.csv"
    df.to_csv(latest, index=False)
    df.to_csv(dated, index=False)
    print("[WRITE]", latest.name, "rows:", len(df))

# =====================
# 主流程
# =====================
def run():
    all_data = []
    for metal, url in COMMODITIES.items():
        print(f"[INFO] 抓取 {metal} → {url}")
        resp = requests.get(url, timeout=60)
        print(f"[INFO] status {metal}: {resp.status_code}")
        if resp.status_code == 200:
            df = tidy_excel(resp.content, metal)
            if df is not None and not df.empty:
                all_data.append(df)
            else:
                print(f"[WARN] {metal} 解析後為空")
        else:
            print(f"[WARN] 無法下載 {metal}")

    if not all_data:
        # 寫個探針檔，方便檢查 Actions 是否有跑到這一步
        probe = OUTPUT_DIR / f"usgs_probe_{STAMP}.csv"
        probe.write_text("note,no data parsed on runner\n")
        print("[PROBE] wrote", probe)
        return

    df_all = pd.concat(all_data, ignore_index=True)
    df_all["Year"] = df_all["Year"].astype(int)
    df_all = df_all.sort_values(["Commodity", "Year", "Country"])
    save_csv(df_all, "usgs_world_production_long")

if __name__ == "__main__":
    run()

