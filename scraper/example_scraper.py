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

        for sheet in xls.sheet_names:
            # 不要把第 0 列當 header，整張表讀進來自己找表頭
            df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
            if df.empty:
                continue

            header_idx = None
            # 找到同一列同時包含 'country' 且該列至少有 3 個年份欄位
            for i in range(min(len(df), 200)):  # 前 200 列足夠
                row = df.iloc[i].astype(str).str.strip()
                has_country = row.str.contains(r"^country$", case=False, na=False).any()
                years = row[row.apply(_is_year)]
                if has_country and len(years) >= 3:
                    header_idx = i
                    break

            if header_idx is None:
                print(f"[DBG] sheet {sheet}: 找不到 header，略過")
                continue

            # 以 header_idx 作為欄名，並去掉上方雜訊
            header = df.iloc[header_idx].astype(str).str.strip().tolist()
            work = df.iloc[header_idx + 1 :].copy()
            work.columns = header

            # 找 Country 欄（可能大小寫不同或「Country or area」）
            country_col = None
            for c in work.columns:
                if str(c).strip().lower() == "country":
                    country_col = c
                    break
            if country_col is None:
                for c in work.columns:
                    if "country" in str(c).strip().lower():
                        country_col = c
                        break
            if country_col is None:
                print(f"[DBG] sheet {sheet}: 沒有 Country 欄，略過")
                continue

            # 只保留 Country + 年份欄位
            year_cols = [c for c in work.columns if _is_year(c)]
            if len(year_cols) < 3:
                print(f"[DBG] sheet {sheet}: 年份欄不足，略過")
                continue

            use = work[[country_col] + year_cols].rename(columns={country_col: "Country"})
            use = use[use["Country"].notna() & (use["Country"].astype(str).str.strip() != "")]
            long = use.melt(id_vars=["Country"], var_name="Year", value_name="Production")
            long["Commodity"] = commodity

            # 數值化
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

