from io import BytesIO

import pandas as pd


def ensure_phone_string(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "phone_fmt" in df.columns:
        df["phone"] = df["phone_fmt"]
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    return df


def to_xlsx_bytes(df: pd.DataFrame, text_cols=("phone",)) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Providers")
        wb = writer.book
        ws = writer.sheets["Providers"]
        text_fmt = wb.add_format({"num_format": "@"})
        for col_name in text_cols:
            if col_name in df.columns:
                col_idx = df.columns.get_loc(col_name)
                ws.set_column(col_idx, col_idx, 16, text_fmt)
    return buf.getvalue()
