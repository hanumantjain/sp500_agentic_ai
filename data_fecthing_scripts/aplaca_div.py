# save_alpha_vantage_corp_actions.py
import os, time, requests, pathlib, csv, io
from itertools import islice

API = "https://www.alphavantage.co/query"
API_KEY = "GB9BRVHE936LZVNI"  # <-- your key

# --- pick the slice you want (1-based, inclusive) ---
START_LINE = 13
END_LINE   = 24

def iter_symbols(path):
    """Yield symbols from file, skipping blanks/comments."""
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            yield s.upper()

def read_symbols_range(path: str, start: int, end: int):
    """Return symbols for the 1-based inclusive range after filtering."""
    if start < 1 or end < start:
        raise ValueError("range must be 1-based and end >= start")
    return list(islice(iter_symbols(path), start - 1, end))

def save_raw_csv(sym: str, fn: str, api_key: str, outdir: str = "av_raw", timeout: int = 30) -> bool:
    """
    Fetch Alpha Vantage Fundamentals CSV for one symbol.
    Ensures a 'symbol' column exists by adding it when missing.
    Returns True on success, False if rate-limited / error text.
    """
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    params = {"function": fn, "symbol": sym, "datatype": "csv", "apikey": api_key}
    r = requests.get(API, params=params, timeout=timeout)
    r.raise_for_status()
    text = r.text

    # AV rate-limit or error often comes back as plain text or JSON-like blob.
    if "Thank you for using Alpha Vantage" in text or text.lstrip().startswith("{"):
        print(f"[limit/err] {fn} {sym}: {text[:140]}...")
        return False

    # Parse the CSV; if there's no 'symbol' header, add one and fill with the ticker
    buf = io.StringIO(text)
    reader = csv.reader(buf)
    rows = list(reader)
    if not rows:
        print(f"[warn] {fn} {sym}: empty CSV")
        return True  # nothing to write, but not a hard error

    header = rows[0]
    header_lower = [h.strip().lower() for h in header]
    need_symbol_col = ("symbol" not in header_lower)

    outpath = f"{outdir}/{fn.lower()}_{sym}.csv"
    with open(outpath, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if need_symbol_col:
            w.writerow(["symbol"] + header)
            for row in rows[1:]:
                w.writerow([sym] + row)
        else:
            # header already has symbol; just write as-is
            w.writerows(rows)

    print(f"[ok] wrote {outpath}  (added symbol col: {need_symbol_col})")
    return True

if __name__ == "__main__":
    symbols = read_symbols_range("sp500_symbols.txt", START_LINE, END_LINE)
    print(f"symbols {START_LINE}..{END_LINE} -> {symbols}")

    for s in symbols:
        if not save_raw_csv(s, "DIVIDENDS", API_KEY): break
        if not save_raw_csv(s, "SPLITS", API_KEY): break
        time.sleep(2)  # gentle pacing (2 calls per symbol)
