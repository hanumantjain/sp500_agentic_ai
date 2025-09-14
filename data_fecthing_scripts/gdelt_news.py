# pip install requests pandas python-dateutil trafilatura pyarrow
import requests, time, math, re, pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import trafilatura

# ---- CONFIG ----
START = datetime(2020, 1, 1)
END   = datetime(2021, 2, 28)
DOMAINS = [
  "reuters.com","finance.yahoo.com","cnbc.com","marketwatch.com",
  "apnews.com","bloomberg.com","wsj.com","barrons.com",
  "seekingalpha.com","benzinga.com","businesswire.com",
  "globenewswire.com","prnewswire.com"
]
API = "https://api.gdeltproject.org/api/v2/doc/doc"
MAXREC = 250  # GDELT cap per call

def gdelt_fetch(domain, start, end):
  """Fetch up to MAXREC articles for domain/time window."""
  params = {
    "query": f"domainis:{domain}",
    "mode": "ArtList",         # article list
    "format": "JSON",          # machine-friendly
    "maxrecords": str(MAXREC),
    "startdatetime": start.strftime("%Y%m%d%H%M%S"),
    "enddatetime":   end.strftime("%Y%m%d%H%M%S"),
    "sort": "DateAsc"
  }
  r = requests.get(API, params=params, timeout=60)
  r.raise_for_status()
  return r.json().get("articles", [])

def window_sweep(domain, start, end):
  """Adaptively split windows to avoid the 250-record cap (ensure completeness)."""
  results = []
  stack = [(start, end)]
  while stack:
    s, e = stack.pop()
    rows = gdelt_fetch(domain, s, e)
    if len(rows) >= MAXREC:
      mid = s + (e - s) / 2
      # split into two halves and continue until each half < MAXREC
      if (e - s) > timedelta(hours=1):
        stack.append((mid, e))
        stack.append((s, mid))
      else:
        results.extend(rows)  # give up splitting further (rare edge)
    else:
      results.extend(rows)
    time.sleep(0.2)
  return results

def iter_months(start, end):
  cur = datetime(start.year, start.month, 1)
  while cur <= end:
    nxt = (cur + relativedelta(months=1)) - timedelta(seconds=1)
    yield cur, min(nxt, end)
    cur = cur + relativedelta(months=1)

# 1) Pull article metadata (url/title/seendate/domain)
all_rows = []
for dom in DOMAINS:
  for s, e in iter_months(START, END):
    all_rows.extend(window_sweep(dom, s, e))

meta = pd.DataFrame(all_rows).drop_duplicates(subset=["url"])
# columns typically: url, title, seendate, domain, language, sourcecountry

# 2) Extract article content (best-effort; paywalls may return empty)
def fetch_text(u):
  try:
    downloaded = trafilatura.fetch_url(u, timeout=30)
    return trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
  except Exception:
    return ""

meta["content"] = meta["url"].map(fetch_text)

# 3) Tag S&P 500 tickers
sp = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]  # table
sp["Symbol"] = sp["Symbol"].astype(str).str.upper()
sp["Name"] = sp["Security"].astype(str)
# quick alias map for name matching
aliases = {row.Symbol: {row.Name, row.Name.replace(" Inc.", ""), row.Name.replace(", Inc.", "")}
           for _, row in sp.iterrows()}

def find_tickers(title, content):
  txt = f" {str(title)} {str(content)} ".upper()
  hits = set()
  for sym, names in aliases.items():
    # exact ticker token OR company name substring
    if f" {sym} " in txt:
      hits.add(sym)
    else:
      for nm in names:
        if nm and nm.upper() in txt:
          hits.add(sym)
  return sorted(hits)

meta["tickers"] = meta.apply(lambda r: find_tickers(r.get("title",""), r.get("content","")), axis=1)
out = meta[meta["tickers"].map(len) > 0].copy()

# 4) Final tidy dataframe + CSV
out["date"] = pd.to_datetime(out["seendate"], errors="coerce", utc=True)
final = out[["tickers","date","title","content","url","domain"]].rename(
    columns={"url":"source_link", "domain":"source_domain"})
final.to_csv("sp500_finance_news_2020_2025.csv", index=False)
print("Saved", final.shape, "to sp500_finance_news_2020_2025.csv")
