#!/usr/bin/env python3
# scrape_sp500_stooq_symbols_paginated.py
import re
import csv
import sys
import time
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"

BASE = "https://stooq.com/t/?i=579&v=1"  # S&P500 list
# add &d=YYYYMMDD to freeze a specific snapshot; omit `d` to get today's

def fetch(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    return r.text

def extract_symbols(html):
    soup = BeautifulSoup(html, "html.parser")
    symbols = set()

    # (A) anchor hrefs like ?s=AAPL.US
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]s=([A-Z0-9.-]+\.US)\b", a["href"].upper())
        if m:
            symbols.add(m.group(1))

    # (B) fallback regex scan (handles plain text cells)
    symbols |= set(re.findall(r"\b[A-Z0-9]+(?:-[A-Z0-9]+)?\.US\b", soup.get_text(" ").upper()))

    return symbols

def discover_last_page(html):
    # Look for &l=N links and take the max; if none found, return 1
    pages = set([1])
    for m in re.finditer(r"[?&]l=(\d+)\b", html):
        pages.add(int(m.group(1)))
    return max(pages) if pages else 1

def collect_symbols(snapshot_date=None, polite_delay=0.7):
    # page 1 url
    base_url = BASE if not snapshot_date else f"{BASE}&d={snapshot_date}"
    html1 = fetch(base_url)
    last = discover_last_page(html1)  # e.g., 6
    all_syms = set(extract_symbols(html1))

    # pages 2..last
    for l in range(2, last + 1):
        url = f"{base_url}&l={l}"
        html = fetch(url)
        all_syms |= extract_symbols(html)
        time.sleep(polite_delay)

    return sorted(all_syms), last

def main():
    # Usage: python scrape_sp500_stooq_symbols_paginated.py [YYYYMMDD]
    snapshot_date = sys.argv[1] if len(sys.argv) > 1 else None
    symbols, last = collect_symbols(snapshot_date)
    print(f"[ok] pages: {last}, symbols: {len(symbols)}")

    # Basic sanity: expect ~500
    if len(symbols) < 400:
        print("[warn] unusually low symbol count; site layout may have changed.", file=sys.stderr)

    with open("sp500_stooq_symbols.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        for s in symbols:
            w.writerow([s])

    print("wrote sp500_stooq_symbols_1.csv")

if __name__ == "__main__":
    main()
