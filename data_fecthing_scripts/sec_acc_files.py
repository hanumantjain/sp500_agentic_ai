#!/usr/bin/env python3
import argparse, csv, io, os, re, time
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
MAX_RPS = 8  # stay under SEC's 10 req/sec

PRIMARY_HTML_HINT = re.compile(r"(10-k|10q|10-q|8-k|8k|primary|main|document)", re.I)
EX99_RE = re.compile(r"^ex[-_]?99", re.I)
R_RENDERED_PAGE = re.compile(r"^r\d+\.htm$", re.I)  # XBRL-rendered "Rxx.htm" pages we usually skip

@dataclass
class FileItem:
    name: str
    size: Optional[int]
    last_modified: Optional[str]
    type_hint: Optional[str]

def ua_headers():
    ua = os.getenv("SEC_UA")
    if not ua:
        raise SystemExit("Set SEC_UA env var, e.g. SEC_UA='Your Name your.email@example.com'")
    return {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}

def throttle():
    time.sleep(1.0 / MAX_RPS)

def no_leading_zeros(cik: str) -> str:
    # Remove CIK prefix if present
    if cik.upper().startswith('CIK'):
        cik = cik[3:]  # Remove 'CIK' prefix
    return str(int(cik))

def acc_nodash(acc: str) -> str:
    return acc.replace("-", "")

def get_json(url: str) -> dict:
    throttle()
    r = requests.get(url, headers=ua_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def get_bytes(url: str) -> bytes:
    throttle()
    r = requests.get(url, headers=ua_headers(), timeout=60)
    r.raise_for_status()
    return r.content

def list_accession_files(cik: str, acc: str) -> List[FileItem]:
    base = f"{SEC_ARCHIVES_BASE}/{no_leading_zeros(cik)}/{acc_nodash(acc)}"
    j = get_json(f"{base}/index.json")
    items = j.get("directory", {}).get("item", []) or []
    out: List[FileItem] = []
    for it in items:
        sz = it.get("size")
        out.append(FileItem(
            name=it.get("name"),
            size=int(sz.replace(",", "")) if (sz and str(sz).strip().isdigit() is False) else (int(sz) if sz else None),
            last_modified=it.get("last-modified"),
            type_hint=it.get("type")))
    return out

def guess_primary_html(files: List[FileItem]) -> Optional[str]:
    # Heuristic: biggest .htm(l) that isn't Rxx.htm, EX-*, css/js/jpg/etc.
    htmls = [f for f in files if f.name and f.name.lower().endswith((".htm", ".html"))]
    htmls = [f for f in htmls if not R_RENDERED_PAGE.match(f.name)]
    htmls = [f for f in htmls if not EX99_RE.match(f.name.lower())]
    if not htmls:
        return None
    # Prefer names with helpful hints; else pick largest by size
    hinted = [f for f in htmls if PRIMARY_HTML_HINT.search(f.name)]
    pick = max(hinted or htmls, key=lambda x: (x.size or 0))
    return pick.name

def choose_files(files: List[FileItem], which: str) -> List[Tuple[str, bool, Optional[str]]]:
    """
    Return [(filename, is_primary, exhibit_type?)]
    which in {'primary','primary+ex99','all'}
    """
    chosen = []
    primary = guess_primary_html(files)
    if primary:
        chosen.append((primary, True, None))
    if which in ("primary+ex99", "all"):
        for f in files:
            nm = (f.name or "").lower()
            if EX99_RE.match(nm):
                chosen.append((f.name, False, "EX-99"))
    if which == "all":
        for f in files:
            if f.name not in [c[0] for c in chosen]:
                chosen.append((f.name, False, None))
    # de-dup while preserving order
    seen = set()
    dedup = []
    for t in chosen:
        if t[0] not in seen:
            seen.add(t[0])
            dedup.append(t)
    return dedup

def html_to_text(data: bytes) -> Tuple[str, str]:
    soup = BeautifulSoup(data, "html.parser")
    for tag in soup(["script", "style", "noscript"]): tag.extract()
    text = soup.get_text("\n")
    text = re.sub(r"\n{2,}", "\n\n", text).strip()
    return text, "text/html"

def pdf_to_text(data: bytes) -> Tuple[str, str]:
    reader = PdfReader(io.BytesIO(data))
    parts = []
    for p in reader.pages:
        parts.append(p.extract_text() or "")
    return "\n".join(parts).strip(), "application/pdf"

def bytes_to_text(name: str, data: bytes) -> Tuple[str, str]:
    ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    if ext in ("htm", "html"): return html_to_text(data)
    if ext == "pdf":           return pdf_to_text(data)
    # Fallback as UTF-8 text (e.g., .txt)
    try:
        return data.decode("utf-8", errors="ignore"), "text/plain"
    except Exception:
        return "", "application/octet-stream"

def run(input_csv: str, files_out_csv: str, docs_out_csv: Optional[str], which: str):
    rows_in = []
    with open(input_csv, newline="") as fh:
        r = csv.DictReader(fh)
        for row in r:
            cik = row.get("cik") or row.get("CIK")
            acc = row.get("accession_number") or row.get("accession")
            if not cik or not acc: continue
            rows_in.append((cik.strip(), acc.strip()))

    with open(files_out_csv, "w", newline="") as f_out:
        files_writer = csv.writer(f_out)
        files_writer.writerow(["cik", "accession_number", "name", "url", "size", "last_modified", "type_hint", "picked", "is_primary", "exhibit_type"])

        docs_writer = None
        if docs_out_csv:
            docs_writer = csv.writer(open(docs_out_csv, "w", newline=""))
            docs_writer.writerow(["cik", "accession_number", "name", "url", "content_type", "text"])

        for cik, acc in rows_in:
            base = f"{SEC_ARCHIVES_BASE}/{no_leading_zeros(cik)}/{acc_nodash(acc)}"
            files = list_accession_files(cik, acc)
            picks = choose_files(files, which=which)

            # Write inventory
            all_names = {f.name for f in files}
            picked_names = {p[0] for p in picks}
            for f in files:
                url = f"{base}/{f.name}"
                files_writer.writerow([cik, acc, f.name, url, f.size or "", f.last_modified or "", f.type_hint or "", f.name in picked_names, any(p[0]==f.name and p[1] for p in picks), next((p[2] for p in picks if p[0]==f.name), "")])

            # Download & extract text for chosen ones
            if docs_writer:
                for (name, _is_primary, _exhibit) in picks:
                    url = f"{base}/{name}"
                    data = get_bytes(url)
                    text, ctype = bytes_to_text(name, data)
                    docs_writer.writerow([cik, acc, name, url, ctype, text])

if __name__ == "__main__":
    import io
    parser = argparse.ArgumentParser(description="EDGAR index.json → files inventory CSV (+ optional docs CSV)")
    parser.add_argument("--input", required=True, help="CSV with columns: cik, accession_number")
    parser.add_argument("--files-out", required=True, help="Output CSV listing all files in index.json")
    parser.add_argument("--docs-out", help="If set, also download & extract text for chosen files into this CSV")
    parser.add_argument("--which", default="primary+ex99", choices=["primary","primary+ex99","all"], help="Which files to download for docs CSV")
    args = parser.parse_args()
    run(args.input, args.files_out, args.docs_out, args.which)
