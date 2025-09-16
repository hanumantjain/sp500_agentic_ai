#!/usr/bin/env python3
import argparse, csv, io, os, re, time, requests
from dataclasses import dataclass
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup
from pypdf import PdfReader

SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
MAX_RPS = 8  # stay < 10 req/s (SEC fair-access)

PRIMARY_HTML_HINT = re.compile(
    r"(10-k|10q|10-q|8-k|8k|primary|main|document|form10)", re.I
)
EX99_RE = re.compile(r"^ex[-_]?99(\.\d+)?\.(htm|html|pdf)$", re.I)
FINREP_RE = re.compile(r"financial[_-]?report.*\.(xlsx?|xlsm)$", re.I)
R_RENDERED = re.compile(r"^r\d+\.htm$", re.I)


@dataclass
class FileItem:
    name: str
    size: Optional[int]
    last_modified: Optional[str]
    type_hint: Optional[str]


def ua_headers():
    ua = os.getenv("SEC_UA")
    if not ua:
        raise SystemExit(
            "Set SEC_UA env var, e.g. SEC_UA='Your Name your.email@example.com'"
        )
    return {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}


def throttle():
    time.sleep(1.0 / MAX_RPS)


def no_leading_zeros(cik: str) -> str:
    # Remove CIK prefix if present
    if cik.upper().startswith("CIK"):
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
    out = []
    for it in items:
        sz = it.get("size")
        size = None
        if sz:
            try:
                size = int(str(sz).replace(",", ""))
            except:
                size = None
        out.append(
            FileItem(
                name=it.get("name") or "",
                size=size,
                last_modified=it.get("last-modified"),
                type_hint=it.get("type"),
            )
        )
    return out


def guess_primary_html(files: List[FileItem]) -> Optional[str]:
    htmls = [f for f in files if f.name.lower().endswith((".htm", ".html"))]
    htmls = [f for f in htmls if not R_RENDERED.match(f.name)]
    htmls = [
        f for f in htmls if not f.name.lower().startswith(("ex-", "ex_", "ex"))
    ]  # skip exhibits
    if not htmls:
        return None
    hinted = [f for f in htmls if PRIMARY_HTML_HINT.search(f.name)]
    pick = max(hinted or htmls, key=lambda x: (x.size or 0))
    return pick.name


def choose_minimal(
    files: List[FileItem], include_primary=True, include_ex99=True, include_finrep=True
) -> List[Tuple[str, str]]:
    """Return [(name, kind)] where kind in {'primary','ex99','financial_report'}"""
    chosen = []
    if include_primary:
        prim = guess_primary_html(files)
        if prim:
            chosen.append((prim, "primary"))
    if include_ex99:
        for f in files:
            if EX99_RE.search(f.name):
                chosen.append((f.name, "ex99"))
    if include_finrep:
        for f in files:
            if FINREP_RE.search(f.name):
                chosen.append((f.name, "financial_report"))
    # dedupe, keep order
    seen = set()
    out = []
    for n, k in chosen:
        if n not in seen:
            seen.add(n)
            out.append((n, k))
    return out


def html_to_text(data: bytes) -> Tuple[str, str]:
    soup = BeautifulSoup(data, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    text = soup.get_text("\n")
    text = re.sub(r"\n{2,}", "\n\n", text).strip()
    return text, "text/html"


def pdf_to_text(data: bytes) -> Tuple[str, str]:
    reader = PdfReader(io.BytesIO(data))
    parts = [(p.extract_text() or "") for p in reader.pages]
    return "\n".join(parts).strip(), "application/pdf"


def bytes_to_text(name: str, data: bytes) -> Tuple[str, str]:
    ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    if ext in ("htm", "html"):
        return html_to_text(data)
    if ext == "pdf":
        return pdf_to_text(data)
    try:
        return data.decode("utf-8", errors="ignore"), "text/plain"
    except:
        return "", "application/octet-stream"


def run(
    input_csv: str,
    docs_out_csv: str,
    include_primary: bool,
    include_ex99: bool,
    include_finrep: bool,
):
    with open(input_csv, newline="") as fh:
        rows = list(csv.DictReader(fh))

    total_rows = len(rows)
    processed = 0
    successful = 0

    print(f"Processing {total_rows:,} records...")

    with open(docs_out_csv, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(
            ["cik", "accession_number", "name", "url", "kind", "content_type", "text"]
        )
        for row in rows:
            cik = (row.get("cik") or row.get("CIK") or "").strip()
            acc = (row.get("accession_number") or row.get("accession") or "").strip()
            if not cik or not acc:
                processed += 1
                continue
            try:
                base = f"{SEC_ARCHIVES_BASE}/{no_leading_zeros(cik)}/{acc_nodash(acc)}"
                files = list_accession_files(cik, acc)
                picks = choose_minimal(
                    files, include_primary, include_ex99, include_finrep
                )
                for name, kind in picks:
                    url = f"{base}/{name}"
                    try:
                        data = get_bytes(url)
                        text, ctype = bytes_to_text(name, data)
                        w.writerow([cik, acc, name, url, kind, ctype, text])
                        successful += 1
                    except Exception:
                        continue
            except Exception:
                pass

            processed += 1
            if processed % 100 == 0:
                print(
                    f"Progress: {processed:,}/{total_rows:,} ({processed/total_rows*100:.1f}%) - Success: {successful:,}"
                )

    print(
        f"Completed: {processed:,}/{total_rows:,} records processed, {successful:,} documents downloaded"
    )


if __name__ == "__main__":
    import io, re

    ap = argparse.ArgumentParser(
        description="Download only primary HTML, EX-99 exhibits, and Financial_Report.* from EDGAR index.json -> docs_text.csv"
    )
    ap.add_argument(
        "--input",
        default="../data/test_cik_1800.csv",
        help="CSV with columns: cik,accession_number",
    )
    ap.add_argument(
        "--docs-out",
        default="../data/submissions_sec_docs/cik_1800_docs.csv",
        help="Output CSV with extracted text (filtered files only)",
    )
    ap.add_argument("--no-primary", action="store_true", help="Skip primary HTML")
    ap.add_argument("--no-ex99", action="store_true", help="Skip EX-99 exhibits")
    ap.add_argument("--no-finrep", action="store_true", help="Skip Financial_Report.*")
    args = ap.parse_args()

    # Set SEC_UA environment variable if not set
    if not os.getenv("SEC_UA"):
        os.environ["SEC_UA"] = "Test User test@example.com"
        print("Set SEC_UA environment variable for testing")

    run(
        args.input,
        args.docs_out,
        include_primary=not args.no_primary,
        include_ex99=not args.no_ex99,
        include_finrep=not args.no_finrep,
    )
