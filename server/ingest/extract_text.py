# server/ingest/extract_text.py
from __future__ import annotations
import os, io, re, pathlib, mimetypes
from dataclasses import dataclass
from typing import List, Optional, Tuple

import fitz  # PyMuPDF
from bs4 import BeautifulSoup
from PIL import Image
import pytesseract
import uuid, hashlib, pathlib


# ---------- small data class ----------
@dataclass
class Chunk:
    doc_id: str           
    chunk_id: str         
    source_path: str
    page_no: Optional[int]
    chunk_no: int
    text: str

# ---------- helpers ----------
_HTML_EXT = {".html", ".htm"}
_IMG_EXT  = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
_TXT_EXT  = {".txt", ".md", ".csv"}  # treat as plain text for now

def compute_file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _clean_whitespace(s: str) -> str:
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n\n", s)
    return s.strip()

def _chunk_text(text: str, max_chars: int = 6000, overlap: int = 300) -> List[str]:
    """simple char-based chunker with overlap to keep context."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]
    chunks = []
    i = 0
    while i < len(text):
        j = min(i + max_chars, len(text))
        chunks.append(text[i:j])
        if j == len(text): break
        i = j - overlap  # step back for overlap
        if i < 0: i = 0
    return chunks

# ---------- extractors ----------
def extract_pdf(path: str) -> List[Tuple[int, str]]:
    """return list of (page_no, text) for a PDF"""
    out = []
    with fitz.open(path) as doc:
        for i, page in enumerate(doc, start=1):
            # plain text; if you prefer layout-ish, use "text" with sort=True
            txt = page.get_text("text", sort=True) or ""
            out.append((i, _clean_whitespace(txt)))
    return out

def extract_html(path: str) -> str:
    with open(path, "rb") as f:
        data = f.read()
    soup = BeautifulSoup(data, "html.parser")
    for tag in soup(["script", "style", "noscript"]): tag.extract()
    txt = soup.get_text("\n")
    return _clean_whitespace(txt)

def extract_txt(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return _clean_whitespace(f.read())

def extract_image(path: str) -> str:
    # basic OCR; you can tune lang='eng' or DPI if needed
    img = Image.open(path)
    txt = pytesseract.image_to_string(img)
    return _clean_whitespace(txt)

# ---------- main API ----------
def detect_kind(path: str) -> str:
    ext = pathlib.Path(path).suffix.lower()
    if ext in _HTML_EXT: return "html"
    if ext in _IMG_EXT:  return "image"
    if ext in _TXT_EXT:  return "txt"
    if ext == ".pdf":    return "pdf"
    # fallback by mimetype
    mt, _ = mimetypes.guess_type(path)
    if mt and "html" in mt:  return "html"
    if mt and "pdf"  in mt:  return "pdf"
    if mt and mt.startswith("text/"): return "txt"
    return "binary"

def extract_to_chunks(path: str,
                      max_chars: int = 6000,
                      overlap: int = 300) -> List[Chunk]:
    kind = detect_kind(path)
    doc_id = compute_file_hash(path)
    chunks: List[Chunk] = []

    if kind == "pdf":
        for page_no, page_text in extract_pdf(path):
            for j, c in enumerate(_chunk_text(page_text, max_chars, overlap)):
                chunks.append(Chunk(
                    doc_id=doc_id,
                    chunk_id=f"{doc_id}:{j}",
                    source_path=path,
                    page_no=page_no,
                    chunk_no=j,
                    text=c
                ))
    elif kind == "html":
        text = extract_html(path)
        for j, c in enumerate(_chunk_text(text, max_chars, overlap)):
            chunks.append(Chunk(
                doc_id=doc_id,
                chunk_id=f"{doc_id}:{j}",
                source_path=path,
                page_no=None,
                chunk_no=j,
                text=c
            ))
    elif kind == "txt":
        text = extract_txt(path)
        for j, c in enumerate(_chunk_text(text, max_chars, overlap)):
            chunks.append(Chunk(
                doc_id=doc_id,
                chunk_id=f"{doc_id}:{j}",
                source_path=path,
                page_no=None,
                chunk_no=j,
                text=c
            ))
    elif kind == "image":
        text = extract_image(path)
        for j, c in enumerate(_chunk_text(text, max_chars, overlap)):
            chunks.append(Chunk(
                doc_id=doc_id,
                chunk_id=f"{doc_id}:{j}",
                source_path=path,
                page_no=None,
                chunk_no=j,
                text=c
            ))
    else:
        # unsupported binary: return empty (or raise)
        return []

    return chunks

# ---------- tiny CLI for testing ----------
if __name__ == "__main__":
    import argparse, csv
    ap = argparse.ArgumentParser(description="Extract text and chunk it")
    ap.add_argument("--input", required=True, help="file or folder")
    ap.add_argument("--out-csv", required=True, help="where to write chunks CSV")
    ap.add_argument("--max-chars", type=int, default=6000)
    ap.add_argument("--overlap", type=int, default=300)
    args = ap.parse_args()

    paths: List[str] = []
    p = pathlib.Path(args.input)
    if p.is_dir():
        for ext in list(_HTML_EXT | _IMG_EXT | _TXT_EXT | {".pdf"}):
            paths += [str(x) for x in p.rglob(f"*{ext}")]
    else:
        paths = [str(p)]

    rows = []
    for path in paths:
        for ch in extract_to_chunks(path, args.max_chars, args.overlap):
            rows.append([ch.doc_id, ch.chunk_id, ch.source_path, ch.page_no or "", ch.chunk_no, ch.text])

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True) if os.path.dirname(args.out_csv) else None
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["doc_id", "chunk_id", "source_path", "page_no", "chunk_no", "text"])
        w.writerows(rows)

    print(f"wrote {len(rows)} chunks → {args.out_csv}")
