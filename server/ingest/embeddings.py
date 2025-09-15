#!/usr/bin/env python3
import os, argparse, pathlib, collections, sys

# Add the server directory to the path to find config and db modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from db import engine, Session

# import your extractor (the one you pasted)
from extract_text import extract_to_chunks, _HTML_EXT, _IMG_EXT, _TXT_EXT

INSERT_SQL = """
INSERT INTO user_chat_docs
  (doc_id, chunk_no, chunk_id, symbol, title, url, page_no, text)
VALUES
  (:doc_id, :chunk_no, :chunk_id, :symbol, :title, :url, :page_no, :text)
ON DUPLICATE KEY UPDATE
  text=VALUES(text),
  symbol=VALUES(symbol),
  title=VALUES(title),
  url=VALUES(url),
  page_no=VALUES(page_no);
"""

def iter_paths(input_path: str):
    """Yield file paths to process (respects the same extensions your extractor supports)."""
    p = pathlib.Path(input_path)
    if p.is_file():
        yield str(p)
        return
    if p.is_dir():
        exts = set(_HTML_EXT | _IMG_EXT | _TXT_EXT | {".pdf"})
        for ext in exts:
            for x in p.rglob(f"*{ext}"):
                yield str(x)
        return
    raise SystemExit(f"Not found: {input_path}")

def resequence(chunks):
    """
    Ensure chunk_no is 0..N-1 per doc_id and chunk_id = f'{doc_id}:{chunk_no}'.
    Your extractor's chunk_no may reset per page; this removes collisions.
    """
    by_doc = collections.defaultdict(list)
    for ch in chunks:
        by_doc[ch.doc_id].append(ch)

    out = []
    for doc_id, items in by_doc.items():
        seq = 0
        for ch in items:
            ch.chunk_no = seq
            ch.chunk_id = f"{doc_id}:{seq}"
            seq += 1
            out.append(ch)
    return out

def insert_batches(rows, batch_size=500):
    total = 0
    buf = []
    
    with engine.connect() as conn:
        for r in rows:
            # derive some optional metadata
            symbol = getattr(r, "symbol", None)
            title  = getattr(r, "title", None)
            url    = getattr(r, "url", None)

            if not r.text:
                continue  # skip empty chunks

            buf.append({
                "doc_id": r.doc_id,
                "chunk_no": r.chunk_no,
                "chunk_id": r.chunk_id,
                "symbol": symbol,
                "title": title,
                "url": url,
                "page_no": r.page_no,
                "text": r.text
            })
            
            if len(buf) >= batch_size:
                conn.execute(text(INSERT_SQL), buf)
                conn.commit()
                total += len(buf)
                buf.clear()
        
        if buf:
            conn.execute(text(INSERT_SQL), buf)
            conn.commit()
            total += len(buf)
    
    return total

def main():
    ap = argparse.ArgumentParser(description="Extract → chunk → insert into TiDB (Auto-Embedding).")
    ap.add_argument("--input", required=True, help="file or folder of uploads")
    ap.add_argument("--max-chars", type=int, default=6000)
    ap.add_argument("--overlap",   type=int, default=300)
    ap.add_argument("--batch",     type=int, default=500)
    ap.add_argument("--symbol",    help="optional ticker to stamp on all rows", default=None)
    ap.add_argument("--title",     help="optional title to stamp on all rows", default=None)
    ap.add_argument("--url",       help="optional source URL to stamp on all rows", default=None)
    args = ap.parse_args()

    all_chunks = []
    for path in iter_paths(args.input):
        chunks = extract_to_chunks(path, args.max_chars, args.overlap)
        # attach optional metadata if provided
        if args.symbol or args.title or args.url:
            for ch in chunks:
                if args.symbol: setattr(ch, "symbol", args.symbol)
                if args.title:  setattr(ch, "title",  args.title)
                if args.url:    setattr(ch, "url",    args.url)
        all_chunks.extend(chunks)

    if not all_chunks:
        print("No chunks found.")
        return

    # Make chunk_no unique per doc and rebuild chunk_id
    all_chunks = resequence(all_chunks)

    total = insert_batches(all_chunks, batch_size=args.batch)
    print(f"Inserted/updated {total} chunks into docs_auto "
          f"(auto-embedding happens in TiDB).")

if __name__ == "__main__":
    main()
