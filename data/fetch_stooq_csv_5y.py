# collect_sp500_from_stooq_dump.py
import os, shutil, pathlib

BASE = "/Users/omkar/Downloads/data/daily/us"                  # <-- change me
OUT  = "data"                # where you want the files
LIST = "sp500_stooq.txt"                         # from step 1

tickers = {t.strip().upper() for t in open(LIST) if t.strip()}
# files on disk are lower-case like aapl.us.txt; make a filename set
wanted_files = {t.lower() + ".txt" for t in tickers}  # AAPL.US -> aapl.us.txt

os.makedirs(OUT, exist_ok=True)

count = 0
for root, _, files in os.walk(BASE):
    for fn in files:
        if fn.lower() in wanted_files:
            src = os.path.join(root, fn)
            # keep a flat output, or use subfolders per exchange if you want
            dst = os.path.join(OUT, fn.lower())
            # use copyfile for a real copy; use symlink to save disk
            try:
                # pathlib.Path(dst).symlink_to(src)          # <- space saver
                shutil.copyfile(src, dst)                     # <- plain copy
                count += 1
            except FileExistsError:
                pass

print("copied", count, "files to", OUT)
