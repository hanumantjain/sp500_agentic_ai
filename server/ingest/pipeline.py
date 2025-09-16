#!/usr/bin/env python3
"""
Pipeline script that extracts text and inserts directly into TiDB database
"""
import argparse
import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description, env=None):
    """Run a command and handle errors"""
    print(f"\n🔄 {description}")
    print(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    
    if result.returncode != 0:
        print(f"❌ Error in {description}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    
    print(f"✅ {description} completed successfully")
    if result.stdout.strip():
        print(result.stdout)

def main():
    parser = argparse.ArgumentParser(description="Extract text and insert into TiDB database")
    parser.add_argument("--input", required=True, help="Input file or directory")
    parser.add_argument("--max-chars", type=int, default=6000, help="Max characters per chunk")
    parser.add_argument("--overlap", type=int, default=300, help="Overlap between chunks")
    parser.add_argument("--batch", type=int, default=500, help="Batch size for database inserts")
    parser.add_argument("--symbol", help="Optional ticker symbol to stamp on all rows")
    parser.add_argument("--title", help="Optional title to stamp on all rows")
    parser.add_argument("--url", help="Optional source URL to stamp on all rows")
    
    args = parser.parse_args()
    
    # Build the embeddings command (which now handles extraction + database insertion)
    embed_cmd = [
        sys.executable, "server/ingest/embeddings.py",
        "--input", args.input,
        "--max-chars", str(args.max_chars),
        "--overlap", str(args.overlap),
        "--batch", str(args.batch)
    ]
    
    # Add optional metadata if provided
    if args.symbol:
        embed_cmd.extend(["--symbol", args.symbol])
    if args.title:
        embed_cmd.extend(["--title", args.title])
    if args.url:
        embed_cmd.extend(["--url", args.url])
    
    run_command(embed_cmd, "Text extraction and database insertion")
    
    print(f"\n🎉 Pipeline completed successfully!")
    print(f"📄 Text extracted and chunks inserted into TiDB database")

if __name__ == "__main__":
    main()
