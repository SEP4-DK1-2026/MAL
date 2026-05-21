from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write a parquet file containing the first N rows from an input parquet."
    )
    parser.add_argument("input_path", help="Path to the source parquet file")
    parser.add_argument(
        "output_path",
        nargs="?",
        help="Path to the output parquet file. Defaults to <input_stem>_head<N>.parquet",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100,
        help="Number of rows to keep from the start of the file (default: 100)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    input_path = Path(args.input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet file not found: {input_path}")

    output_path = Path(args.output_path) if args.output_path else input_path.with_name(
        f"{input_path.stem}_head{args.rows}.parquet"
    )

    data = pd.read_parquet(input_path)
    sample = data.head(args.rows)
    sample.to_parquet(output_path, index=False)

    print(f"Read {len(data)} rows from {input_path}")
    print(f"Wrote {len(sample)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
