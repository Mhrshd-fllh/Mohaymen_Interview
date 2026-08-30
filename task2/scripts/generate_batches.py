import os
import argparse
from pathlib import Path


def split_csv_into_batches(
    source_csv_path: str,
    output_dir: str = "test_stream",
    batch_size: int = 5000,
    num_batches: int = 3,
):
    source_path = Path(source_csv_path).resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Source file not found at: {source_path}")

    out_path = Path(output_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    print(f"📖 Reading source dataset: {source_path}")
    print(f"📁 Output directory: {out_path}")
    print(f"⚙️ Generating {num_batches} batches with {batch_size} records each...")

    with open(source_path, "r", encoding="utf-8") as src:
        header = src.readline()
        if not header:
            raise ValueError("Source CSV file is empty!")

        for batch_idx in range(1, num_batches + 1):
            batch_filename = f"sms_batch_{batch_idx}.csv"
            batch_filepath = out_path / batch_filename

            lines_written = 0
            with open(batch_filepath, "w", encoding="utf-8") as out:
                out.write(header)
                for _ in range(batch_size):
                    line = src.readline()
                    if not line:
                        break
                    out.write(line)
                    lines_written += 1

            print(f"  ✅ Created {batch_filename} ({lines_written} records)")
            if lines_written < batch_size:
                print(f"  ℹ️ Reached end of source dataset.")
                break

    print("\n🎉 All streaming batches generated successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split master CSV into streaming micro-batches.")
    parser.add_argument(
        "--source",
        type=str,
        default="../REF_SMS/REF_CBS_SMS2.csv",
        help="Path to master CSV file (default: ../REF_SMS/REF_CBS_SMS2.csv)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="test_stream",
        help="Target output directory (default: test_stream)",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=5000,
        help="Number of rows per batch (default: 5000)",
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=3,
        help="Number of batches to produce (default: 3)",
    )

    args = parser.parse_args()
    split_csv_into_batches(
        source_csv_path=args.source,
        output_dir=args.output,
        batch_size=args.size,
        num_batches=args.batches,
    )
