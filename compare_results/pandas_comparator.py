import sys
from pathlib import Path
import pandas as pd

EXPECTED_ARGS = 3

ERROR = 2
FAILURE = 1
SUCCESS = 0

NO_FAILURES = 0

EXPECTED_FILES_DIR_INDEX = 1
ACTUAL_FILES_DIR_INDEX = 2

DEFAULT_MAP = [
    ("q1_results.csv",                "ready_query_1.csv"),
    ("q2_best_selling_results.csv",   "ready_query_2_best_selling.csv"),
    ("q2_most_profits_results.csv",   "ready_query_2_most_profits.csv"),
    ("q3_tpv_results.csv",            "ready_query_3.csv"),
    ("q4_most_purchases_results.csv", "ready_query_4.csv"),
]

DELIMITER = ","

def read_csv_file(path: Path):
    return pd.read_csv(path, sep=DELIMITER, header=None, dtype="string")

def occurrences_per_row(df: pd.DataFrame) -> pd.Series:
    return df.apply(tuple, axis=1).value_counts(dropna=False)

def main():
    if len(sys.argv) != EXPECTED_ARGS:
        print("usage: compare_csv_fixed_pandas.py <expected_dir> <actual_dir>", file=sys.stderr)
        return ERROR

    expected_dir = Path(sys.argv[EXPECTED_FILES_DIR_INDEX])
    actual_dir = Path(sys.argv[ACTUAL_FILES_DIR_INDEX])

    print("\nComparison Report\n" + "=" * 18)
    failures = 0

    for expected_file_name, actual_file_name in DEFAULT_MAP:
        expected_file_path = expected_dir / expected_file_name
        actual_file_path = actual_dir / actual_file_name
        print(f"\n{expected_file_name}  <->  {actual_file_name}")

        if not expected_file_path.exists():
            print(f"[MISSING] expected: {expected_file_path}")
            failures += 1
            continue
        
        if not actual_file_path.exists():
            print(f"[MISSING] actual:   {actual_file_path}")
            failures += 1
            continue

        try:
            expected_dataframe = read_csv_file(expected_file_path)
            actual_dataframe = read_csv_file(actual_file_path)
            if expected_file_name.startswith("q3"):
                expected_dataframe[2] = expected_dataframe[2].astype(float).round(3)
                actual_dataframe[2] = actual_dataframe[2].astype(float).round(3)

        except Exception as e:
            print(f"[ERROR] reading files: {e}")
            failures += 1
            continue

        if expected_dataframe.shape[1] != actual_dataframe.shape[1]:
            print(f"[DIFF] number of columns: expected={expected_dataframe.shape[1]} actual={actual_dataframe.shape[1]}")
            failures += 1
            continue

        expected_occurrences = occurrences_per_row(expected_dataframe)
        actual_occurrences = occurrences_per_row(actual_dataframe)

        if expected_occurrences.equals(actual_occurrences.reindex(expected_occurrences.index, fill_value=0)) and actual_occurrences.equals(expected_occurrences.reindex(actual_occurrences.index, fill_value=0)):
            print("[OK]")
        else:
            diff_expected = (expected_occurrences - actual_occurrences.reindex(expected_occurrences.index, fill_value=0)).replace(0, pd.NA).dropna()
            diff_actual = (actual_occurrences - expected_occurrences.reindex(actual_occurrences.index, fill_value=0)).replace(0, pd.NA).dropna()

            if not diff_expected.empty:
                print("[DIFF] missing from expected:")
                for (row_tuple, count) in diff_expected.items():
                    print("       ", row_tuple)

            if not diff_actual.empty:
                print("[DIFF] mistaken from actual:")
                for (row_tuple, count) in diff_actual.items():
                    print("       ", row_tuple)

            failures += 1

    print("\nSummary\n" + "-" * 7)
    total = len(DEFAULT_MAP)
    print(f"Files compared: {total}")
    print(f"Matched:        {total - failures}")
    print(f"Differences:    {failures}")

    return SUCCESS if failures == NO_FAILURES else FAILURE

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(ERROR)
