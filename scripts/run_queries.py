import datetime
import duckdb
import json
from pathlib import Path

DB_PATH = Path("data/analytics/product_and_design_analytics.duckdb")
QUERIES_DIR = Path("sql/queries")
OUTPUT_DIR = Path("data/output/query_results")


def run_query_file(con, file_path):
    query = file_path.read_text(encoding="utf-8")

    print(f"\nRunning {file_path}...")
    result = None
    try:
        result = con.sql(query)
    except Exception as e:
        print(f"Error running {file_path.name}: {e}")
        return {"success": False, "error": str(e), "output_file": None}

    if result is not None:
        result.show()
        result.write_csv(
            str(OUTPUT_DIR.joinpath(f"{file_path.stem}.csv")), overwrite=True
        )
        return {
            "success": True,
            "output_file": file_path.name,
            "row_count": len(result),
            "result_columns": ", ".join(result.columns),
        }


def run_queries():

    con = duckdb.connect(DB_PATH)

    query_run_logs = {"total_queries": 0, "timestamp": None, "results": {}}
    query_count = 0
    for sql_file in sorted(QUERIES_DIR.glob("*.sql")):
        query_count += 1
        query_run_logs["results"][sql_file.name] = run_query_file(con, sql_file)

    query_run_logs["total_queries"] = query_count
    query_run_logs["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(
        OUTPUT_DIR.joinpath("query_run_logs.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(query_run_logs, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    run_queries()
