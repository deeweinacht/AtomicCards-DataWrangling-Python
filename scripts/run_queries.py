import duckdb
from pathlib import Path

DB_PATH = Path("data/warehouse/analytics.duckdb")
SQL_DIR = Path("sql_queries")

def run_query_file(con, file_path):
    query = file_path.read_text(encoding="utf-8")

    print(f"\nRunning {file_path}...")
    result = None
    try:
        result = con.sql(query)
    except Exception as e:
        print(f"Error running {file_path.name}: {e}")
    
    if result is not None:
        result.show()


def run_queries():
    con = duckdb.connect(DB_PATH)

    ''''
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        run_query_file(con, sql_file)
    '''
    run_query_file(con, SQL_DIR.joinpath("test.sql"))

if __name__ == "__main__":
    run_queries()