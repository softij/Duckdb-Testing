import time
import duckdb
import mysql.connector
from mysql.connector import Error


# --------------------------- DUCKDB ---------------------------

def duckdb_latency(duckdb_cursor):
    # Creation a DuckDB table and insert data.
    duckdb_cursor.execute("CREATE TABLE duckdb_example (id INT, name VARCHAR)")
    duckdb_cursor.execute("INSERT INTO duckdb_example VALUES (1, 'Taylor'), (2, 'Edd'), (3, 'Charlie')")

    # Query DuckDB to check latency
    duckdb_start_time = time.time()
    duckdb_cursor.execute("SELECT * FROM duckdb_example")
    duckdb_end_time = time.time()
    duckdb_latency = duckdb_end_time - duckdb_start_time
    return duckdb_latency


# Measure query execution time for DuckDB
def duckdb_select_scalability(duckdb_cursor):

    execution_time = []

    # Creation a DuckDB table and insert data.
    duckdb_cursor.execute("CREATE TABLE musicians(id INT, instrument VARCHAR, player VARCHAR)")
    for i in range(500000):
        duckdb_cursor.execute("INSERT INTO musicians VALUES (1, 'Piano', 'Ludwig van Beethoven'), (2, 'Guitar', 'Jimi Hendrix'), (3, 'Violin', 'Itzhak Perlman'), (4, 'Flute', 'James Galway'), (5, 'Drums', 'John Bonham')")

    query_1 = "SELECT * FROM musicians LIMIT 5000"
    query_2 = "SELECT * FROM musicians  LIMIT 50000"
    query_all = "SELECT * FROM musicians"

    start_time = time.time()
    result_1 = duckdb_cursor.execute(query_1).fetchall()
    end_time = time.time()
    execution_time.append(end_time - start_time)

    start_time = time.time()
    result_2 = duckdb_cursor.execute(query_2).fetchall()
    end_time = time.time()
    execution_time.append(end_time - start_time)

    start_time = time.time()
    result_3 = duckdb_cursor.execute(query_all).fetchall()
    end_time = time.time()
    execution_time.append(end_time - start_time)

    return execution_time, len(result_1), len(result_2), len(result_3)


# --------------------------- MYSQL ---------------------------
def mysql_latency(mysql_cursor, mysql_connection):
    # Create a new MySQL table and insert data.
    mysql_cursor.execute("CREATE TABLE mysql_example (id INT, name VARCHAR(20))")
    mysql_cursor.execute("INSERT INTO mysql_example VALUES (1, 'Taylor'), (2, 'Edd'), (3, 'Charlie')")
    mysql_connection.commit()

    # Query mysql_example table to check latency.
    mysql_start_time = time.time()
    mysql_cursor.execute("SELECT * FROM mysql_example")
    mysql_end_time = time.time()
    mysql_latency = mysql_end_time - mysql_start_time
    return mysql_latency


def insert_data_into_mysql(mysql_cursor, mysql_connection):
    insert_query = "INSERT INTO musicians (instrument, player) VALUES (%s, %s)"
    data = [('Instrument ' + str(i), 'Player ' + str(i)) for i in range(1, 500000 + 1)]
    mysql_cursor.executemany(insert_query, data)
    mysql_connection.commit()


def mysql_query_execution_time(mysql_cursor):
    execution_time = []
    query_1 = "SELECT * FROM musicians LIMIT 5000"
    query_2 = "SELECT * FROM musicians  LIMIT 50000"
    query_3 = "SELECT * FROM musicians"

    start_time = time.time()
    mysql_cursor.execute(query_1)
    result_1 = mysql_cursor.fetchall()
    end_time = time.time()
    execution_time.append(end_time - start_time)

    start_time = time.time()
    mysql_cursor.execute(query_2)
    result_2 = mysql_cursor.fetchall()
    end_time = time.time()
    execution_time.append(end_time - start_time)

    start_time = time.time()
    mysql_cursor.execute(query_3)
    result_3 = mysql_cursor.fetchall()
    end_time = time.time()
    execution_time.append(end_time - start_time)

    return execution_time, len(result_1), len(result_2), len(result_3)


def mysql_select_scalability(mysql_cursor, mysql_connection):
    mysql_cursor.execute("CREATE TABLE musicians(id INT, instrument VARCHAR(255), player VARCHAR(255))")
    insert_data_into_mysql(mysql_cursor, mysql_connection)
    return mysql_query_execution_time(mysql_cursor)


if __name__ == '__main__':

    # ---------------------------- DUCKDB EXPERIMENTS ----------------------------
    # Establish DuckDB connection.
    duckdb_connection = duckdb.connect(database=':memory:')
    duckdb_cursor = duckdb_connection.cursor()

    # --------- EXPERIMENT 1 ---------
    duckdb_latency = duckdb_latency(duckdb_cursor)
    print("\n[DuckDB] Experiment #1: Latency")
    print(f"[DuckDB] Latency of select all from duckdb_example table (3 rows): {duckdb_latency} seconds")

    # --------- EXPERIMENT 2 ---------
    duckdb_execution_times, result_1, result_2, result_3 = duckdb_select_scalability(duckdb_cursor)
    print("\n[DuckDB] Experiment #2: Scalability")
    print(f"[DuckDB] Execution Time: {duckdb_execution_times[0]:.6f} seconds (Rows: {result_1})")
    print(f"[DuckDB] Execution Time: {duckdb_execution_times[1]:.6f} seconds (Rows: {result_2})")
    print(f"[DuckDB] Execution Time: {duckdb_execution_times[2]:.6f} seconds (Rows: {result_3})")

    # Close DuckDB connection.
    duckdb_connection.close()
    print("DuckDB connection is closed.\n")

    # ---------------------------- MYSQL EXPERIMENTS ----------------------------
    try:
        # Establish MySQL connection.
        mysql_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password")

        if mysql_connection.is_connected():
            db_Info = mysql_connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)

            mysql_cursor = mysql_connection.cursor(buffered=True)

            # create new database to use for experiments (if it does not exist).
            mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS example_db")
            mysql_cursor.execute("USE example_db")
            mysql_cursor.execute("DROP TABLE IF EXISTS mysql_example")
            mysql_cursor.execute("DROP TABLE IF EXISTS musicians")

    except Error as e:
        print("Error while connecting to MySQL", e)

    # --------- EXPERIMENT 1 ---------
    mysql_latency = mysql_latency(mysql_cursor, mysql_connection)
    print("\n[mySQL] Experiment #1:")
    print(f"[mySQL] Latency of select all from mySQL_example table (3 rows): {mysql_latency} seconds")

    # --------- EXPERIMENT 2 ---------
    mysql_execution_times, result_1, result_2, result_3 = mysql_select_scalability(mysql_cursor, mysql_connection)
    print("\n[mySQL] Experiment #2: Scalability")
    print(f"[mySQL] Execution Time: {mysql_execution_times[0]:.6f} seconds (Rows: {result_1})")
    print(f"[mySQL] Execution Time: {mysql_execution_times[1]:.6f} seconds (Rows: {result_2})")
    print(f"[mySQL] Execution Time: {mysql_execution_times[2]:.6f} seconds (Rows: {result_3})")

    # Clean up and close mySQL connection.
    if mysql_connection.is_connected():
        # Delete all tables created in this script.
        # Optional: can also delete all databases created.
        mysql_cursor.execute("DROP TABLE IF EXISTS mysql_example")
        mysql_cursor.execute("DROP TABLE IF EXISTS musicians")

        # Close the connection
        mysql_cursor.close()
        mysql_connection.close()
        print("MySQL connection is closed.\n")
