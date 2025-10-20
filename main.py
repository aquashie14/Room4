"""Simple MySQL connection helpers and a safe sample query.

This module provides:
- get_connection() to create a mysql.connector connection from environment
  variables (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME).
- test_connection() to validate connectivity and print server info.
- sample_query() demonstrates running a simple SELECT using the helper.

Run directly to test your configuration:
  python main.py
"""

import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

load_dotenv()


def get_connection():
    """Create and return a mysql.connector connection using env vars.

    Raises ValueError if required variables are missing. Raises mysql.connector.Error
    on connection problems.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")

    missing = [name for name, val in (
        ("DB_HOST", host), ("DB_USER", user), ("DB_PASSWORD", password)
    ) if not val]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # Convert port to int, let mysql.connector raise if invalid
    try:
        port = int(port)
    except ValueError:
        raise ValueError("DB_PORT must be an integer")

    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database or None,
        autocommit=True,
    )
    return conn


def test_connection():
    """Try to open and close a connection, printing a short status message."""
    conn = None
    try:
        conn = get_connection()
        if conn.is_connected():
            server_info = conn.server_info
            print(f"Connected to MySQL server version {server_info}")
        else:
            print("Connection object created but not connected")
    except ValueError as ve:
        print(f"Configuration error: {ve}")
    except Error as err:
        print(f"Error connecting to MySQL: {err}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def sample_query(limit=10):
    """Run a sample query against a `users` table and print up to `limit` rows.

    This function is optional and will only run when executed directly.
    It uses `get_connection()` so it will raise the same config errors.
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users")
        rows = cur.fetchmany(limit)
        print(f"Fetched {len(rows)} rows (showing up to {limit}):")
        for r in rows:
            print(r)
    except ValueError as ve:
        print(f"Configuration error: {ve}")
    except Error as err:
        print(f"Error running sample query: {err}")
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    # Runs a configuration check; uncomment sample_query() to try an example SELECT
    test_connection()
    # sample_query()