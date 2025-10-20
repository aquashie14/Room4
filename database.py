from main import get_connection

def get_all_users(limit=20):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()  # Use dictionary=True for nice key/value results
        cur.execute("SELECT * FROM users LIMIT %s", (limit,))
        rows = cur.fetchall()
        for row in rows:
            print(row)
        return rows
    except Exception as e:
        print(f"Error querying DB: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Example usage
if __name__ == "__main__":
    get_all_users()
