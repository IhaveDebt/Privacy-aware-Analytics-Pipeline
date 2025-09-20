"""
Privacy-aware Analytics Pipeline
Single-file demo of SQL analytics with differential privacy.
"""

import psycopg
import numpy as np

DB_URL = "postgresql://postgres:postgres@localhost:5432/privacy_db"
PRIVACY_BUDGET = 1.0  # total epsilon
USED_BUDGET = 0.0

def init_db():
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Users table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT,
                age INT,
                email TEXT
            );
            """)
            # Seed sample data
            cur.execute("""
            INSERT INTO users (name, age, email)
            SELECT 'User' || i, (20 + (i % 30)), 'user' || i || '@example.com'
            FROM generate_series(1,100) AS s(i)
            ON CONFLICT DO NOTHING;
            """)

def laplace_noise(scale):
    return np.random.laplace(0, scale)

def query_with_dp(sql, epsilon=0.1):
    global USED_BUDGET
    if USED_BUDGET + epsilon > PRIVACY_BUDGET:
        print("Privacy budget exceeded!")
        return None
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
            noisy_result = tuple(r + laplace_noise(1/epsilon) if isinstance(r,(int,float)) else r for r in result)
            USED_BUDGET += epsilon
            return noisy_result

def remaining_budget():
    return PRIVACY_BUDGET - USED_BUDGET

if __name__ == "__main__":
    init_db()
    print("Privacy-aware Analytics Pipeline (SQL + DP)")
    print(f"Total privacy budget: {PRIVACY_BUDGET}\n")
    while True:
        query = input("Enter SQL query (or 'exit'): ")
        if query.lower() == "exit":
            break
        epsilon = float(input("Privacy epsilon for this query (e.g., 0.1): "))
        result = query_with_dp(query, epsilon)
        if result is not None:
            print(f"Result (DP applied): {result}")
            print(f"Remaining privacy budget: {remaining_budget()}\n")
