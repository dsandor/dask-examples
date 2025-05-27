#!/usr/bin/env python3
import psycopg2
import json

def verify_data():
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname="csvdata",
            user="postgres",
            password="Password123",
            host="localhost"
        )
        
        with conn.cursor() as cur:
            # Get the count of records
            cur.execute("SELECT COUNT(*) FROM test_equity")
            count = cur.fetchone()[0]
            print(f"Total records in test_equity: {count}")
            
            # Get the first few records
            cur.execute("""
                SELECT id_bb_global, data
                FROM test_equity
                LIMIT 5;
            """)
            
            print("\nSample records:")
            print("=" * 50)
            for row in cur.fetchall():
                try:
                    id_bb_global = row[0]
                    data = row[1]
                    print(f"\nID: {id_bb_global}")
                    print("-" * 50)
                    
                    if data:
                        if isinstance(data, dict):
                            print(f"Number of fields: {len(data)}")
                            print("First 5 fields:")
                            for i, (k, v) in enumerate(data.items()):
                                if i >= 5:
                                    print("  ...")
                                    break
                                print(f"  {k}: {v}")
                        else:
                            print(f"Data type: {type(data).__name__}")
                            print(f"Data: {data}")
                    else:
                        print("No data")
                        
                except Exception as e:
                    print(f"Error processing row: {e}")
                print("-" * 50)
            
            # Check if our problematic ID exists
            cur.execute("""
                SELECT id_bb_global, data IS NOT NULL as has_data
                FROM test_equity 
                WHERE id_bb_global = 'LgyMu6dSbEP4';
            """)
            
            problem_id = cur.fetchone()
            if problem_id:
                print("\nProblematic ID (LgyMu6dSbEP4) found in database!")
                print(f"Has data: {problem_id[1]}")
            else:
                print("\nProblematic ID (LgyMu6dSbEP4) not found in database.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    verify_data()
