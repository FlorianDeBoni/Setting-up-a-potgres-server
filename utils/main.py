from connect import cur, conn
from csv_to_sql import create_table_sql, table_name, generate_inserts

def main():

    
    # Execute SQL
    cur.execute(create_table_sql)
    print(f"✅ Created table: {table_name}")

    # Insert data
    cp = 0
    for insert_sql in generate_inserts():
        cur.execute(insert_sql)
        cp += 1
    print(f"✅ Inserted {cp} rows into {table_name}")

    # Commit and close
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
