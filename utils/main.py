from connect import cur, conn
from csv_to_sql import create_sizes_table_sql, create_sizes_table, create_table_sql, generate_enum_tables, populate_enum_tables, table_name, generate_inserts

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
    
    # Execute SQL for enum tables
    for create_enum_sql in generate_enum_tables():
        cur.execute(create_enum_sql)
    print("✅ Created enum tables")

    # Insert data
    cp = 0
    for insert_sql in populate_enum_tables():
        cur.execute(insert_sql)
        cp += 1
    print(f"✅ Inserted {cp} rows into enum tables")

    # Execute SQL
    cur.execute(create_sizes_table_sql)
    print(f"✅ Created table: Sizes")

    cp = 0
    for insert_sql in create_sizes_table():
        cur.execute(insert_sql)
        cp += 1
    print(f"✅ Inserted {cp} rows into Sizes tables")

    # Commit and close
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
