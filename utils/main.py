import sys
from connect import cur, conn
from csv_to_sql import (
    create_sizes_table_sql,
    create_sizes_table,
    create_table_sql,
    create_cleaned_table_sql,
    generate_cleaned_inserts,
    generate_enum_tables,
    populate_enum_tables,
    table_name,
    generate_inserts,
    cleaned_df,
    attributes,
    detect_relationship_types,
    generate_bridge_tables,
    generate_fk_alter_statements,
    populate_one_to_one,
    populate_bridge_tables,
)

def drop_all_tables():
    """Drop all relevant tables safely in dependency order."""
    drop_sql = """
    DROP TABLE IF EXISTS
        cleaned_data_segments_map,
        cleaned_data_certification_map,
        cleaned_data_quality_map,
        cleaned_data_colour_map,
        cleaned_data_range_map,
        cleaned_data_quality_number_map,
        cleaned_data_product_type_map,
        cleaned_data_industry_name_map,
        cleaned_data_product_type_attributes_map,
        cleaned_data;

    DROP TABLE IF EXISTS
        sizes,
        range,
        colour,
        quality,
        certification,
        segments,
        product_type,
        product_type_attributes,
        industry_name,
        quality_number,
        rawdata
    CASCADE;
    """
    cur.execute(drop_sql)
    conn.commit()
    print("🗑️  All tables dropped successfully.")

def main():
    try:
        if "--drop" in sys.argv:
            drop_all_tables()
        
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

        cur.execute(create_cleaned_table_sql)
        print(f"✅ Created table: Cleaned Data")


        cp = 0
        for insert_sql in generate_cleaned_inserts():
            cur.execute(insert_sql)
            cp += 1
        print(f"✅ Inserted {cp} rows into cleaned_data table")
        
        # === RELATIONSHIP DETECTION ===
        one_to_one, many_to_many = detect_relationship_types(cleaned_df, attributes)
        print(f"🔍 1:1 attributes: {one_to_one}")
        print(f"🔍 N:N attributes: {many_to_many}")

        # === CREATE BRIDGE TABLES ===
        for create_bridge_sql in generate_bridge_tables(many_to_many):
            cur.execute(create_bridge_sql)
        print("✅ Created bridge tables for many-to-many attributes")

        # === ADD FOREIGN KEYS FOR 1:1 ===
        for alter_sql in generate_fk_alter_statements(one_to_one):
            cur.execute(alter_sql)
        print("✅ Added foreign key columns for 1:1 attributes")

        # === POPULATE RELATIONSHIPS ===
        cp = 0
        for insert_sql in populate_one_to_one(cleaned_df, one_to_one):
            cur.execute(insert_sql)
            cp += 1
        print(f"✅ Populated {cp} one-to-one relationships")

        cp = 0
        for insert_sql in populate_bridge_tables(cleaned_df, many_to_many):
            cur.execute(insert_sql)
            cp += 1
        print(f"✅ Populated {cp} many-to-many bridge relationships")

        # Commit and close
        conn.commit()
    except Exception as e:
        print("❌ An error occurred:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
