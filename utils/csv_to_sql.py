import pandas as pd
import regex as re
from product_attributes_embedder import get_text_embeddings

# === Load CSV ===
df = pd.read_csv("./DB-MASCOT.csv")

# === Sanitize column names ===
def sanitize_column(name):
    name = name.strip().lower()                       # lowercase
    name = re.sub(r"[^\w]", "_", name)                # replace non-word chars with _
    if re.match(r"^\d", name):                        # if starts with digit, prefix with col_
        name = f"col_{name}"
    if len(name) > 63:                                # Postgres max identifier length
        name = name[:63]
    return name

df.columns = [sanitize_column(col) for col in df.columns]

# === Infer SQL type ===
def infer_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(series):
        return "DECIMAL(10, 2)"
    elif pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    else:
        max_len = series.astype(str).str.len().max()
        if max_len > 255:
            return "TEXT"          # use TEXT for long strings
        max_len = max(10, max_len)  # minimum length 10
        return f"VARCHAR({max_len})"

# === Generate main table DDL ===
columns_ddl = [f"id SERIAL PRIMARY KEY"]
for col in df.columns:
    columns_ddl.append(f"{col} {infer_sql_type(df[col])}")

table_name = "RawData"
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {', '.join(columns_ddl)}
);
"""

# === Generate insert statements for main table ===
def generate_inserts():
    def sql_value(val):
        if pd.isna(val):
            return "NULL"
        elif isinstance(val, str):
            val_escaped = val.replace("'", "''")
            return f"'{val_escaped}'"
        elif isinstance(val, bool):
            return 'TRUE' if val else 'FALSE'
        else:
            return str(val)
    cp = 0
    for _, row in df.iterrows():
        if cp>20:
            break  # Limit to first 20 rows for testing
        cp+=1
        values = ", ".join(sql_value(row[col]) for col in df.columns)
        yield f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({values});"

# === Create per-attribute enumeration tables ===
attributes = ["Product Type",
            "Range",
            "Certification",
            "Industry name",
            "Product type attributes",
            "Segments",
            "Quality",
            "Colour",
            "Quality Number",
            "Washing Symbol Name",
            "Product Categories",
            "Brand",
            ]

def generate_enum_tables():
    for col in attributes:
        enum_table = sanitize_column(col)
        create_enum_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {enum_table} (
            id SERIAL PRIMARY KEY,
            value TEXT UNIQUE,
            embedding VECTOR(384) -- assuming 384-dim embeddings with all-MiniLM-L6-v2
        );
        """
        yield create_enum_table_sql



def populate_enum_tables():
    for col in attributes:
        enum_table = sanitize_column(col)
        print(f"Processing enum table: {enum_table}")
        if col == "Product type attributes":
            # Split multi-value attributes by ';' and ',' and clean up
            unique_values = (
                df[enum_table]
                .astype(str)
                .str.split(r";\s*")          # split by ';' or '; '
                .explode()                   # expand list entries into rows 
                .str.split(r",\s*")         
                .explode()
                .dropna()
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .drop_duplicates()
                .unique()
            )
        else:
            # Split multi-value attributes by ';' or '; ' and clean up
            unique_values = (
                df[enum_table]
                .astype(str)
                .str.split(r";\s*")          # split by ';' or '; '
                .explode()                   # expand list entries into rows
                .dropna()
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .drop_duplicates()
                .unique()
            )

        cp = 0
        for val in unique_values:
            if val=="nan":
                continue 
            if cp > 20:
                break  # Limit to first 20 unique values for testing
            cp += 1

            # Generate embedding for the clean value
            embedding = get_text_embeddings(val)
            if embedding is None:
                continue

            embedding_str = ",".join([f"{x:.6f}" for x in embedding])
            val_escaped = str(val).replace("'", "''")

            insert_enum_sql = f"""
                INSERT INTO {enum_table} (value, embedding)
                VALUES ('{val_escaped}', ARRAY[{embedding_str}]::vector)
                ON CONFLICT (value) DO NOTHING;
            """
            yield insert_enum_sql

    
# === Create table ===
create_sizes_table_sql = """
CREATE TABLE IF NOT EXISTS Sizes (
    id SERIAL PRIMARY KEY,
    value TEXT UNIQUE
);
"""
           
def create_sizes_table():
    # Columns of interest
    size_columns = [
        "eu_size",
        "eu_size_part_1",
        "eu_size_part_2",
        "uk_size",
        "uk_size_part_1",
        "uk_size_part_2",
        "us_size",
        "us_size_part_1",
        "us_size_part_2",
    ]

    # Collect all values across columns
    all_sizes = pd.Series(dtype=str)
    for col in size_columns:
        col_values = (
            df[col]
            .astype(str)
            .str.split(";")
            .explode()
            .dropna()
            .str.strip()
            .replace("", pd.NA)
            .dropna()
        )
        all_sizes = pd.concat([all_sizes, col_values])

    # Deduplicate
    unique_sizes = all_sizes.drop_duplicates().unique()

    # === Insert values ===
    for val in unique_sizes:
        if val=="nan":
            continue 
        val_escaped = str(val).replace("'", "''")
        insert_sql = f"INSERT INTO Sizes (value) VALUES ('{val_escaped}') ON CONFLICT (value) DO NOTHING;"
        yield insert_sql

# === Create product_data table with unique product_quality_colour_number entries ===
size_columns = [c for c in df.columns if "size" in c.lower()]

# Aggregate size columns
def merge_sizes(df):
    merged = df.groupby("product_quality_colour_number")[size_columns].agg(
        lambda col: "; ".join(
            sorted(set(str(x).strip() for x in col if pd.notna(x) and x != ""))
        )
    )
    merged.reset_index(inplace=True)
    return merged

def normalize_multivalues(df):
    for col in df.columns:
        if df[col].dtype == object:
            if col == "product_type_attributes":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\s*,\s*", " | ", regex=True)
                    .replace({"nan": None})
                )
            else:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\s*;\s*", " | ", regex=True)
                    .replace({"nan": None})
                )
    return df

def build_product_data(df):
    merged_sizes = merge_sizes(df)
    df_no_sizes = df.drop(columns=size_columns).drop_duplicates(
        subset=["product_quality_colour_number"], keep="first"
    )
    cleaned = df_no_sizes.merge(merged_sizes, on="product_quality_colour_number", how="left")
    cleaned = normalize_multivalues(cleaned)
    return cleaned

def build_product_data(df):
    merged_sizes = merge_sizes(df)
    df_no_sizes = df.drop(columns=size_columns).drop_duplicates(
        subset=["product_quality_colour_number"], keep="first"
    )
    cleaned = df_no_sizes.merge(merged_sizes, on="product_quality_colour_number", how="left")
    cleaned = normalize_multivalues(cleaned)
    return cleaned

cleaned_df = build_product_data(df)

columns_ddl_cleaned = [f"id SERIAL PRIMARY KEY"]
for col in cleaned_df.columns:
    columns_ddl_cleaned.append(f"{col} {infer_sql_type(cleaned_df[col])}")

create_cleaned_table_sql = f"""
CREATE TABLE IF NOT EXISTS product_data (
    {', '.join(columns_ddl_cleaned)}
);
"""

def sql_value(val):
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, str):
        val_escaped = val.replace("'", "''")
        return f"'{val_escaped}'"
    elif isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    else:
        return str(val)

def generate_cleaned_inserts():
    cp = 0
    for _, row in cleaned_df.iterrows():
        if cp>20:
            break  # Limit to first 20 rows for testing
        cp+=1
        values = ", ".join(sql_value(row[col]) for col in cleaned_df.columns)
        yield f"INSERT INTO product_data ({', '.join(cleaned_df.columns)}) VALUES ({values});"

# === Detect 1:1 vs N:N relationships (fixed for " | " separator) ===
def detect_relationship_types(df, attributes):
    one_to_one = []
    many_to_many = []
    for attr in attributes:
        col = sanitize_column(attr)
        if col not in df.columns:
            continue
        # Look specifically for the literal " | " token
        if df[col].astype(str).str.contains(r"\s\|\s", regex=True, na=False).any():
            many_to_many.append(col)   # already sanitized name
        else:
            one_to_one.append(col)
    return one_to_one, many_to_many

# === Create bridge tables for many-to-many attributes ===
def generate_bridge_tables(many_to_many):
    for attr in many_to_many:
        enum_table = sanitize_column(attr)
        bridge_table = f"product_data_{enum_table}_map"
        create_bridge_sql = f"""
        CREATE TABLE IF NOT EXISTS {bridge_table} (
            cleaned_id BIGINT REFERENCES product_data(id) ON DELETE CASCADE,
            {enum_table}_id BIGINT REFERENCES {enum_table}(id) ON DELETE CASCADE,
            PRIMARY KEY (cleaned_id, {enum_table}_id)
        );
        """
        yield create_bridge_sql


# === Add foreign key columns for one-to-one attributes ===
def generate_fk_alter_statements(one_to_one):
    for attr in one_to_one:
        enum_table = sanitize_column(attr)
        alter_sql = f"""
        ALTER TABLE product_data
        ADD COLUMN IF NOT EXISTS {enum_table}_id BIGINT REFERENCES {enum_table}(id);
        """
        yield alter_sql


# === Populate 1:1 relationships (fixed) ===
# === Populate 1:1 relationships (with enum upsert) ===
def populate_one_to_one(cleaned_df, one_to_one):
    for _, row in cleaned_df.iterrows():
        key = str(row["product_quality_colour_number"]).strip()
        if not key or key.lower() in ("nan", "none"):
            continue
        key_escaped = key.replace("'", "''")

        for attr in one_to_one:
            val = str(row[attr]).strip()
            if not val or val.lower() in ("nan", "none"):
                continue
            val_escaped = val.replace("'", "''")

            # Upsert enum value and then set FK via a COALESCE of inserted/existing id
            yield f"""
            WITH v(value) AS (VALUES ('{val_escaped}')),
            ins AS (
                INSERT INTO {attr}(value)
                SELECT value FROM v
                ON CONFLICT (value) DO NOTHING
                RETURNING id, value
            )
            UPDATE product_data p
            SET {attr}_id = COALESCE(
                (SELECT id FROM ins),
                (SELECT id FROM {attr} WHERE value = (SELECT value FROM v))
            )
            WHERE p.product_quality_colour_number = '{key_escaped}';
            """

# === Populate N:N (bridge) relationships (split on " | " + enum upsert) ===
def populate_bridge_tables(cleaned_df, many_to_many):
    for _, row in cleaned_df.iterrows():
        key = str(row["product_quality_colour_number"]).strip()
        if not key or key.lower() in ("nan", "none"):
            continue
        key_escaped = key.replace("'", "''")

        for attr in many_to_many:
            # values are normalized as "... | ... | ..."
            raw = str(row[attr])
            if not raw or raw.lower() in ("nan", "none"):
                continue

            # Split on " | " (allow extra spaces just in case)
            for val in re.split(r"\s*\|\s*", raw):
                val = val.strip()
                if not val or val.lower() in ("nan", "none"):
                    continue
                val_escaped = val.replace("'", "''")
                bridge_table = f"product_data_{attr}_map"

                # Upsert enum value, then insert bridge row using the resolved id
                yield f"""
                WITH v(value) AS (VALUES ('{val_escaped}')),
                ins AS (
                    INSERT INTO {attr}(value)
                    SELECT value FROM v
                    ON CONFLICT (value) DO NOTHING
                    RETURNING id, value
                ),
                enum_id AS (
                    SELECT COALESCE(
                        (SELECT id FROM ins),
                        (SELECT id FROM {attr} WHERE value = (SELECT value FROM v))
                    ) AS id
                )
                INSERT INTO {bridge_table} (cleaned_id, {attr}_id)
                SELECT c.id, e.id
                FROM product_data c
                CROSS JOIN enum_id e
                WHERE c.product_quality_colour_number = '{key_escaped}'
                ON CONFLICT DO NOTHING;
                """

# === Execute detection and generate SQL ===
one_to_one, many_to_many = detect_relationship_types(cleaned_df, attributes)

# Generate DDLs
bridge_table_sql = list(generate_bridge_tables(many_to_many))
fk_alter_sql = list(generate_fk_alter_statements(one_to_one))

# Populate data links
one_to_one_inserts = list(populate_one_to_one(cleaned_df, one_to_one))
many_to_many_inserts = list(populate_bridge_tables(cleaned_df, many_to_many))

