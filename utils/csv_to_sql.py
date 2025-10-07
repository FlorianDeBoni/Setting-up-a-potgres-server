
import pandas as pd
import regex as re

# --- Load CSV ---
df = pd.read_csv("./DB-MASCOT.csv")

# --- Sanitize column names ---
def sanitize_column(name):
    name = name.strip().lower()
    name = re.sub(r"[^\w]", "_", name)
    if re.match(r"^\d", name):
        name = f"col_{name}"
    if len(name) > 63:
        name = name[:63]
    return name

df.columns = [sanitize_column(col) for col in df.columns]

attributes = ["Product Type",
                "Range",
                "Certification",
                "Industry name",
                "Product type attributes",
                "Segments",
                "Quality",
                "Colour",
                "Quality Number"]

# --- Infer SQL type ---
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
            return "TEXT"
        max_len = max(10, max_len)
        return f"VARCHAR({max_len})"

# --- Generate SQL schema ---
table_name = "rawdata"
columns_ddl = [f"id SERIAL PRIMARY KEY"]
for col in df.columns:
    if col not in detect_enum_columns(df):
        columns_ddl.append(f"{col} {infer_sql_type(df[col])}")

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {', '.join(columns_ddl)}
);
"""

# --- Create child tables for enum columns ---
child_tables_sql = []
for col in enum_columns:
    col_table = f"{table_name}_{col}"
    child_tables_sql.append(f"""
CREATE TABLE IF NOT EXISTS {col_table} (
    id SERIAL PRIMARY KEY,
    {table_name}_id INT REFERENCES {table_name}(id),
    {col} TEXT
);
""")

# --- Generate INSERT statements ---
def generate_inserts():
    def sql_value(val):
        if pd.isna(val):
            return "NULL"
        elif isinstance(val, str):
            return "'" + val.replace("'", "''") + "'"
        elif isinstance(val, bool):
            return 'TRUE' if val else 'FALSE'
        else:
            return str(val)

    base_cols = [c for c in df.columns if c not in enum_columns]
    for _, row in df.iterrows():
        base_values = ", ".join(sql_value(row[c]) for c in base_cols)
        yield f"INSERT INTO {table_name} ({', '.join(base_cols)}) VALUES ({base_values});"
        for col in enum_columns:
            if pd.notna(row[col]):
                for val in str(row[col]).split(';'):
                    val = val.strip()
                    if val:
                        child_table = f"{table_name}_{col}"
                        yield f"INSERT INTO {child_table} ({table_name}_id, {col}) VALUES (CURRVAL(pg_get_serial_sequence('{table_name}', 'id')), {sql_value(val)});"