import pandas as pd
import regex as re

# Load the CSV
df = pd.read_csv("./DB-MASCOT.csv")
# Sanitize column names
def sanitize_column(name):
    name = name.strip().lower()                       # lowercase
    name = re.sub(r"[^\w]", "_", name)               # replace non-word chars with _
    if re.match(r"^\d", name):                       # if starts with digit, prefix with col_
        name = f"col_{name}"
    if len(name) > 63:                               # Postgres max identifier length
        name = name[:63]
    return name

df.columns = [sanitize_column(col) for col in df.columns]

# Infer SQL type
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

# Generate columns
columns_ddl = [f"id SERIAL PRIMARY KEY"]
for col in df.columns:
    columns_ddl.append(f"{col} {infer_sql_type(df[col])}")

# Create table
table_name = "RawData"
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {', '.join(columns_ddl)}
);
"""

def generate_inserts():
    def sql_value(val):
        if pd.isna(val):
            return "NULL"
        elif isinstance(val, str):
            # Escape single quotes by doubling them
            val_escaped = val.replace("'", "''")
            return f"'{val_escaped}'"
        elif isinstance(val, bool):
            return 'TRUE' if val else 'FALSE'
        else:
            return str(val)

    cp = 0
    for _, row in df.iterrows():
        if cp < 20:
            cp+=1
            # print(cp)
            values = ", ".join(sql_value(row[col]) for col in df.columns)
            yield f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({values});"
        else:
            break