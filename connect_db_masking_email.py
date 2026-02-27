mport teradatasql
import pandas as pd

def mask_email(email):
    if email is None or "@" not in email:
        return email
    name, domain = email.split("@")
    return name[:2] + "*" * (len(name) - 2) + "@" + domain

# Connection
conn = teradatasql.connect(
    host="hostname",
    user="Jay",
    password="****"
)

query = "SELECT customer_id, email FROM customers"

df = pd.read_sql(query, conn)

# Apply masking
df["masked_email"] = df["email"].apply(mask_email)

print(df.head())