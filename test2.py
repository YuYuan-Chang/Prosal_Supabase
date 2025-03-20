# Option 2: Paginate through all rows in the "notices" table
import os
from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

all_data = []
limit = 1000  # batch size
offset = 0

while True:
    response = (
        supabase.table("notices")
        .select("*")
        .range(offset, offset + limit - 1)
        .execute()
    )

    # If no more data is returned, break out of the loop.
    if not response.data:
        break

    all_data.extend(response.data)
    offset += limit

print("Total rows fetched:", len(all_data))