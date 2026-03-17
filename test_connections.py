import snowflake.connector
from apify_client import ApifyClient
from dotenv import load_dotenv
import os

load_dotenv()

def test_snowflake():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()[0]
    print(f"✅ Snowflake connected — version {version}")
    conn.close()

def test_apify():
    client = ApifyClient(os.getenv("APIFY_API_TOKEN"))
    user = client.user("me").get()
    print(f"✅ Apify connected — logged in as {user['username']}")

if __name__ == "__main__":
    test_snowflake()
    test_apify()
    print("\n🎉 All connections working — ready to build!")