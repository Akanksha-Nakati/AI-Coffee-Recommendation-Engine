import os
from dotenv import load_dotenv
load_dotenv()
print("DATABASE_URL:", os.environ.get("DATABASE_URL"))

