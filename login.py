import os

import shioaji as sj
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SHIOAJI_API_KEY", "").strip()
secret_key = os.getenv("SHIOAJI_SECRET_KEY", "").strip()
simulation = os.getenv("SHIOAJI_SIMULATION", "true").strip().lower() == "true"

if not api_key or not secret_key:
    raise SystemExit("請先在 .env 填入 SHIOAJI_API_KEY 和 SHIOAJI_SECRET_KEY")

api = sj.Shioaji(simulation=simulation)
accounts = api.login(api_key=api_key, secret_key=secret_key)
print(accounts)
