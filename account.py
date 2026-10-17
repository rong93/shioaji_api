import os
from pprint import pprint

import shioaji as sj
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SHIOAJI_API_KEY", "").strip()
secret_key = os.getenv("SHIOAJI_SECRET_KEY", "").strip()
simulation = os.getenv("SHIOAJI_SIMULATION", "true").strip().lower() == "true"

if not api_key or not secret_key:
    raise SystemExit("請先在 .env 填入 SHIOAJI_API_KEY 和 SHIOAJI_SECRET_KEY")

api = sj.Shioaji(simulation=simulation)
api.login(api_key=api_key, secret_key=secret_key)

account = api.stock_account
print("模式:", "模擬" if simulation else "正式")
print("帳戶:", account)


def show(title, fn):
    print(f"\n=== {title} ===")
    try:
        pprint(fn())
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}")


show("餘額", lambda: api.account_balance(account))
show("持股", lambda: api.list_positions(account))
show("交割款", lambda: api.list_settlements(account))
show("委託", lambda: api.update_status(account))
