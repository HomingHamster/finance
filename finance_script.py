import time
import requests

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# CONFIGURE THESE
API_KEY = "YOUR_ALPACA_KEY"
API_SECRET = "YOUR_ALPACA_SECRET"
PAPER = False

# PROBABLY LEAVE THESE ALONE
TARGETS_URL = "http://finance-test.fellowship.monster/api"
POLL_SECONDS = 120
DRIFT_THRESHOLD = 0.02
MIN_NOTIONAL = 5.00

client = TradingClient(API_KEY, API_SECRET, paper=PAPER)


def get_target_weights():
    r = requests.get(TARGETS_URL, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data["target_weights"] if "target_weights" in data else data


def get_equity_and_positions():
    account = client.get_account()
    equity = float(account.equity)
    positions = {}
    for p in client.get_all_positions():
        positions[p.symbol] = float(p.market_value)
    return equity, positions


def submit_notional_order(symbol, side, notional):
    order = MarketOrderRequest(
        symbol=symbol,
        notional=round(notional, 2),
        side=side,
        time_in_force=TimeInForce.DAY,
    )
    return client.submit_order(order_data=order)


def rebalance_once():
    target_weights = get_target_weights()
    equity, positions = get_equity_and_positions()

    current_symbols = set(positions)
    target_symbols = set(target_weights)

    for symbol in sorted(current_symbols | target_symbols):
        current_value = positions.get(symbol, 0.0)
        current_weight = current_value / equity if equity > 0 else 0.0
        target_weight = float(target_weights.get(symbol, 0.0))
        diff = target_weight - current_weight

        if abs(diff) <= DRIFT_THRESHOLD:
            continue

        notional = abs(diff) * equity
        if notional < MIN_NOTIONAL:
            continue

        side = OrderSide.BUY if diff > 0 else OrderSide.SELL
        submit_notional_order(symbol, side, notional)
        print(f"{side.value.upper():4} {symbol} ${notional:.2f} "
              f"drift={diff:+.4f}")


if __name__ == "__main__":
    while True:
        clock = client.get_clock()

        if clock.is_open:
            try:
                rebalance_once()
            except Exception as e:
                print("error:", e)
        else:
            print("Market is closed.")

        time.sleep(POLL_SECONDS)
