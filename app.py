from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import json
import html

app = FastAPI()
JSON_PATH = "/app/rebal/rebalance_plan2.json"


def load_data():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_target_weights():
    return load_data().get("target_weights", {})


CONSUMER_SCRIPT = r'''import time
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
'''


@app.get("/", response_class=HTMLResponse)
def homepage():
    data = load_data()
    weights = data.get("target_weights", {})
    drift = data.get("config", {}).get("drift_threshold", 0.02)

    rows = "".join(
        f"""
        <tr>
          <td>{html.escape(symbol)}</td>
          <td>{weight:.6f}</td>
        </tr>
        """
        for symbol, weight in weights.items()
    )

    escaped_script = html.escape(CONSUMER_SCRIPT)

    return f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Target Weights API</title>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/normalize.css@8.0.1/normalize.min.css">
      <style>
        :root {{
          --bg: #f3f5f1;
          --panel: #fbfcfa;
          --panel-2: #f7faf6;
          --line: #d8e0d5;
          --text: #203024;
          --muted: #5f7365;
          --soft: #7f9183;
          --accent: #5f7f62;
          --accent-dark: #48624b;
          --code-bg: #eef3ed;
          --shadow: 0 10px 30px rgba(40, 60, 44, 0.08);
          --radius: 18px;
          --radius-sm: 12px;
          --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
          --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }}

        * {{
          box-sizing: border-box;
        }}

        html {{
          scroll-behavior: smooth;
        }}

        body {{
          margin: 0;
          font-family: var(--sans);
          background:
            radial-gradient(circle at top left, rgba(116, 146, 118, 0.10), transparent 32%),
            linear-gradient(180deg, #f6f8f4 0%, var(--bg) 100%);
          color: var(--text);
          line-height: 1.6;
        }}

        .wrap {{
          width: min(980px, calc(100% - 32px));
          margin: 0 auto;
          padding: 32px 0 56px;
        }}

        .hero {{
          background: linear-gradient(180deg, rgba(251, 252, 250, 0.96), rgba(247, 250, 246, 0.98));
          border: 1px solid var(--line);
          border-radius: var(--radius);
          padding: 28px;
          box-shadow: var(--shadow);
        }}

        .eyebrow {{
          display: inline-block;
          font-size: 12px;
          letter-spacing: 0.12em;
          text-transform: uppercase;
          color: var(--accent-dark);
          background: #e8efe7;
          border: 1px solid #d4ddd2;
          border-radius: 999px;
          padding: 6px 10px;
          margin-bottom: 14px;
        }}

        h1 {{
          margin: 0 0 10px;
          font-size: clamp(32px, 5vw, 52px);
          line-height: 1.05;
          letter-spacing: -0.03em;
          font-weight: 700;
        }}

        .lead {{
          margin: 0;
          color: var(--muted);
          font-size: 17px;
        }}

        .grid {{
          display: grid;
          grid-template-columns: 1fr;
          gap: 20px;
          margin-top: 20px;
        }}

        @media (min-width: 860px) {{
          .grid {{
            grid-template-columns: 320px 1fr;
            align-items: start;
          }}
        }}

        .card {{
          background: rgba(251, 252, 250, 0.92);
          border: 1px solid var(--line);
          border-radius: var(--radius);
          box-shadow: var(--shadow);
          overflow: hidden;
        }}

        .card-head {{
          padding: 18px 20px 10px;
        }}

        .card-head h2 {{
          margin: 0;
          font-size: 18px;
          letter-spacing: -0.02em;
          margin-top: 1em;
        }}

        .card-head p, .card-head li {{
          margin: 6px 0 0;
          color: var(--muted);
          font-size: 14px;
        }}

        .pad {{
          padding: 0 20px 20px;
        }}

        table {{
          width: 100%;
          border-collapse: collapse;
          font-size: 15px;
        }}

        th, td {{
          text-align: left;
          padding: 10px 0;
          border-bottom: 1px solid var(--line);
        }}

        th {{
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: var(--soft);
          font-weight: 600;
        }}

        td:last-child, th:last-child {{
          text-align: right;
          font-variant-numeric: tabular-nums;
        }}

        .meta {{
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          margin-top: 16px;
        }}

        .pill {{
          display: inline-flex;
          align-items: center;
          gap: 8px;
          border: 1px solid var(--line);
          background: var(--panel-2);
          color: var(--muted);
          padding: 8px 12px;
          border-radius: 999px;
          font-size: 13px;
        }}

        .api-link {{
          color: var(--accent-dark);
          text-decoration: none;
          border-bottom: 1px solid rgba(72, 98, 75, 0.25);
        }}

        .api-link:hover {{
          border-bottom-color: rgba(72, 98, 75, 0.65);
        }}

        .code-wrap {{
          background: var(--code-bg);
          border-top: 1px solid var(--line);
        }}

        .code-snippet {{
          background: var(--code-bg);
          border: 1px solid var(--line);
          width: fit-content;
          margin: 0.7em 0 0.7em 0;
        }}

        pre {{
          margin: 0;
          padding: 20px;
          overflow-x: auto;
          font-family: var(--mono);
          font-size: 13px;
          line-height: 1.55;
          color: #1d2d21;
        }}

        .code-snippet pre {{
          padding: 0.6em;
        }}

        code {{
          font-family: var(--mono);
        }}

        .small {{
          color: var(--soft);
          font-size: 13px;
        }}
      </style>
    </head>
    <body>
      <main class="wrap">
        <section class="hero">
          <div style="margin-bottom: 3em;" class="eyebrow">Test Project</div>
          <h1>Target weights example.</h1>
          <p style="margin-bottom: 2.3em;" class="lead">
            A minimal homepage for viewing my current target weights, and a compact consumer script that polls
            this website's <a class="api-link" href="/api">/api</a> endpoint to rebalance when your portfolio
            drift exceeds {drift:.2%}. Later, this project will likely focus on green stocks and smaller 
            businesses, but it does not at the moment.
          </p>
          <div class="meta">
            <div class="pill">API route: /api</div>
            <div class="pill">Symbols: {len(weights)}</div>
          </div>
        </section>

        <section class="grid">
          <article class="card">
            <div class="card-head">
              <h2>Current Target Weights</h2>
              <p>These numbers are the ratio out of a total of 1 that would be in each stock.</p>
              <p>Weights might not change while the market is closed.</p>
            </div>
            <div class="pad">
              <table>
                <thead>
                  <tr>
                    <th>Ticker Symbol</th>
                    <th>Weight</th>
                  </tr>
                </thead>
                <tbody>
                  {rows}
                </tbody>
              </table>
            </div>
          </article>

          <article class="card">
            <div class="card-head">

              <h2>Not Financial Advice</h2>
              <p>
                The information provided on this platform is for general informational and educational purposes only. It does not constitute, and should not be considered, professional or personal financial advice. Before making any financial or investment decisions, you should consult a licensed financial advisor to assess your specific personal circumstances, financial situation, and objectives.
              </p>


              <h2>How To Install</h2>
              <p>Please read fully before using this script.</p>
              <p><ol>
                <li>Sign up for a "Trading API" account on <a href="https://alpaca.markets">Alpaca Markets</a>, and get your ID approved. This is a trading broker that will allow you to
                  invest and then control your investment using an API, with free sign up.</li>
                <li>Log in.</li>
                <li>Select Live Trading mode (with real money), not Paper Trading mode (with fake money). Do this using the dropdown selector in the top left, so you can make a real cash deposit. You can
                  test how the script works using just Paper Trading mode if you want to.</li>
                <li>Deposit money, via Crypto or Bank Transfer. You will likely want ~$30 or more. You can add funds with the Funds & Wallet menu item which is on the left of the Alpaca website when in Live
                  Trading mode. Remember there are fees for deposits, trades, and withdrawals.</li>
                <li>On the right-hand side of the main dashboard page, halfway down, regenerate your API keys. They will be generated for Live Trading if you are in Live Trading mode when you generate them,
                  or Paper Trading if you are in Paper Trading mode when you generate them.</li>
                <li>Copy the keys into the places in the script where it says to configure API_KEY and SECRET_KEY.</li>
                <li>Check the PAPER setting in the script is set to match your if API keys are for Paper Trading or Live Trading. By default, the PAPER variable is False, the default is for Live Trading mode,
                  for Paper Trading mode change this to True.</li>
                <li>Save the script below into a file called something like "finance_script.py". When we run the code, we will use this name.</li>
                <li>Open a terminal window with <a href="https://python.org">Python</a> already installed, and run the following <a href="https://pypi.org/project/pip/">pip</a> install command
                  (pip usually comes bundled with Python by default):
                  <div class="code-snippet"><pre><code>pip install requests alpaca-py</pre></code></div></li>
                <li>Run this next command below to start the rebalancing script. You should make sure the script stays actively running the entire time that you want your portfolio rebalanced, if your computer sleeps
                  the script will not actively reblance your portfolio.
                  <div class="code-snippet"><pre><code>python3 finance_script.py</pre></code></div>
                  When the market is closed the script will print a line to your terminal window every two minutes, saying the market is closed.
                </li>
                <li>Once you get to the point of withdrawing your funds again, you might want to know the following things:
                  <ul>
                    <li>Once you have stopped the script you need to manually liquidate the stocks you have invested in. You can do this using the little (X) button on the right-hand side of the Position section of the Dashboard.
                    <li>You will often have to wait for the regular market opening hours to sell the stock.</li>
                    <li>There is a waiting time for a stock sale to settle once you have sold it, before it can be withdrawn.</li>
                    <li>Then there is sometimes a waiting time for withdrawals to clear into your bank as well.</li>
                    <li>Withdrawing Bitcoin can be faster, once your address is already added and cleared. You can do this by buying the BTC/USD ticker, you can then action a Bitcoin withdrawal using the left-hand
                      menu under Funds & Wallet.</li>
                    <li>Adding a new bitcoin address requires a 24h Crypto withdrawals security hold.</li>
                    <li>You should plan for around 3+ days for making withdrawals, and you might need to check more than once.</li>
                  </ul>
                </li>
                <li>Done. Hopefully your balance will go up or down.</li>
              </ol></p>

              <p><small>Running the ingestion script that provides the ticker weights here costs about $2.50 a day, you can donate; but only if it actually makes you feel better.
                You are encouraged to avoid donating if you aren't sure that you want to: <a href="https://circuspam.coffee/tips">Circuspam Tips</a>.</small></p>

              <h2>Consumer Script</h2>
              <p>
                This script allows you to view and follow my personal investments yourself, in an automated way (subject to potential change, this is a test). It polls the API on this site,
                and it submits market orders to rebalance your entire alpaca account balance using your own account keys that you add for the Alpaca Markets Trading API (which am unaffiliated with).
              </p>
            </div>
            <div class="code-wrap">
              <pre><code>{escaped_script}</code></pre>
            </div>
          </article>
        </section>
        <div style="margin: 100px; text-align: center">
          <p class="small"><a href="https://github.com/HomingHamster/finance">Green Finance Framework</a> by <a href="https://circuspam.coffee">Felix Farquharson</a> is marked <a href="https://creativecommons.org/publicdomain/zero/1.0/">CC0 1.0</a><img src="https://mirrors.creativecommons.org/presskit/icons/cc.svg" alt="" style="max-width: 1em;max-height:1em;margin-left: .2em;"><img src="https://mirrors.creativecommons.org/presskit/icons/zero.svg" alt="" style="max-width: 1em;max-height:1em;margin-left: .2em;"></p>
        </div>
      </main>
    </body>
    </html>
    """


@app.get("/api")
def api():
    return {"target_weights": load_target_weights()}
