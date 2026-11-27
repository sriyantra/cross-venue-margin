"""Records closing prices of the same perpetual on both venues, minute by
minute, into study/data. Standard library only: python3 study/fetch.py"""
import csv, json, time, urllib.request

HL = "https://api.hyperliquid.xyz/info"
OKX = "https://www.okx.com/api/v5/market/history-candles"
BARS = {"1m": "1m", "5m": "5m", "1h": "1H"}


def get(url, body=None):
    data = json.dumps(body).encode() if body else None
    request = urllib.request.Request(
        url, data, {"content-type": "application/json", "user-agent": "hedge-study"}
    )
    with urllib.request.urlopen(request, timeout=30) as reply:
        return json.load(reply)


def hyperliquid(coin, interval):
    req = {"coin": coin, "interval": interval, "startTime": 0, "endTime": int(time.time() * 1000)}
    candles = get(HL, {"type": "candleSnapshot", "req": req})
    return {c["t"]: c["c"] for c in candles}


def okx(coin, interval, start):
    closes, after = {}, ""
    while True:
        url = f"{OKX}?instId={coin}-USDT-SWAP&bar={BARS[interval]}&limit=100{after}"
        rows = get(url)["data"]
        if not rows:
            return closes
        for row in rows:
            closes[int(row[0])] = row[4]
        oldest = int(rows[-1][0])
        if oldest <= start:
            return closes
        after = f"&after={oldest}"
        time.sleep(0.12)


for coin in ("BTC", "ETH"):
    for interval in BARS:
        ours = hyperliquid(coin, interval)
        theirs = okx(coin, interval, min(ours))
        # The last candle on each venue is still open.
        times = sorted(set(ours) & set(theirs))[:-1]
        with open(f"study/data/{coin}_{interval}.csv", "w", newline="") as out:
            rows = csv.writer(out)
            rows.writerow(["time_ms", "hyperliquid", "okx"])
            rows.writerows([t, ours[t], theirs[t]] for t in times)
        print(coin, interval, len(times), "of", len(ours))
