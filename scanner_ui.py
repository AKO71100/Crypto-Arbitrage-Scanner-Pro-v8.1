import os
import sys
import time
import threading
import sqlite3
import requests
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
from typing import Dict, Tuple, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- ЛОГИКА СКАНЕРА (Back-end) ---

PriceMap = Dict[str, Tuple[float, float]]

class ArbitrageScannerLogic:
    def __init__(self, log_callback, data_callback, status_callback):
        self.log = log_callback
        self.update_data = data_callback
        self.update_status = status_callback
        self.running = False
        self.stop_event = threading.Event()

        # Настройки (будут обновлены из UI)
        self.min_spread = 0.35
        self.max_spread = 10.0
        self.min_volume_24h = 200000
        self.top_n = 700
        self.window_min_minutes = 10
        self.window_hour_minutes = 60

        self.fees = {
            "binance": 0.05, "bybit": 0.055, "okx": 0.05,
            "gateio": 0.05, "bingx": 0.05, "hyperliquid": 0.025
        }

        self.coin_data_cache = {}
        self.last_coingecko_update = 0.0
        self.coingecko_cache_duration = 600
        self.iteration = 0
        self.session = self._make_session()
        self.bybit_base_url = "https://api.bybit.com"

        self.spread_hist = defaultdict(deque)
        self.history_path = "spread_history.sqlite"
        self.db = self._init_db()
        self._load_recent_history()

    def update_settings(self, spread, volume, top_n, window):
        self.min_spread = float(spread)
        self.min_volume_24h = float(volume)
        self.top_n = int(top_n)
        self.window_min_minutes = int(window)

    def _init_db(self):
        db = sqlite3.connect(self.history_path, check_same_thread=False)
        cur = db.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS asset_spread (
                ts INTEGER NOT NULL,
                asset TEXT NOT NULL,
                best_net REAL NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_ts ON asset_spread(asset, ts)")
        db.commit()
        return db

    def _load_recent_history(self):
        now = int(time.time())
        cutoff = now - self.window_hour_minutes * 60
        cur = self.db.cursor()
        cur.execute("DELETE FROM asset_spread WHERE ts < ?", (cutoff,))
        self.db.commit()
        cur.execute("SELECT ts, asset, best_net FROM asset_spread WHERE ts >= ? ORDER BY ts ASC", (cutoff,))
        count = 0
        for ts, asset, best_net in cur.fetchall():
            self.spread_hist[asset].append((ts, float(best_net)))
            count += 1
        self.log(f"📚 БД: Загружено {count} записей истории.")

    def _make_session(self):
        s = requests.Session()
        retry = Retry(total=2, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504))
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({"User-Agent": "CryptoScanner/8.1 UI"})
        return s

    # --- ЗАГРУЗКА ИСТОРИЧЕСКИХ СВЕЧЕЙ (Полный код из v7.1) ---

    def _fetch_klines_binance(self, symbol: str, limit: int = 60) -> List[Tuple[int, float]]:
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": f"{symbol}USDT", "interval": "1m", "limit": limit}
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200: return []
            return [(int(k[0]) // 1000, float(k[4])) for k in resp.json()]
        except: return []

    def _fetch_klines_bybit(self, symbol: str, limit: int = 60) -> List[Tuple[int, float]]:
        try:
            url = f"{self.bybit_base_url}/v5/market/kline"
            params = {"category": "linear", "symbol": f"{symbol}USDT", "interval": "1", "limit": limit}
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200: return []
            klines = [(int(k[0]) // 1000, float(k[4])) for k in resp.json().get("result", {}).get("list", [])]
            return klines[::-1] 
        except: return []

    def _fetch_klines_okx(self, symbol: str, limit: int = 60) -> List[Tuple[int, float]]:
        try:
            url = "https://www.okx.com/api/v5/market/candles"
            params = {"instId": f"{symbol}-USDT-SWAP", "bar": "1m", "limit": limit}
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200: return []
            klines = [(int(k[0]) // 1000, float(k[4])) for k in resp.json().get("data", [])]
            return klines[::-1]
        except: return []

    def _fetch_klines_gateio(self, symbol: str, limit: int = 60) -> List[Tuple[int, float]]:
        try:
            url = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
            params = {"contract": f"{symbol}_USDT", "interval": "1m", "limit": limit}
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200: return []
            return [(int(k["t"]), float(k["c"])) for k in resp.json()]
        except: return []

    def _calculate_historical_spreads(self, asset: str, long_ex: str, short_ex: str) -> bool:
        """Загрузка свечей и расчет истории для одной пары"""
        fetch_funcs = {
            "binance": self._fetch_klines_binance,
            "bybit": self._fetch_klines_bybit,
            "okx": self._fetch_klines_okx,
            "gateio": self._fetch_klines_gateio
        }

        lex, sex = long_ex.lower(), short_ex.lower()
        if lex not in fetch_funcs or sex not in fetch_funcs: return False

        k_long = fetch_funcs[lex](asset, 60)
        k_short = fetch_funcs[sex](asset, 60)

        if not k_long or not k_short: return False

        p_long = {ts: p for ts, p in k_long}
        p_short = {ts: p for ts, p in k_short}

        common = []
        for ts_l in p_long:
            for ts_s in p_short:
                if abs(ts_l - ts_s) <= 30:
                    common.append((ts_l, ts_s))
                    break

        fees = self.fees.get(lex, 0.05) + self.fees.get(sex, 0.05)
        added = False
        for tl, ts in common:
            pl, ps = p_long[tl], p_short[ts]
            if pl <= 0: continue
            gross = ((ps - pl) / pl) * 100
            net = gross - fees
            avg_ts = (tl + ts) // 2
            self.spread_hist[asset].append((avg_ts, float(net)))
            added = True

        return added

    def _preload_history_for_opportunities(self, opps):
        """Массовая загрузка истории для найденных монет"""
        tasks = []
        for o in opps:
            asset = o["Asset"]
            # Загружаем, только если истории мало (<5 записей)
            if len(self.spread_hist[asset]) < 5:
                # Извлекаем названия бирж из строки "BINANCE->BYBIT"
                parts = o["Pair"].split("->")
                if len(parts) == 2:
                    tasks.append((asset, parts[0], parts[1]))

        if not tasks: return

        self.update_status(f"📥 Загрузка истории свечей для {len(tasks)} монет...")
        self.log(f"📥 Старт загрузки истории для {len(tasks)} монет...")

        count = 0
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(self._calculate_historical_spreads, a, l, s): a for a, l, s in tasks}
            for f in as_completed(futs):
                if f.result(): count += 1

        self.log(f"✅ История загружена для {count} монет.")
        self._save_history_db()

    def _save_history_db(self):
        rows = []
        for asset, dq in self.spread_hist.items():
            for ts, net in dq:
                rows.append((ts, asset, net))
        if rows:
            cur = self.db.cursor()
            cur.execute("DELETE FROM asset_spread")
            cur.executemany("INSERT INTO asset_spread(ts, asset, best_net) VALUES (?, ?, ?)", rows)
            self.db.commit()

    # --- ОБЫЧНЫЕ МЕТОДЫ ---

    def fetch_coingecko_data(self):
        current_time = time.time()
        if self.coin_data_cache and (current_time - self.last_coingecko_update) < self.coingecko_cache_duration:
            return

        self.log(f"📊 Обновление CoinGecko (Топ-{self.top_n})...")
        try:
            pages = (self.top_n // 250) + 1
            all_coins = []
            for page in range(1, pages + 1):
                url = "https://api.coingecko.com/api/v3/coins/markets"
                params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": page}
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    all_coins.extend(resp.json())
                if self.stop_event.is_set(): return

            new_cache = {}
            for coin in all_coins:
                sym = str(coin.get("symbol", "")).upper().strip()
                if sym:
                    new_cache[sym] = {
                        "market_cap": float(coin.get("market_cap") or 0),
                        "volume_24h": float(coin.get("total_volume") or 0),
                        "rank": float(coin.get("market_cap_rank") or 9999)
                    }
            self.coin_data_cache = new_cache
            self.last_coingecko_update = current_time
            self.log(f"✅ CoinGecko: {len(new_cache)} монет.")
        except Exception as e:
            self.log(f"⚠️ Ошибка CoinGecko: {e}")

    def load_all_prices(self):
        # Полные методы загрузки цен
        def get_binance():
            r = {}
            try:
                resp = self.session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=10)
                for i in resp.json():
                    if i['symbol'].endswith('USDT'): r[i['symbol'][:-4]] = (float(i['lastPrice']), float(i['quoteVolume']))
            except: pass
            return r

        def get_bybit():
            r = {}
            try:
                resp = self.session.get("https://api.bybit.com/v5/market/tickers?category=linear", timeout=10)
                for i in resp.json()['result']['list']:
                    if i['symbol'].endswith('USDT'): r[i['symbol'][:-4]] = (float(i['lastPrice']), float(i['turnover24h']))
            except: pass
            return r

        def get_okx():
            r = {}
            try:
                resp = self.session.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP", timeout=10)
                for i in resp.json().get('data', []):
                    if '-USDT-' in i['instId']:
                        p = float(i['last'])
                        r[i['instId'].split('-')[0]] = (p, float(i['volCcy24h']) * p)
            except: pass
            return r

        def get_gateio():
            r = {}
            try:
                resp = self.session.get("https://api.gateio.ws/api/v4/futures/usdt/tickers", timeout=10)
                for i in resp.json():
                    if '_USDT' in i['contract']: r[i['contract'].replace('_USDT','')] = (float(i['last']), float(i['volume_24h_quote']))
            except: pass
            return r

        def get_bingx():
            r = {}
            try:
                resp = self.session.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker", timeout=10)
                for i in resp.json().get('data', []):
                    if i['symbol'].endswith('-USDT'):
                        p = float(i['lastPrice'])
                        r[i['symbol'].replace('-USDT','')] = (p, float(i['volume']) * p)
            except: pass
            return r

        def get_hyperliquid():
            r = {}
            try:
                resp = self.session.post("https://api.hyperliquid.xyz/info", json={"type": "metaAndAssetCtxs"}, timeout=10)
                d = resp.json()
                for i, m in enumerate(d[0]['universe']):
                    if i < len(d[1]):
                        r[m['name']] = (float(d[1][i]['markPx']), float(d[1][i]['dayNtlVlm']))
            except: pass
            return r

        results = {}
        with ThreadPoolExecutor(max_workers=6) as ex:
            fs = {
                ex.submit(get_binance): "binance", ex.submit(get_bybit): "bybit",
                ex.submit(get_okx): "okx", ex.submit(get_gateio): "gateio",
                ex.submit(get_bingx): "bingx", ex.submit(get_hyperliquid): "hyperliquid"
            }
            for f in as_completed(fs):
                results[fs[f]] = f.result()
        return results

    def _push_history(self, best_map):
        now = int(time.time())
        cutoff = now - self.window_hour_minutes * 60
        rows = []
        for asset, val in best_map.items():
            self.spread_hist[asset].append((now, val))
            while self.spread_hist[asset] and self.spread_hist[asset][0][0] < cutoff:
                self.spread_hist[asset].popleft()
            rows.append((now, asset, val))

        cur = self.db.cursor()
        cur.executemany("INSERT INTO asset_spread(ts, asset, best_net) VALUES (?, ?, ?)", rows)
        self.db.commit()

    def _get_stats(self, asset):
        dq = self.spread_hist.get(asset)
        if not dq: return ("-", "-", "-", "-")
        now = int(time.time())
        cut10 = now - self.window_min_minutes * 60
        vals10 = [v for ts, v in dq if ts >= cut10]
        vals60 = [v for ts, v in dq]

        if not vals10: return ("-", "-", "-", "-")
        mn, mx = min(vals10), max(vals10)
        hit = sum(1 for v in vals60 if v >= self.min_spread)
        return (f"{mn:.2f}", f"{mx:.2f}", f"{mx-mn:.2f}", str(hit))

    def run_cycle(self):
        self.running = True
        first_run = True

        while not self.stop_event.is_set():
            try:
                self.iteration += 1
                self.update_status(f"Итерация #{self.iteration}: Загрузка цен...")

                if not self.coin_data_cache:
                    self.fetch_coingecko_data()

                prices = self.load_all_prices()
                if self.stop_event.is_set(): break

                self.update_status(f"Итерация #{self.iteration}: Поиск спредов...")

                best_map = {}
                display_list = []

                exchanges = list(prices.keys())
                for i, ex1 in enumerate(exchanges):
                    for ex2 in exchanges[i+1:]:
                        common = set(prices[ex1].keys()) & set(prices[ex2].keys())
                        for asset in common:
                            if asset not in self.coin_data_cache: continue

                            p1, v1 = prices[ex1].get(asset, (0,0))
                            p2, v2 = prices[ex2].get(asset, (0,0))
                            if p1 <=0 or p2 <=0: continue

                            if p1 < p2:
                                long_ex, short_ex, lp, sp = ex1, ex2, p1, p2
                            else:
                                long_ex, short_ex, lp, sp = ex2, ex1, p2, p1

                            vol = min(v1, v2)
                            gross = ((sp - lp) / lp) * 100
                            fees = self.fees.get(long_ex, 0.05) + self.fees.get(short_ex, 0.05)
                            net = gross - fees

                            if gross > self.max_spread: continue
                            if vol < self.min_volume_24h: continue

                            # Для истории запоминаем лучший спред монеты
                            if asset not in best_map or net > best_map[asset]:
                                best_map[asset] = net

                            if net < self.min_spread: continue

                            # Rating
                            score = 0
                            if net > 0.5: score += 30
                            if vol > 500000: score += 20
                            rating = "🟢 SUPER" if score >= 50 else ("🟡 GOOD" if score >= 30 else "🔴 RISKY")

                            # Собираем данные для таблицы
                            display_list.append({
                                "Asset": asset,
                                "Pair": f"{long_ex.upper()}->{short_ex.upper()}",
                                "Net": f"{net:.2f}%",
                                "Price": f"{lp:.4f}",
                                "Vol": f"{vol/1000:.0f}K",
                                "Rating": rating,
                                "raw_net": net 
                            })

                self._push_history(best_map)

                # --- ПРЕДЗАГРУЗКА ИСТОРИИ (ТОЛЬКО НА 1-й ИТЕРАЦИИ) ---
                if first_run and display_list:
                    self._preload_history_for_opportunities(display_list)
                    first_run = False

                # Теперь, когда история обновлена (или загружена), заполняем статистику
                final_ui_list = []
                display_list.sort(key=lambda x: x['raw_net'], reverse=True)

                # Фильтр дублей (берем лучший для монеты) + топ-100
                seen_assets = set()
                for item in display_list:
                    if item["Asset"] in seen_assets: continue
                    seen_assets.add(item["Asset"])

                    st_min, st_max, st_delta, st_hit = self._get_stats(item["Asset"])
                    item["Min10"] = st_min
                    item["Max10"] = st_max
                    item["Delta"] = st_delta
                    item["Hit1h"] = st_hit
                    final_ui_list.append(item)
                    if len(final_ui_list) >= 100: break

                self.update_data(final_ui_list)
                self.update_status(f"Ожидание 30 сек... (Найдено: {len(display_list)})")

                for _ in range(30):
                    if self.stop_event.is_set(): break
                    time.sleep(1)

            except Exception as e:
                self.log(f"Ошибка цикла: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)

# --- ГРАФИЧЕСКИЙ ИНТЕРФЕЙС (Front-end) ---

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Arbitrage Scanner Pro v8.1 (History Included)")
        self.root.geometry("1100x700")

        style = ttk.Style()
        style.theme_use('clam')

        # TOP PANEL
        top_frame = ttk.LabelFrame(root, text="Настройки сканера", padding=10)
        top_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(top_frame, text="Min Spread (%):").grid(row=0, column=0, padx=5)
        self.ent_spread = ttk.Entry(top_frame, width=8)
        self.ent_spread.insert(0, "0.35")
        self.ent_spread.grid(row=0, column=1, padx=5)

        ttk.Label(top_frame, text="Min Vol ($):").grid(row=0, column=2, padx=5)
        self.ent_vol = ttk.Entry(top_frame, width=10)
        self.ent_vol.insert(0, "200000")
        self.ent_vol.grid(row=0, column=3, padx=5)

        ttk.Label(top_frame, text="Top Coins:").grid(row=0, column=4, padx=5)
        self.ent_top = ttk.Entry(top_frame, width=8)
        self.ent_top.insert(0, "700")
        self.ent_top.grid(row=0, column=5, padx=5)

        ttk.Label(top_frame, text="Hist Window (min):").grid(row=0, column=6, padx=5)
        self.ent_win = ttk.Entry(top_frame, width=5)
        self.ent_win.insert(0, "10")
        self.ent_win.grid(row=0, column=7, padx=5)

        self.btn_start = ttk.Button(top_frame, text="🚀 ЗАПУСК", command=self.start_scan)
        self.btn_start.grid(row=0, column=8, padx=20)

        self.btn_stop = ttk.Button(top_frame, text="🛑 СТОП", command=self.stop_scan, state="disabled")
        self.btn_stop.grid(row=0, column=9, padx=5)

        self.lbl_status = ttk.Label(top_frame, text="Готов к работе", foreground="blue")
        self.lbl_status.grid(row=1, column=0, columnspan=10, sticky="w", pady=5)

        # MIDDLE PANEL (Table)
        mid_frame = ttk.Frame(root)
        mid_frame.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("Asset", "Pair", "Net", "Price", "Vol", "Min10", "Max10", "Delta", "Hit1h", "Rating")
        self.tree = ttk.Treeview(mid_frame, columns=cols, show="headings", selectmode="browse")

        widths = [80, 150, 80, 100, 100, 80, 80, 80, 80, 100]
        for col, w in zip(cols, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor="center")

        vsb = ttk.Scrollbar(mid_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # BOTTOM PANEL (Logs)
        bot_frame = ttk.LabelFrame(root, text="Лог событий", padding=5)
        bot_frame.pack(fill="x", padx=10, pady=5, side="bottom")

        self.log_area = scrolledtext.ScrolledText(bot_frame, height=6, state="disabled", font=("Consolas", 9))
        self.log_area.pack(fill="both", expand=True)

        self.logic_thread = None
        self.scanner = ArbitrageScannerLogic(self.log_msg, self.update_table, self.set_status)

    def log_msg(self, msg):
        self.root.after(0, lambda: self._append_log(msg))

    def _append_log(self, msg):
        self.log_area.configure(state="normal")
        t = datetime.now().strftime("%H:%M:%S")
        self.log_area.insert("end", f"[{t}] {msg}\n")
        self.log_area.see("end")
        self.log_area.configure(state="disabled")

    def set_status(self, msg):
        self.root.after(0, lambda: self.lbl_status.config(text=msg))

    def update_table(self, data):
        self.root.after(0, lambda: self._refresh_tree(data))

    def _refresh_tree(self, data):
        for row in self.tree.get_children():
            self.tree.delete(row)
        for item in data:
            vals = (item["Asset"], item["Pair"], item["Net"], item["Price"], 
                    item["Vol"], item["Min10"], item["Max10"], item["Delta"], 
                    item["Hit1h"], item["Rating"])

            tag = "normal"
            if "SUPER" in item["Rating"]: tag = "super"
            elif "RISKY" in item["Rating"]: tag = "risky"

            self.tree.insert("", "end", values=vals, tags=(tag,))

        self.tree.tag_configure("super", background="#d1ffc4")
        self.tree.tag_configure("risky", background="#ffcccc")

    def start_scan(self):
        try:
            self.scanner.update_settings(
                self.ent_spread.get(), self.ent_vol.get(),
                self.ent_top.get(), self.ent_win.get()
            )
            self.scanner.stop_event.clear()
            self.logic_thread = threading.Thread(target=self.scanner.run_cycle, daemon=True)
            self.logic_thread.start()

            self.btn_start.config(state="disabled")
            self.btn_stop.config(state="normal")
            self.log_msg("🚀 Сканер запущен.")
        except ValueError:
            messagebox.showerror("Ошибка", "Проверьте числа в настройках!")

    def stop_scan(self):
        if self.scanner:
            self.scanner.stop_event.set()
            self.set_status("Остановка...")
            self.btn_start.config(state="normal")
            self.btn_stop.config(state="disabled")
            self.log_msg("🛑 Стоп.")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
