"""
Polymarket BTC 5分钟量化 - 终极稳定修复版 V80.3
修复内容：
- 彻底解决 "ValueError: Invalid format specifier"（格式化 % 和对齐问题）
- 加强变量空值保护和类型安全
- 保留 V80.2 所有网络重试、智能风控、最强日志功能
"""

import os, asyncio, time, json, requests, threading, websocket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

# ================== 1. 配置与风控 ==================
PAPER_TRADING_MODE = True    
LOG_FILE_NAME = "trade_log-004.txt"

EV_THRESHOLD = 0.085          
WIN_PROB_THRESHOLD = 0.55     
MIN_PRICE_DIFF = 25.0         

TIME_WINDOW_MIN = 60          
TIME_WINDOW_MAX = 285        

PRICE_BREAK_THRESHOLD = 30.0 

KELLY_FRACTION = 0.4          
GLOBAL_RISK_SCALE = 0.05      
MAX_TRADE_SIZE = 10.0        
MIN_TRADE_SIZE = 1.0         

TAKE_PROFIT_PCT = 0.25        
BREAK_EVEN_TRIGGER = 0.10     
TRAILING_TRIGGER = 0.18       
TRAILING_RETRACE = 0.03      
HARD_STOP_LOSS = -0.20        
TREND_REVERSE_DIFF = 25.0     

MAX_DRAWDOWN_PCT = 0.08       
VOLATILITY_DISCOUNT_FACTOR = 0.3   
MAX_COOL_DOWN_SEC = 180       

# ================== 2. 增强型 requests Session ==================
def create_gamma_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "Polymarket-Bot-V80.3"})
    return session

gamma_session = create_gamma_session()

class TradeStats:
    def __init__(self, initial_balance):
        self.total_trades = 0
        self.wins = 0
        self.total_pnl_usd = 0.0
        self.balance = initial_balance
        self.start_balance = initial_balance
        self.start_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.max_balance = initial_balance
        self.trade_history = []

    def update(self, pnl_usd, side, entry_p, exit_p, size, pnl_pct, reason):
        self.total_trades += 1
        self.total_pnl_usd += pnl_usd
        self.balance += pnl_usd
        if self.balance > self.max_balance:
            self.max_balance = self.balance
        if pnl_usd > 0: 
            self.wins += 1
        
        detail_msg = (f"📝 成交确认 | {side} | 入场:{entry_p:.3f} → 出场:{exit_p:.3f} | "
                      f"仓位:{size:.1f} | 盈亏:${pnl_usd:+.2f} ({pnl_pct:+.2%}) | 原因:{reason}")
        
        self.trade_history.append({
            "time": time.strftime('%H:%M:%S'),
            "side": side,
            "entry": entry_p,
            "exit": exit_p,
            "size": size,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "reason": reason
        })
        
        print("\n" + detail_msg)
        write_to_log(detail_msg)

    def get_drawdown(self):
        if self.max_balance == 0: return 0
        return (self.max_balance - self.balance) / self.max_balance

    def get_summary(self):
        wr = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
        dd = self.get_drawdown() * 100
        return (f"📊 统计 | 开单数: {self.total_trades} | 胜率: {wr:.1f}% | "
                f"盈亏: ${self.total_pnl_usd:+.2f} | 余额: ${self.balance:.2f} | 回撤: {dd:.1f}%")

stats = TradeStats(1000.0)

def write_to_log(message):
    timestamp = time.strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] {message}"
    print(formatted_msg)
    with open(LOG_FILE_NAME, "a", encoding="utf-8") as f:
        f.write(formatted_msg + "\n")

# ================== 3. 行情获取逻辑 ==================
current_chainlink_price = None
live_prob_up = 0.5 
target_up_asset_id = None
price_to_beat_dict = {}
recent_diffs = []          
MAX_RECENT_DIFFS = 6

def on_price_msg(ws, message):
    global current_chainlink_price
    try:
        data = json.loads(message)
        payload = data.get("payload", {})
        if data.get("topic") == "crypto_prices_chainlink" and payload.get("symbol") == "btc/usd":
            current_chainlink_price = float(payload.get("value"))
    except: pass

def on_market_msg(ws, message):
    global live_prob_up, target_up_asset_id
    try:
        data = json.loads(message)
        if data.get("event_type") == "last_trade_price" and data.get("asset_id") == target_up_asset_id:
            live_prob_up = float(data["price"])
    except: pass

def run_price_ws():
    while True:
        try:
            ws = websocket.WebSocketApp("wss://ws-live-data.polymarket.com",
                on_open=lambda ws: ws.send(json.dumps({"action":"subscribe","subscriptions":[{"topic":"crypto_prices_chainlink","type":"*","filters":'{"symbol":"btc/usd"}'}]})),
                on_message=on_price_msg)
            ws.run_forever(ping_interval=10, ping_timeout=5)
        except: pass
        time.sleep(5)

def run_market_ws(tids):
    global target_up_asset_id
    target_up_asset_id = tids[0]
    while True:
        try:
            ws = websocket.WebSocketApp("wss://ws-subscriptions-clob.polymarket.com/ws/market",
                on_open=lambda ws: ws.send(json.dumps({"assets_ids": tids, "type": "market"})),
                on_message=on_market_msg)
            ws.run_forever(ping_interval=10, ping_timeout=5)
        except: pass
        time.sleep(5)

async def execute_trade(tid, price, size, side):
    return True, max(0.01, min(0.99, price))

def safe_get_market(slug):
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    for attempt in range(6):
        try:
            r = gamma_session.get(url, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 5:
                write_to_log(f"❌ gamma-api 最终失败 ({slug}): {type(e).__name__}")
                return None
            wait = (2 ** attempt) * 0.6
            write_to_log(f"⚠️ gamma-api 重试 {attempt+1}/6 ({slug}) | 等待 {wait:.1f}s")
            time.sleep(wait)
    return None

# ================== 4. 主交易引擎 V80.3 ===================
async def main():
    global current_chainlink_price, live_prob_up, recent_diffs
    current_slug = position = None
    is_first_period = True 
    last_file_log_time = 0
    cooldown_until = 0
    last_exit_price_diff = 0 

    threading.Thread(target=run_price_ws, daemon=True).start()
    while current_chainlink_price is None: 
        await asyncio.sleep(1)

    write_to_log(f"🚀 系统启动 | V80.3 修复版（格式化错误已修复） | 初始余额: ${stats.balance}")

    while True:
        try:
            now = time.time()
            ts = (int(now) // 300) * 300
            slug = f"btc-updown-5m-{ts}"
            rem = max(0, (ts + 300) - now)

            if slug != current_slug:
                write_to_log(f"🔄 尝试周期切换: {slug}")
                data = safe_get_market(slug)
                if data:
                    tids = json.loads(data.get('clobTokenIds', '[]'))
                    if len(tids) >= 2:
                        is_first_period = False if current_slug is not None else True
                        current_slug, up_tid, down_tid = slug, tids[0], tids[1]
                        price_to_beat_dict[slug] = round(current_chainlink_price or 0, 2)
                        threading.Thread(target=run_market_ws, args=([up_tid, down_tid],), daemon=True).start()
                        cooldown_until = 0 
                        recent_diffs.clear()
                        write_to_log(f"🔔 周期切换成功: {slug} | 基准价: {price_to_beat_dict[slug]}")
                        position = None
                await asyncio.sleep(2)
                continue

            p2b = price_to_beat_dict.get(current_slug, 0)
            diff = (current_chainlink_price - p2b) if current_chainlink_price is not None else 0.0
            
            # 波动率计算
            recent_diffs.append(diff)
            if len(recent_diffs) > MAX_RECENT_DIFFS:
                recent_diffs.pop(0)
            
            volatility = 1.0
            if len(recent_diffs) >= 3:
                mean_diff = sum(recent_diffs) / len(recent_diffs)
                variance = sum((d - mean_diff)**2 for d in recent_diffs) / len(recent_diffs)
                volatility = max(0.5, min(3.0, (variance ** 0.5) / 8 + 1))

            base_win_prob = 0.5 + (diff * 0.0045)
            discount = 1 - VOLATILITY_DISCOUNT_FACTOR * max(0, volatility - 1.2)
            win_prob = max(0.01, min(0.99, base_win_prob * discount))

            ev_up = (win_prob * (1 - live_prob_up)) - ((1 - win_prob) * live_prob_up)
            ev_down = ((1 - win_prob) * live_prob_up) - (win_prob * (1 - live_prob_up))

            # 全局回撤保护
            if stats.get_drawdown() > MAX_DRAWDOWN_PCT:
                if position is None:
                    msg = f"⛔ 全局最大回撤保护触发！当前回撤: {stats.get_drawdown()*100:.1f}% > 8%，系统暂停！"
                    print("\n" + msg)
                    write_to_log(msg)
                    await asyncio.sleep(60)
                    continue

            # 持仓管理
            if position:
                side = position['side']
                curr_p = live_prob_up if side == "UP" else (1 - live_prob_up)
                entry_p = position['entry']
                pnl_pct = (curr_p - entry_p) / entry_p if entry_p != 0 else 0.0
                pnl_usd = (curr_p - entry_p) * position['size']
                if curr_p > position.get('max_p', entry_p):
                    position['max_p'] = curr_p

                exit_reason = None
                is_profit_exit = False 

                if pnl_pct <= HARD_STOP_LOSS: 
                    exit_reason = "🛑 硬止损割肉"
                elif (position.get('max_p', entry_p) - entry_p) / entry_p > BREAK_EVEN_TRIGGER and pnl_pct < 0.01: 
                    exit_reason = "🛡️ 保本平仓"
                elif side == "UP" and diff < (position['entry_diff'] - TREND_REVERSE_DIFF):
                    exit_reason = f"❌ 趋势反转(Δ反向>{TREND_REVERSE_DIFF})"
                elif side == "DOWN" and diff > (position['entry_diff'] + TREND_REVERSE_DIFF):
                    exit_reason = f"❌ 趋势反转(Δ反向>{TREND_REVERSE_DIFF})"
                elif pnl_pct >= TAKE_PROFIT_PCT: 
                    exit_reason, is_profit_exit = "🎯 目标止盈", True
                elif (position.get('max_p', entry_p) - entry_p) / entry_p > TRAILING_TRIGGER and (position.get('max_p', entry_p) - curr_p) / entry_p > TRAILING_RETRACE: 
                    exit_reason, is_profit_exit = "📉 追踪锁利", True
                elif rem < 10: 
                    exit_reason = "⌛ 结算强制离场"

                if exit_reason:
                    ok, _ = await execute_trade(position['tid'], curr_p - 0.01 if curr_p > 0.01 else curr_p, position['size'], SELL)
                    if ok:
                        stats.update(pnl_usd, side, entry_p, curr_p, position['size'], pnl_pct, exit_reason)
                        
                        if not is_profit_exit:
                            loss_amount = abs(pnl_usd)
                            cool_sec = min(MAX_COOL_DOWN_SEC, 30 + int(loss_amount * 15))
                            cooldown_until = now + cool_sec
                            last_exit_price_diff = diff
                            cool_msg = f" (自适应冷静期: {cool_sec}s | 亏损:${pnl_usd:.2f})"
                        else:
                            cooldown_until = 0
                            cool_msg = " (盈利平仓: 无冷却)"
                        
                        print(f"✨ 结算完成 | {stats.get_summary()}{cool_msg}")
                        write_to_log(f"结算完成 | {stats.get_summary()}{cool_msg}")
                        position = None
                        await asyncio.sleep(1)
                        continue
                
                # 修复后的安全格式化
                pnl_str = f"{pnl_pct:+.2%}"
                current_status = (f"📈 {side} | 盈亏:{pnl_str}(${pnl_usd:+.2f}) | "
                                  f"进场Δ:{position.get('entry_diff', 0):+.2f} | 现Δ:{diff:+.2f} | "
                                  f"Vol:{volatility:.2f} | 剩:{rem:3.0f}s")
            
            # 开单逻辑
            else:
                prefix = "📡 监控"
                in_cooldown = now < cooldown_until
                price_break = in_cooldown and abs(diff - last_exit_price_diff) > PRICE_BREAK_THRESHOLD
                
                if not is_first_period and TIME_WINDOW_MIN < rem < TIME_WINDOW_MAX:
                    if abs(diff) < MIN_PRICE_DIFF:
                        prefix = "🌊 震荡过滤"
                    elif in_cooldown and not price_break:
                        prefix = f"💤 冷静({int(cooldown_until - now)}s)"
                    else:
                        side = None
                        if win_prob >= WIN_PROB_THRESHOLD and ev_up > EV_THRESHOLD:
                            side, tid, p, ev = "UP", up_tid, live_prob_up, ev_up
                        elif (1 - win_prob) >= WIN_PROB_THRESHOLD and ev_down > EV_THRESHOLD:
                            side, tid, p, ev = "DOWN", down_tid, (1 - live_prob_up), ev_down

                        if side:
                            if price_break: write_to_log(f"⚡ 波动激活! Δ变动: {diff - last_exit_price_diff:+.2f}")
                            b = (1 - p) / p if p > 0 else 0
                            kelly_f = (ev / b if b > 0 else 0) * KELLY_FRACTION
                            final_size = min((stats.balance * kelly_f * GLOBAL_RISK_SCALE) / max(p, 0.01), MAX_TRADE_SIZE)
                            
                            if final_size >= MIN_TRADE_SIZE:
                                ok, entry_p = await execute_trade(tid, min(0.99, max(0.01, p + 0.01)), final_size, BUY)
                                if ok:
                                    position = {'side':side, 'tid':tid, 'entry':entry_p, 'max_p':entry_p, 'size':final_size, 'entry_diff':diff}
                                    
                                    hard_stop_pnl = final_size * HARD_STOP_LOSS
                                    expected_pnl = final_size * ev
                                    
                                    print(f"\n🚀 开单 | {side} | 数量:{final_size:.1f} | Δ:{diff:.2f} | Vol:{volatility:.2f} | EV:{ev:.3f} | "
                                          f"硬止损PNL:${hard_stop_pnl:+.2f} | 预期EV_PNL:${expected_pnl:+.2f}")
                                    write_to_log(f"🚀 开单 | {side} | Size:{final_size:.1f} | EntryDiff:{diff:.2f} | Vol:{volatility:.2f} | "
                                                 f"EV:{ev:.3f} | 硬止损PNL:${hard_stop_pnl:+.2f} | 预期EV_PNL:${expected_pnl:+.2f}")

                if is_first_period: prefix = "🛠️ 观察"
                current_status = (f"{prefix} BTC:{current_chainlink_price or 0:.2f} | Δ:{diff:+.2f} | "
                                  f"Vol:{volatility:.2f} | WinP:{win_prob:.2f} | EV_U:{ev_up:+.3f} | EV_D:{ev_down:+.3f} | 剩:{rem:3.0f}s")

            if now - last_file_log_time >= 2:
                write_to_log(current_status)
                last_file_log_time = now

            await asyncio.sleep(1)
        except Exception as e:
            write_to_log(f"⚠️ 主循环异常: {type(e).__name__} - {str(e)[:150]}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
