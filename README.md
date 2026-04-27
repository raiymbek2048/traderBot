# TraderBot

Автоматизированная торговая система для Bybit (ETHUSDT perpetual). Две независимые стратегии + Alpha Gate фильтр сигналов.

## Архитектура

```
traderbot/
├── analyst/            # Данные: funding rate, OHLCV, OI
│   ├── fetcher.py      # BybitFetcher (ccxt + httpx fallback)
│   ├── signal.py       # Сигналы стратегии A (funding MR)
│   └── main.py         # Аналитик-демон
├── executor/           # Исполнение ордеров
│   ├── position.py     # Управление позицией
│   └── main.py         # Экзекьютор-демон
├── gate/               # Alpha Gate v2 (shadow mode)
│   ├── scorer.py       # Composite score (liq×0.5 + div×0.3 + onchain×0.2)
│   ├── main.py         # Gate-демон (shadow, пишет GateDecision в БД)
│   └── sources/
│       ├── funding_divergence.py   # Bybit vs Binance spread
│       ├── liquidation_screen.py  # Прокси ликвидаций по объёму
│       └── macro_blocker.py       # RSS-фильтр системных событий
├── momentum/           # Стратегия B: 5m momentum (paper only)
│   ├── signal.py       # EMA cross + OI delta + VWAP / BTC lead-lag
│   └── main.py         # Momentum-демон (синхрон по 5m свечам)
├── shared/
│   ├── config.py       # Config из .env
│   └── db.py           # SQLAlchemy модели (SQLite / PostgreSQL)
├── scripts/
│   ├── backtest.py             # Бэктест стратегии A (funding MR)
│   ├── momentum_backtest.py    # Бэктест стратегии B (5m momentum)
│   ├── gate_backtest.py        # Исторический бэктест Alpha Gate
│   └── gate_effectiveness.py  # Пост-анализ shadow-mode данных
├── .env                # Параметры (см. ниже)
└── traderbot.db        # SQLite база данных
```

## Стратегии

### Стратегия A — Funding Rate Mean Reversion
- **Таймфрейм:** 1h / 4h
- **Сигнал:** funding rate аномалия + RSI + SMA тренд
- **Частота:** ~7 сделок/месяц
- **Режим:** paper trading

### Стратегия B — 5m VWAP Mean Reversion ✅ Валидирована
- **Таймфрейм:** 5 минут
- **ETH branch:** цена отклоняется >0.5% от 4h rolling VWAP + volume spike + RSI(2) > 80 / < 20 → фейдим отклонение
- **BTC branch:** BTC cumulative 3-bar return > 0.20%, ETH ещё не отреагировал → входим за BTC
- **Режим:** ETH branch — ranging/transition; BTC branch — trending/transition
- **SL/TP:** 0.20% / 0.40% (R:R = 2:1), breakeven WR = 53.5%
- **Max hold:** 24 бара (2 часа)
- **Частота:** ~1/день

**Backtest результаты (90d):** WR=59.8%, total=+6.15%, Sharpe=N/A  
**Validation (180d):** WR=58.5%, total=+11.26%, Sharpe=3.23, MaxDD=-1.43% ✅

## Alpha Gate v2 (shadow mode)

Фильтр сигналов на основе 3 источников:

| Источник | Вес | Описание |
|---|---|---|
| Liquidation screen | 0.5 | Объём-прокси ликвидаций |
| Funding divergence | 0.3 | Bybit vs Binance спред > 0.01% |
| On-chain | 0.2 | Зарезервировано |

Порог: `composite_score ≥ 0.6` → approve. Shadow mode = записывает решения без блокировки входов.

Исторический бэктест (12 мес): `gate_effectiveness = +113.2%`, `win_rate_delta = +9.2%` ✅

## Запуск

### Установка
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Демоны (paper mode)
```bash
# Alpha Gate shadow mode
python -m gate.main &

# Momentum paper trading
python -m momentum.main &

# Analyst + Executor (стратегия A)
python -m analyst.main &
python -m executor.main &
```

### Бэктесты
```bash
# Momentum strategy
python scripts/momentum_backtest.py --days 90
python scripts/momentum_backtest.py --days 180 --sl 0.003 --tp 0.006

# Alpha Gate validation
python scripts/gate_backtest.py --months 12

# Post-shadow analysis (после накопления данных)
python scripts/gate_effectiveness.py
```

## Конфигурация (.env)

```env
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BYBIT_TESTNET=false

TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...

DATABASE_URL=sqlite:///traderbot.db

# Стратегия A
SYMBOL=ETHUSDT
LEVERAGE=2
RISK_PER_TRADE=0.015
FUNDING_THRESHOLD=0.0001
STOP_LOSS_PCT=0.01
TAKE_PROFIT_PCT=0.02
PAPER_TRADING=true

# Стратегия B (momentum)
MOMENTUM_ENABLED=true
MOMENTUM_SL_PCT=0.0035
MOMENTUM_TP_PCT=0.0070
MOMENTUM_BTC_THRESHOLD=0.0035
MOMENTUM_EMA_FAST=8
MOMENTUM_EMA_SLOW=21
MOMENTUM_VWAP_THRESHOLD=0.002
MOMENTUM_MAX_HOLD_BARS=24
```

## База данных

| Таблица | Описание |
|---|---|
| `signals` | Сигналы стратегии A |
| `trades` | Сделки стратегии A |
| `momentum_trades` | Сделки стратегии B (paper) |
| `gate_decisions` | Alpha Gate решения (shadow mode) |
| `funding_rates` | История funding rates |

## Текущий статус

| Компонент | PID | Статус |
|---|---|---|
| gate.main | 2277 | ✅ shadow mode |
| momentum.main | 9063 | ✅ paper trading (валидирована) |
