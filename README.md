# TraderBot

Трёхмесячное исследование: существует ли устойчивый край в розничном
крипто-алготрейдинге на публичных сигналах. **Проверено 8 гипотез, 7 закрыты
количественно, 1 в тесте. Реальными деньгами не рискнули ни разу.**

📖 **Начни отсюда:**
- [`PROJECT_JOURNEY.md`](PROJECT_JOURNEY.md) — полная история: 19 фаз, 28 багов,
  18 уроков, все цифры
- [`LOOPHOLE_SEARCH.md`](LOOPHOLE_SEARCH.md) — поиск лазейки: репрайсинг журналов
  под непроверенные режимы исполнения
- [`STRATEGY_NEXT.md`](STRATEGY_NEXT.md) — что делать дальше и почему

---

## Главный результат

```
Спот-арбитраж мажоры:  край 0.04-0.08%  vs комиссии 0.20%     ❌
Спот-арбитраж альты:   $2-7/день, ребаланс съедает            ❌
Funding spot+perp:     0 входов за 2 недели при пороге 0.20%   ❌
Перп-перп spread:      146 сделок / −$30.59                    ❌
Liq-momentum:          36 сделок / −$0.74, режимно-зависим     ❌
Momentum 5m:           0 сделок за 3 недели                    ❌
Cross-sectional carry: фандинг +$110, basis −$175              ❌
Buy&hold фандинга:     обвал ×30 out-of-sample                 ❌
Перп-перп + мин.холд:  запущен 27.07, критерии заранее         🧪
```

**Край всегда примерно равен комиссии.** Два независимых замера на своих журналах:

| Стратегия | taker | 0% / maker |
|---|:---:|:---:|
| liq-momentum (36 сделок) | −$0.74 | **+$1.24** |
| перп-перп (146 сделок) | −$31.19 | −$20.97 |

Сигнал реален, он целиком уходит бирже. Отсюда вывод: атаковать структуру
комиссий, а не искать новый индикатор.

---

## Активные сервисы (AWS Lightsail Tokyo, systemd)

| Сервис | Модуль | Роль |
|---|---|---|
| `holdtest` | `funding.hold_paper` | 🧪 Перп-перп с мин.холдом до начисления. Критерии успеха зафиксированы в докстринге **до** первой сделки |
| `publicalerts` | `alerts.public` | Канал [@crypto_liq_radar](https://t.me/crypto_liq_radar): каскады ≥$100k, фандинг каждые 4ч, дневная сводка |
| `liqrec` | `arbitrage.liq_recorder` | Рекордер ликвидаций Bybit WS → 84k+ событий |
| `funding` | `funding.run` | Скан 619 перпов, spot+perp delta-neutral, порог 0.20% |

**Остановлены** (доказали неработоспособность): `liqmom`, `momentum`,
`spreadarb`, `fundspread`, `spreadpaper`.

---

## Структура

```
traderbot/
├── funding/
│   ├── hold_paper.py    🧪 мин.холд до начисления (единственный активный тест)
│   ├── executor.py         BybitClient, delta-neutral spot+perp, settled-accrual
│   ├── monitor.py          скан всех перпов, анти-churn стрики
│   ├── run.py              лончер funding
│   ├── spread_scan.py      сканер спреда фандинга Bybit vs Binance
│   └── spread_paper.py     ⛔ A/B-решётка 11 вариантов (закрыта, журнал испорчен)
├── alerts/
│   └── public.py         📡 публичный канал алертов
├── arbitrage/
│   ├── liq_recorder.py     рекордер ликвидаций (heartbeat WS)
│   ├── liq_momentum.py  ⛔ follow-momentum после каскадов (закрыт)
│   ├── monitor.py       ⛔ спот-арбитраж VWAP по глубине (закрыт)
│   ├── executor.py         атомарное исполнение + unwind голой ноги
│   ├── scan.py             скан альтов на спреды
│   └── validate.py         депт-валидатор кандидатов
├── scripts/
│   ├── loophole_analysis.py  🔍 репрайсинг журналов под maker/hold
│   ├── hold_validation.py    🔍 hold-модель на 2 мес settled-истории
│   ├── carry_backtest.py     cross-sectional carry (вердикт: −$102)
│   ├── funding_persistence.py  эпизодная модель, пороги из данных
│   ├── liq_cascade_analysis.py  659 каскадов, опровержение отскока
│   └── liq_impulse_analysis.py  sub-second, задержка входа
├── shared/
│   ├── db.py               SQLAlchemy модели + ALTER-миграции
│   ├── config.py           Config из .env
│   └── utils.py
└── traderbot.db            SQLite
```

⛔ = закрытая стратегия, код оставлен как история
🔍 = аналитический скрипт
🧪 = активный тест

**Мёртвый код** (майские модули, не используются ни одним сервисом):
`analyst/`, `bot/`, `executor/`, `risk_manager/`, `gate/`, `momentum/`.

---

## Запуск

```bash
python -m venv .venv && source .venv/bin/activate
pip install websockets loguru sqlalchemy python-dotenv numpy ccxt httpx

# активный тест
python -m funding.hold_paper

# публичные алерты (нужен PUBLIC_CHANNEL_ID)
python -m alerts.public

# рекордер данных
python -m arbitrage.liq_recorder
```

### Анализ на собранных данных
```bash
python scripts/loophole_analysis.py   # репрайсинг журналов
python scripts/hold_validation.py     # out-of-sample hold-модель
python scripts/carry_backtest.py      # cross-sectional carry
```

---

## Конфигурация (.env)

```env
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BINANCE_API_KEY=...
BINANCE_API_SECRET=...

TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...          # приватный чат
PUBLIC_CHANNEL_ID=@channel    # публичный канал алертов

DATABASE_URL=sqlite:///traderbot.db
PAPER_TRADING=true            # ⚠️ всё в paper, реальная торговля не включалась

HOLD_SIZE_USDT=50
```

---

## База данных

| Таблица | Описание |
|---|---|
| `hold_positions` | 🧪 Тест мин.холда. Чистая: один вариант, PnL в двух режимах комиссий |
| `liq_events` | 84k+ ликвидаций Bybit, $286M |
| `funding_spread_snaps` | 78k+ снимков спреда фандинга (топ-20 за цикл) |
| `funding_positions` | Spot+perp позиции с basis/fees/settled-фандингом |
| `spread_positions` | ⚠️ A/B-журнал перп-перп — **испорчен размножением ×2.5**, дедуплицировать перед выводами |
| `maker_fill_probes` | 🧪 Исполнимость лимитников по реальной ленте (joint-fill, adverse selection) |

### ⚠️ Известные ловушки в данных

**1. Один тикер ≠ один актив.** Из 587 общих перпов Bybit/Binance **4 — разные
токены**: `ONUSDT` $88.20 vs $0.177, `SNTUSDT` 257%, `WAVESUSDT` 214%,
`VINEUSDT` 95%. В `funding_spread_snaps` лежат **2554 мусорных снимка** этих
тикеров (гэп до 62830%), записанных до фикса. Фильтруй по расхождению цен
или по списку символов. В `spread_positions` их нет — журнал сделок чист.

**2. `spread_positions` размножен A/B-решёткой ×2.5.** 11 вариантов торговали
одну возможность параллельно. Дедуплицируй по `(symbol, opened_at ±30 мин)`,
иначе одна удачная сделка считается 7 раз.
| `liq_momentum_trades` | 36 сделок follow-momentum |
| `arb_paper_trades` | Спот-арбитраж (realism v2: VWAP, fillable) |

---

## Ключевые уроки метода

Полный список (18) в [`PROJECT_JOURNEY.md`](PROJECT_JOURNEY.md). Три самых дорогих:

1. **Задержка искажает сами данные.** Медленный сервер (500-1100ms) «видел»
   спреды, которых не было — все `+$1731/день` были артефактом измерения.
   Инфраструктура — часть измерительного прибора.

2. **A/B-решётка портит ретро-анализ подвыборок.** 11 вариантов на одном потоке
   пишут одну возможность как 2.5 записи. Любая нарезка журнала обманывает:
   правило дало +$0.402 на 34 записях и −$1.42 на 14 уникальных.

3. **Слишком хорошо = ловушка.** Подтверждено четырежды: висящий спред 5%
   (закрытый вывод), фандинг −48%/д (пре-делистинг), спред 8%+/д (basis −0.5%),
   buy&hold +334%/год (токенизированные акции без спота).
