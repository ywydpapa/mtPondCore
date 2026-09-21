# ============================================================
# mtpond.py - 메인 실행 엔진 및 루프 컨트롤러
# ============================================================
import os
import asyncio
import time
from decimal import Decimal

# funchub 모듈에서 함수, 클래스 및 설정 임포트
import funchub as fh
from funchub import (
    init_config, apply_dynamic_config, get_keys, get_excluded_markets,
    PositionState, ResetRequested, fetch_upbit_accounts, load_tradable_markets,
    build_market_list_from_accounts, safe_fetch_current_prices, place_preplaced_hard_tp,
    restart_reseed_after_cancellation, align_to_half_minute, dynamic_sleep,
    manage_full_limit_sells, manage_passive_limit_sells, enrich_accounts_with_prices,
    get_available_krw, get_active_markets, get_order, get_preplace_portion,
    cancel_order, decide_sell, safe_calc_volume, cancel_all_open_orders_for_market,
    wait_unlock_and_get_balance, get_orderbook_top, order_market_sell,
    martin_guard_or_skip, order_market_buy_price, after_market_buy_place_pre_tp,
    process_intersection_buys, fetch_mtpond_setup
)

_last_cfg_signature = None

async def monitor_positions(user_no: int, server_no: int):
    start_ts = time.time()
    print(f"[INFO] ENABLE_MARTIN={fh.ENABLE_MARTIN} ENABLE_STOP={fh.ENABLE_STOP}")
    if not fh.ENABLE_MARTIN:
        print(f"[INFO] 마틴 비활성화: 일반 매수만 수행, 누적 투자금은 MAX_TOTAL_INVEST_PER_MARKET({fh.MAX_TOTAL_INVEST_PER_MARKET})까지만 허용")
    if not fh.ENABLE_STOP:
        print("[INFO] 자동 손절 비활성화: STOP/Loss-Cut 로직 전부 미작동")

    keys = await get_keys(user_no, server_no)
    if not keys:
        print("[ERR] API 키 없음 → 종료")
        return
    access_key, secret_key = keys

    try:
        excl = await get_excluded_markets()
        if excl:
            print(f"[INFO] 제외 목록 로드 완료: {sorted(excl)}")
    except Exception as e:
        print(f"[EXC][WARN] 제외 목록 로드 실패: {e}")

    class _PS(PositionState):
        def ensure_stop_fields(self, market: str):
            st = self.data.setdefault(market, {})
            st.setdefault("stop_mode_active", False)
            st.setdefault("martingale_count", 0)
            st.setdefault("last_martin_ts", None)
            return st

    ps = _PS()

    print("=== 모니터 시작 ===")
    if fh.EXCLUDED_MARKETS:
        print(f"[INFO] 제외 목록: {sorted(fh.EXCLUDED_MARKETS)}")

    if fh.FULL_LIMIT_SELL_ENABLED:
        print("[INFO] FULL_LIMIT_SELL_ENABLED=1 → 기존 TP/Trail/Stop 로직 비활성, 단일 전체 지정가 유지 모드")
        print(f"[INFO] MODE={fh.FULL_LIMIT_SELL_MODE} BASIS={fh.FULL_LIMIT_SELL_PRICE_BASIS} PCT={fh.FULL_LIMIT_SELL_PERCENT} FIXED={fh.FULL_LIMIT_SELL_FIXED_PRICE}")
    else:
        if fh.ENABLE_NEW_STOP:
            print(f"[INFO] NEW_STOP 활성화 trigger={fh.STOP_TRIGGER_PNL}% first_portion={fh.STOP_LOSS_FIRST_PORTION*100:.1f}% minRemain={fh.STOP_LOSS_MIN_REMAIN_KRW}")
        elif fh.ENABLE_STOP_TRAIL:
            print(f"[INFO] (LEGACY STOP) trigger={fh.STOP_TRIGGER_PNL}% simple={fh.STOP_SIMPLE_MODE}")
        print(f"[INFO] MAX_ACTIVE_MARKETS={fh.MAX_ACTIVE_MARKETS} ALLOW_ADDITIONAL_BUY_WHEN_FULL={fh.ALLOW_ADDITIONAL_BUY_WHEN_FULL}")
        print(f"[INFO] PREPLACE_HARD_TP={fh.PREPLACE_HARD_TP} PREPLACE_TP_TIMEOUT_SEC={fh.PREPLACE_TP_TIMEOUT_SEC} PREPLACE_ON_START={fh.PREPLACE_ON_START}")

    try:
        init_accounts = await fetch_upbit_accounts(access_key, secret_key)
        tradable = await load_tradable_markets()
        raw_markets = build_market_list_from_accounts(init_accounts, fh.BASE_UNIT)
        tradable_markets = [m for m in raw_markets if m in tradable]
        orphan = [m for m in raw_markets if m not in tradable]
        if orphan:
            print(f"[INIT] 거래지원종료(orphan) 제외: {orphan}")
        init_prices = await safe_fetch_current_prices(tradable_markets)

        init_enriched = []
        for acc in init_accounts:
            c = acc.get("currency")
            u = acc.get("unit_currency")
            if c and u == fh.BASE_UNIT and c != fh.BASE_UNIT:
                market = f"{fh.BASE_UNIT}-{c}"
                price = init_prices.get(market)
                if price is None:
                    continue
                avg_raw = acc.get("avg_buy_price")
                try:
                    avg = Decimal(str(avg_raw)) if avg_raw not in (None, "", "0") else Decimal("0")
                except:
                    avg = Decimal("0")
                try:
                    bal = Decimal(str(acc.get("balance", "0")))
                except:
                    bal = Decimal("0")
                if avg > 0 and bal > 0:
                    pnl = ((price - avg) / avg * Decimal("100")).quantize(Decimal("0.01"))
                else:
                    pnl = None
                init_enriched.append({
                    "market": market,
                    "balance": bal,
                    "avg_buy_price": avg if avg > 0 else None,
                    "current_price": price,
                    "pnl_percent": pnl
                })

        for it in init_enriched:
            mkt = it["market"]
            bal = it["balance"]
            avg = it["avg_buy_price"]
            if not avg or bal <= 0:
                continue
            est = (bal * avg).quantize(Decimal("0.0001"))
            ps.buy_info[mkt] = {"total_buys": 1, "total_invested": est}
            st = ps.data.setdefault(mkt, {})
            st.setdefault("entry_source", "pre_existing")
            st.setdefault("first_buy_amount", est)
            st.setdefault("stop_mode_active", False)
            st.setdefault("martingale_count", 0)
            st.setdefault("last_martin_ts", None)
            st["over_cap_flag"] = (fh.MAX_TOTAL_INVEST_PER_MARKET > 0 and est >= fh.MAX_TOTAL_INVEST_PER_MARKET)

            for k, v in {
                "stop_triggered": False, "stop_last_peak": None, "stop_sells_done": 0,
                "worst_pnl": Decimal("0"), "stop_last_sell_ts": None,
                "stop_cooldown_flag": False, "stop_pending_peak": None, "stop_cooldown_start_ts": None
            }.items():
                st.setdefault(k, v)

            if (not fh.FULL_LIMIT_SELL_ENABLED) and fh.PREPLACE_HARD_TP and fh.PREPLACE_ON_START:
                try:
                    locked = Decimal("0")
                    for a2 in init_accounts:
                        if a2.get("currency") == mkt.split("-")[1]:
                            locked = Decimal(str(a2.get("locked", "0")))
                            break
                    total_bal = bal + locked
                except:
                    total_bal = bal
                await place_preplaced_hard_tp(access_key, secret_key, mkt, st, total_bal, avg)

        print(f"[INFO] 초기 추적 {len(ps.buy_info)}개 (orphan={len(orphan)})")
    except Exception as e:
        print(f"[WARN] 초기화 부분 실패 (진행): {e}")

    try:
        await restart_reseed_after_cancellation(access_key, secret_key, ps, init_accounts, init_prices)
        print("[INIT] 재시작 초기화(일괄 취소→재배치) 완료")
    except Exception as e:
        print(f"[INIT][WARN] 재시작 초기화 실패 (다음 루프에서 보정): {e}")

    await align_to_half_minute()
    prev_active_count = None

    # ===== 메인 모니터링 루프 =====
    while True:
        try:
            await get_excluded_markets()
            raw_accounts = await fetch_upbit_accounts(access_key, secret_key)
        except Exception as e:
            print(f"[ERR] 잔고 조회 실패: {e}")
            await dynamic_sleep()
            continue

        tradable = await load_tradable_markets()
        markets_raw = build_market_list_from_accounts(raw_accounts, fh.BASE_UNIT)
        invalid = [m for m in markets_raw if m not in tradable]
        if invalid:
            print(f"[LOOP] 거래불가 제외: {invalid}")
        markets = [m for m in markets_raw if m in tradable]

        try:
            price_map = await safe_fetch_current_prices(markets)
        except Exception as e:
            print(f"[WARN] 가격 조회 실패: {e}")
            price_map = {}

        if fh.FULL_LIMIT_SELL_ENABLED:
            try:
                await manage_full_limit_sells(access_key, secret_key, raw_accounts, price_map, ps)
            except Exception as fle:
                print(f"[FLS][ERR] 관리루프 예외: {fle}")
            await dynamic_sleep()
            continue

        if (not fh.FULL_LIMIT_SELL_ENABLED) and fh.PASSIVE_LIMIT_SELL_ENABLED:
            try:
                await manage_passive_limit_sells(access_key, secret_key, raw_accounts, price_map, ps)
            except Exception as ple:
                print(f"[PASSIVE][ERR] 관리루프 예외: {ple}")

        enriched_all = enrich_accounts_with_prices(raw_accounts, price_map, fh.BASE_UNIT)
        enriched = [
            r for r in enriched_all
            if r.get("current_price") is not None
               and r.get("market") not in (fh.EXCLUDED_MARKETS or set())
        ]
        available_krw = get_available_krw(raw_accounts)

        active_by_notional = set(get_active_markets(enriched))
        active_count = len(active_by_notional)
        remaining_slots = max(0, fh.MAX_ACTIVE_MARKETS - active_count)

        if prev_active_count is None or prev_active_count != active_count:
            print(f"[PORTFOLIO] 활성 {active_count}개 (한도 {fh.MAX_ACTIVE_MARKETS}), remaining_slots={remaining_slots}")
            prev_active_count = active_count

        actions = []
        sell_orders = []

        for it in enriched:
            market = it.get("market")
            pnl = it.get("pnl_percent")
            avg = it.get("avg_buy_price")
            bal = it.get("balance") or Decimal("0")
            locked = it.get("locked") or Decimal("0")
            eff_bal = bal + locked
            cur_price = it.get("current_price")
            if not market or pnl is None or avg is None or cur_price is None:
                continue
            if ps.recently_sold(market):
                continue
            if eff_bal * cur_price < fh.MIN_NOTIONAL_KRW:
                continue

            st = ps.update_or_init(market, pnl, avg)
            ps.ensure_stop_fields(market)

            if st.get("active_limit_uuid"):
                uid = st["active_limit_uuid"]
                cat_pending = st.get("limit_pending_category", "")
                submit_ts = st.get("limit_submit_ts")
                elapsed = (time.time() - submit_ts) if submit_ts else 0

                effective_timeout = fh.LIMIT_SELL_UNFILLED_TIMEOUT_SEC
                if cat_pending == "PRE_HARD_TP1":
                    effective_timeout = None if fh.PREPLACE_TP_TIMEOUT_SEC <= 0 else fh.PREPLACE_TP_TIMEOUT_SEC

                try:
                    od = await get_order(access_key, secret_key, uid)
                    od_state = od.get("state")
                    rem_vol_raw = od.get("remaining_volume")
                    exec_vol_raw = od.get("executed_volume")
                    try:
                        rem_vol = Decimal(str(rem_vol_raw)) if rem_vol_raw is not None else Decimal("0")
                        exec_vol = Decimal(str(exec_vol_raw)) if exec_vol_raw is not None else Decimal("0")
                    except:
                        rem_vol = Decimal("0"); exec_vol = Decimal("0")
                    total_req = None
                    try:
                        total_req = Decimal(st.get("limit_pending_volume", "0"))
                    except:
                        pass
                    if total_req and exec_vol > 0 and exec_vol < total_req:
                        st["pre_tp_partial_filled"] = True

                    if od_state == "done" or rem_vol <= 0:
                        cat = cat_pending
                        print(f"[LIMIT-FILL] market={market} cat={cat} uuid={uid} filled={exec_vol}")
                        if cat == "PRE_HARD_TP1":
                            st["hard_tp_taken"] = True
                            portion_used = get_preplace_portion()
                            if portion_used < 1:
                                ps.reduce_invested_after_sell(market, portion_used)
                            else:
                                ps.mark_sold(market)
                        elif cat == "HARD_TP1":
                            st["hard_tp_taken"] = True
                            if fh.HARD_TP_SELL_PORTION < 1:
                                ps.reduce_invested_after_sell(market, fh.HARD_TP_SELL_PORTION)
                            else:
                                ps.mark_sold(market)
                        elif cat == "HARD_TP2":
                            st["hard_tp2_taken"] = True
                            if fh.HARD_TP2_SELL_PORTION < 1:
                                ps.reduce_invested_after_sell(market, fh.HARD_TP2_SELL_PORTION)
                            else:
                                ps.mark_sold(market)
                        elif cat == "TRAIL":
                            ps.mark_sold(market)

                        for k in ("active_limit_uuid", "limit_submit_ts", "limit_pending_category", "limit_pending_volume"):
                            st.pop(k, None)
                        continue

                    if effective_timeout and elapsed > effective_timeout:
                        try:
                            await cancel_order(access_key, secret_key, uid)
                            print(f"[LIMIT-CANCEL] timeout market={market} cat={cat_pending}")
                        except Exception as ce:
                            print(f"[LIMIT-CANCEL] 실패 market={market}: {ce}")
                        for k in ("active_limit_uuid", "limit_submit_ts", "limit_pending_category", "limit_pending_volume"):
                            st.pop(k, None)
                    continue
                except Exception as e:
                    print(f"[LIMIT] 조회 실패 market={market} uuid={uid} e={e}")
                    continue

            sell, reason, category = decide_sell(market, pnl, st)
            actions.append({
                "market": market, "pnl": pnl, "peak": st["peak_pnl"],
                "drawdown": st.get("last_drawdown"), "max_dd": st.get("max_drawdown"),
                "min_pnl": st.get("min_pnl"), "armed": st["armed"],
                "sell": sell, "cat": category, "reason": reason
            })
            if sell:
                if category == "HARD_TP1":
                    portion = fh.HARD_TP_SELL_PORTION
                elif category == "HARD_TP2":
                    portion = fh.HARD_TP2_SELL_PORTION
                else:
                    portion = fh.SELL_PORTION
                volume = safe_calc_volume(bal, portion)
                if volume <= 0:
                    if category in ("TRAIL", "HARD_TP2") and portion >= 1:
                        ps.mark_sold(market)
                    continue
                sell_orders.append({
                    "market": market, "volume": volume, "pnl": pnl,
                    "category": category, "reason": reason,
                    "state_ref": st, "portion": portion
                })

        immediate_sells = []
        for it in enriched:
            market = it.get("market")
            pnl = it.get("pnl_percent")
            bal = it.get("balance")
            cur_price = it.get("current_price")
            if not market or pnl is None or bal is None or cur_price is None:
                continue
            st = ps.data.setdefault(market, {})
            ps.ensure_stop_fields(market)

            if fh.ENABLE_STOP:
                if pnl <= fh.STOP_TRIGGER_PNL:
                    if not st["stop_mode_active"]:
                        print(f"[STOP-MODE][ON] {market} pnl={pnl}% <= trigger {fh.STOP_TRIGGER_PNL}%")
                    st["stop_mode_active"] = True
                else:
                    if st["stop_mode_active"]:
                        print(f"[STOP-MODE][OFF] {market} pnl={pnl}% > trigger {fh.STOP_TRIGGER_PNL}%")
                    st["stop_mode_active"] = False
            else:
                st["stop_mode_active"] = False

            if fh.ENABLE_STOP and pnl <= fh.LOSS_CUT_RATE:
                immediate_sells.append({
                    "market": market,
                    "pnl": pnl,
                    "balance": bal,
                    "current_price": cur_price
                })

        if fh.ENABLE_STOP:
            for item in immediate_sells:
                mkt = item["market"]
                st = ps.data.get(mkt) or {}
                for k in ("active_limit_uuid", "passive_limit_uuid", "full_limit_uuid", "pre_tp_uuid"):
                    uid = st.get(k)
                    if uid:
                        try:
                            await cancel_order(access_key, secret_key, uid)
                            if k != "pre_tp_uuid":
                                st[k] = None
                        except Exception as ce:
                            print(f"[LOSS-CUT][WARN] cancel {k} {mkt} err={ce}")
                await cancel_all_open_orders_for_market(access_key, secret_key, mkt, max_rounds=4, sleep_between=0.3)
                bal_now, locked_now = await wait_unlock_and_get_balance(access_key, secret_key, mkt, timeout=2.0, poll=0.25)
                vol = safe_calc_volume(bal_now, Decimal("1"))
                if vol <= 0:
                    continue
                try:
                    best_bid, _ = await get_orderbook_top(mkt)
                    notional = vol * best_bid
                    if notional < fh.MIN_ORDER_NOTIONAL_KRW:
                        continue
                except Exception:
                    pass

                try:
                    if not fh.LIVE_TRADING:
                        print(f"[LOSS-CUT][DRY] {mkt} pnl={item['pnl']}% (<= {fh.LOSS_CUT_RATE}%) → SELL ALL vol={vol}")
                    else:
                        resp = await order_market_sell(access_key, secret_key, mkt, vol)
                        print(f"[LOSS-CUT] {mkt} pnl={item['pnl']}% → SELL ALL uuid={resp.get('uuid')} vol={vol}")
                    st["stop_mode_active"] = False
                    st["martingale_count"] = 0
                    st["last_martin_ts"] = None
                    st["last_martin_amount"] = None
                    ps.mark_sold(mkt)
                except Exception as e:
                    print(f"[LOSS-CUT][ERR] {mkt} market sell fail: {e}")

        martin_used_krw_total = Decimal("0")
        if fh.ENABLE_MARTIN:
            for it in enriched:
                market = it.get("market")
                pnl = it.get("pnl_percent")
                cur_price = it.get("current_price")
                if not market or pnl is None or cur_price is None:
                    continue
                st = ps.data.get(market) or {}
                if not st.get("stop_mode_active"):
                    continue
                try:
                    guard = martin_guard_or_skip(market, ps)
                except NameError:
                    guard = False
                if guard:
                    continue

                cur_martin = int(st.get("martingale_count", 0))
                if cur_martin >= fh.MAX_MARTIN:
                    continue

                _, total_inv = ps.get_buy_stats(market)
                if total_inv <= 0:
                    continue

                last_martin_amt = st.get("last_martin_amount")
                try:
                    last_martin_amt = Decimal(str(last_martin_amt)) if last_martin_amt is not None else None
                except:
                    last_martin_amt = None

                if cur_martin == 0:
                    buy_amt = (total_inv * Decimal("2")).quantize(Decimal("0.0001"))
                else:
                    buy_amt = ((last_martin_amt or total_inv) * Decimal("2")).quantize(Decimal("0.0001"))

                remaining_cap = fh.MAX_TOTAL_INVEST_PER_MARKET - total_inv if fh.MAX_TOTAL_INVEST_PER_MARKET > 0 else buy_amt
                if fh.MAX_TOTAL_INVEST_PER_MARKET > 0 and remaining_cap <= 0:
                    print(f"[MARTIN][SKIP] {market} per-market cap reached (inv={total_inv} >= cap={fh.MAX_TOTAL_INVEST_PER_MARKET})")
                    continue

                buy_amt_eff = min(buy_amt, remaining_cap, available_krw)

                if fh.MAX_TOTAL_INVEST_PER_MARKET > 0:
                    new_total = total_inv + buy_amt_eff
                    if new_total > fh.MAX_TOTAL_INVEST_PER_MARKET:
                        clip = (fh.MAX_TOTAL_INVEST_PER_MARKET - total_inv)
                        if clip <= 0:
                            continue
                        buy_amt_eff = min(buy_amt_eff, clip)
                        if buy_amt_eff < fh.MIN_ORDER_NOTIONAL_KRW:
                            continue

                if buy_amt_eff < fh.MIN_ORDER_NOTIONAL_KRW:
                    print(f"[MARTIN][SKIP] {market} clipped_amt {buy_amt_eff} < MIN_ORDER_NOTIONAL_KRW {fh.MIN_ORDER_NOTIONAL_KRW}")
                    continue

                try:
                    if not fh.LIVE_TRADING:
                        print(f"[MARTIN][DRY-BUY] {market} m#{cur_martin + 1} pnl={pnl}% buyKRW={buy_amt_eff} (total_inv={total_inv}, last={last_martin_amt})")
                    else:
                        resp = await order_market_buy_price(access_key, secret_key, market, buy_amt_eff)
                        print(f"[MARTIN][BUY] {market} m#{cur_martin + 1} pnl={pnl}% 금액={buy_amt_eff} uuid={resp.get('uuid')}")
                    ps.record_buy(market, buy_amt_eff)
                    st["martingale_count"] = cur_martin + 1
                    st["last_martin_ts"] = time.time()
                    st["last_martin_amount"] = buy_amt_eff
                    st.setdefault("entry_source", "martin")
                    martin_used_krw_total += buy_amt_eff
                    if fh.PREPLACE_HARD_TP and fh.LIVE_TRADING:
                        asyncio.create_task(after_market_buy_place_pre_tp(access_key, secret_key, market, ps))
                except Exception as e:
                    print(f"[MARTIN][ERR] {market} buy fail: {e}")
        else:
            for it in enriched:
                mkt = it.get("market")
                st = ps.data.get(mkt) or {}
                if st.get("martingale_count") or st.get("last_martin_ts") or st.get("last_martin_amount"):
                    st["martingale_count"] = 0
                    st["last_martin_ts"] = None
                    st["last_martin_amount"] = None

        if martin_used_krw_total > 0:
            available_krw -= martin_used_krw_total

        realized = Decimal("0")
        for so in sell_orders:
            market = so["market"]
            volume = so["volume"]
            category = so["category"]
            pnl = so["pnl"]
            reason = so["reason"]
            st = so["state_ref"]
            portion = so["portion"]
            if volume <= 0:
                continue

            cur_price = None
            for _it in enriched:
                if _it.get("market") == market:
                    cur_price = _it.get("current_price")
                    break

            if not fh.LIVE_TRADING:
                print(f"[DRY_SELL] {market} {category} vol={volume} pnl={pnl}% {reason}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                    if portion < 1:
                        ps.reduce_invested_after_sell(market, portion)
                    else:
                        ps.mark_sold(market)
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                elif category == "TRAIL":
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                else:
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                if cur_price and portion > 0:
                    try: realized += cur_price * volume
                    except: pass
                continue

            try:
                resp = await order_market_sell(access_key, secret_key, market, volume)
                uid = resp.get("uuid")
                print(f"[ORDER] SELL {market} cat={category} vol={volume} pnl={pnl}% uuid={uid} reason={reason}")
                if category == "HARD_TP1":
                    st["hard_tp_taken"] = True
                    if portion < 1:
                        ps.reduce_invested_after_sell(market, portion)
                    else:
                        ps.mark_sold(market)
                elif category == "HARD_TP2":
                    st["hard_tp2_taken"] = True
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                elif category == "TRAIL":
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                else:
                    if portion >= 1:
                        ps.mark_sold(market)
                    else:
                        ps.reduce_invested_after_sell(market, portion)
                if cur_price and portion > 0:
                    try: realized += cur_price * volume
                    except: pass
            except Exception as e:
                print(f"[ERR] 매도 실패 {market}: {e}")
                continue

        if realized > 0:
            available_krw += realized

        try:
            used = await process_intersection_buys(
                access_key=access_key,
                secret_key=secret_key,
                ps=ps,
                remaining_slots=remaining_slots,
                available_krw=available_krw,
            )
            try:
                used = Decimal(str(used))
            except Exception:
                used = Decimal("0")
            if used > 0:
                available_krw -= used
        except Exception as e:
            import traceback
            print(f"[INTX][ERROR] {e}")
            traceback.print_exc()

        if actions:
            print(f"\n[{time.strftime('%H:%M:%S')}] 상태 요약:")
            for a in actions:
                cat = a["cat"]
                status = "HOLD"
                if a["sell"]:
                    status = "SELL"
                elif a["reason"].startswith("ARMED"):
                    status = "ARM"
                print(
                    f"  {a['market']} pnl={a['pnl']} peak={a['peak']} "
                    f"min={a['min_pnl']} dd={a['drawdown']} maxDD={a['max_dd']} "
                    f"armed={a['armed']} cat={cat} -> {status} {a['reason']}"
                )

        if fh.PERIODIC_RESET_SEC > 0 and (time.time() - start_ts) >= fh.PERIODIC_RESET_SEC:
            print(f"[RESET] periodic reset requested after {fh.PERIODIC_RESET_SEC}s")
            raise ResetRequested()

        await dynamic_sleep()


async def run_mtpond_controller(user_no: int, server_no: int):
    task: asyncio.Task | None = None
    global _last_cfg_signature
    print(f"[CTRL] Controller 시작 user={user_no} server={server_no} poll={fh.CONTROLLER_POLL_SEC}s")
    while True:
        try:
            cfg = await fetch_mtpond_setup(user_no)
            if cfg:
                active_flag = str(cfg.get("activeYN", "")).upper() == "Y"
                sig_items = []
                for k, v in cfg.items():
                    if k == "activeYN":
                        continue
                    sig_items.append((k, str(v)))
                sig = tuple(sorted(sig_items))
                if sig != _last_cfg_signature:
                    apply_dynamic_config(cfg)
                    _last_cfg_signature = sig
            else:
                active_flag = None
            now = time.strftime("%H:%M:%S")

            if task and task.done():
                try:
                    exc = task.exception()
                    if isinstance(exc, ResetRequested):
                        print(f"[CTRL {now}] 주기 리셋(ResetRequested) 감지 → 정상 재시작 준비")
                    elif exc:
                        print(f"[CTRL {now}] 모니터 태스크 예외 종료: {exc}")
                except asyncio.CancelledError:
                    print(f"[CTRL {now}] 모니터 태스크 취소됨")
                task = None

            if active_flag is True:
                if task is None:
                    print(f"[CTRL {now}] activeYN=Y → 모니터 시작")
                    task = asyncio.create_task(monitor_positions(user_no, server_no))
            elif active_flag is False:
                if task and not task.done():
                    print(f"[CTRL {now}] activeYN=N → 모니터 중지")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        print("[CTRL] monitorPositions 취소됨")
                    except Exception as e:
                        print(f"[CTRL] monitorPositions 예외 종료: {e}")
                    task = None
            else:
                print(f"[CTRL {now}] activeYN 조회 실패(None) → 상태 유지")

        except Exception as e:
            print(f"[CTRL] 루프 예외: {e}")

        await asyncio.sleep(fh.CONTROLLER_POLL_SEC)


async def main():
    init_config()
    user_no = int(os.getenv("USER_NO", "100001"))
    server_no = int(os.getenv("SERVER_NO", "21"))
    await run_mtpond_controller(user_no, server_no)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] 종료 요청(Ctrl+C)")