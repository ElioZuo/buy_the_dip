"""
错杀抄底 — Bloomberg API 连通性测试
运行: python test_api.py
全部 PASS 才能运行主程序
"""

import blpapi
import sys
from datetime import datetime, timedelta

TESTS_PASSED = 0
TESTS_FAILED = 0


def create_session():
    opts = blpapi.SessionOptions()
    opts.setServerHost("localhost")
    opts.setServerPort(8194)
    session = blpapi.Session(opts)
    return session


def collect(session, timeout_ms=15000):
    msgs = []
    while True:
        ev = session.nextEvent(timeout_ms)
        for msg in ev:
            msgs.append(msg)
        if ev.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.TIMEOUT):
            break
    return msgs


def report(name, passed, detail=""):
    global TESTS_PASSED, TESTS_FAILED
    if passed:
        TESTS_PASSED += 1
        print(f"  ✓ {name}: PASS  {detail}")
    else:
        TESTS_FAILED += 1
        print(f"  ✗ {name}: FAIL  {detail}")


def run_all_tests():
    global TESTS_PASSED, TESTS_FAILED
    TESTS_PASSED = 0
    TESTS_FAILED = 0

    print("=" * 60)
    print("  Bloomberg API 连通性测试")
    print("=" * 60)

    # ---- Test 1: 连接 ----
    session = create_session()
    ok = session.start()
    report("连接 Bloomberg (localhost:8194)", ok)
    if not ok:
        print("\n  [FATAL] 无法连接, 请检查 Bloomberg Terminal 是否运行")
        return False

    # ---- Test 2: 打开 refdata service ----
    ok2 = session.openService("//blp/refdata")
    report("打开 //blp/refdata", ok2)
    if not ok2:
        session.stop()
        return False
    svc = session.getService("//blp/refdata")

    # ---- Test 3: BDP 快照 ----
    try:
        req = svc.createRequest("ReferenceDataRequest")
        req.getElement("securities").appendValue("AAPL US Equity")
        req.getElement("fields").appendValue("PX_LAST")
        session.sendRequest(req)
        msgs = collect(session)
        px = None
        for msg in msgs:
            if msg.hasElement("securityData"):
                arr = msg.getElement("securityData")
                sec = arr.getValueAsElement(0)
                fd = sec.getElement("fieldData")
                if fd.hasElement("PX_LAST"):
                    px = fd.getElementAsFloat("PX_LAST")
        ok3 = px is not None and px > 0
        report("BDP 快照 (AAPL PX_LAST)", ok3, f"value={px}")
    except Exception as e:
        report("BDP 快照", False, str(e))

    # ---- Test 4: BDH 历史价格 ----
    try:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        req = svc.createRequest("HistoricalDataRequest")
        req.getElement("securities").appendValue("AAPL US Equity")
        req.getElement("fields").appendValue("PX_LAST")
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", start)
        req.set("endDate", end)
        session.sendRequest(req)
        msgs = collect(session)
        n = 0
        for msg in msgs:
            if msg.hasElement("securityData"):
                sec = msg.getElement("securityData")
                if sec.hasElement("fieldData"):
                    n = sec.getElement("fieldData").numValues()
        ok4 = n > 0
        report("BDH 历史价格 (AAPL 近7天)", ok4, f"rows={n}")
    except Exception as e:
        report("BDH 历史价格", False, str(e))

    # ---- Test 5: BDH CUR_MKT_CAP 单位验证 ----
    try:
        req = svc.createRequest("HistoricalDataRequest")
        req.getElement("securities").appendValue("AAPL US Equity")
        req.getElement("fields").appendValue("CUR_MKT_CAP")
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", start)
        req.set("endDate", end)
        session.sendRequest(req)
        msgs = collect(session)
        cap = None
        for msg in msgs:
            if msg.hasElement("securityData"):
                sec = msg.getElement("securityData")
                if sec.hasElement("fieldData"):
                    fd = sec.getElement("fieldData")
                    if fd.numValues() > 0:
                        row = fd.getValueAsElement(fd.numValues() - 1)
                        if row.hasElement("CUR_MKT_CAP"):
                            cap = row.getElementAsFloat("CUR_MKT_CAP")
        # BDH 的 CUR_MKT_CAP 应该是百万美元级别 (AAPL ~3,000,000M = $3T)
        ok5 = cap is not None and 100_000 < cap < 100_000_000
        report("BDH CUR_MKT_CAP 单位=百万美元", ok5,
               f"value={cap:,.0f}M (${cap/1000:,.0f}B)" if cap else "None")
    except Exception as e:
        report("BDH CUR_MKT_CAP", False, str(e))

    # ---- Test 6: BDS INDX_MEMBERS ----
    try:
        req = svc.createRequest("ReferenceDataRequest")
        req.getElement("securities").appendValue("SPX Index")
        req.getElement("fields").appendValue("INDX_MEMBERS")
        session.sendRequest(req)
        msgs = collect(session)
        n_members = 0
        sample = ""
        for msg in msgs:
            if msg.hasElement("securityData"):
                arr = msg.getElement("securityData")
                sec = arr.getValueAsElement(0)
                fd = sec.getElement("fieldData")
                if fd.hasElement("INDX_MEMBERS"):
                    bulk = fd.getElement("INDX_MEMBERS")
                    n_members = bulk.numValues()
                    if n_members > 0:
                        row = bulk.getValueAsElement(0)
                        if row.hasElement("Member Ticker and Exchange Code"):
                            sample = row.getElementAsString(
                                "Member Ticker and Exchange Code")
        ok6 = n_members > 400
        report("BDS INDX_MEMBERS (SPX)", ok6,
               f"count={n_members}, sample='{sample}'")
    except Exception as e:
        report("BDS INDX_MEMBERS", False, str(e))

    # ---- Test 7: BDH BEST_EPS + Override ----
    try:
        req = svc.createRequest("HistoricalDataRequest")
        req.getElement("securities").appendValue("AAPL US Equity")
        req.getElement("fields").appendValue("BEST_EPS")
        req.set("periodicitySelection", "DAILY")
        req.set("startDate", start)
        req.set("endDate", end)
        overrides = req.getElement("overrides")
        ov = overrides.appendElement()
        ov.setElement("fieldId", "BEST_FPERIOD_OVERRIDE")
        ov.setElement("value", "1BF")
        session.sendRequest(req)
        msgs = collect(session)
        eps = None
        for msg in msgs:
            if msg.hasElement("securityData"):
                sec = msg.getElement("securityData")
                if sec.hasElement("fieldData"):
                    fd = sec.getElement("fieldData")
                    if fd.numValues() > 0:
                        row = fd.getValueAsElement(0)
                        if row.hasElement("BEST_EPS"):
                            eps = row.getElementAsFloat("BEST_EPS")
        ok7 = eps is not None and eps != 0
        report("BDH BEST_EPS + FPERIOD_OVERRIDE", ok7,
               f"value={eps}")
    except Exception as e:
        report("BDH BEST_EPS", False, str(e))

    session.stop()

    # ---- 汇总 ----
    print(f"\n{'='*60}")
    total = TESTS_PASSED + TESTS_FAILED
    print(f"  结果: {TESTS_PASSED}/{total} PASS, {TESTS_FAILED}/{total} FAIL")
    print(f"{'='*60}")

    if TESTS_FAILED > 0:
        print("  ✗ 有测试失败, 主程序可能无法正常运行")
        return False
    else:
        print("  ✓ 全部通过, 可以运行主程序")
        return True


if __name__ == "__main__":
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
