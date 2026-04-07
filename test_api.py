"""
Buy-the-Dip - Bloomberg API Smoke Test
Run: python test_api.py
All checks must PASS before running the main program.
"""

import blpapi
import sys
import numpy as np
from datetime import datetime, timedelta

PASS_COUNT = 0
FAIL_COUNT = 0


def create_session():
    opts = blpapi.SessionOptions()
    opts.setServerHost("localhost")
    opts.setServerPort(8194)
    return blpapi.Session(opts)


def collect(session, timeout_ms=15000):
    msgs = []
    while True:
        ev = session.nextEvent(timeout_ms)
        for msg in ev:
            msgs.append(msg)
        if ev.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.TIMEOUT):
            break
    return msgs


def check(name, passed, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if passed:
        PASS_COUNT += 1
        print(f"  [PASS] {name}  {detail}")
    else:
        FAIL_COUNT += 1
        print(f"  [FAIL] {name}  {detail}")


def _extract_bdp_field(msgs, field):
    """Extract a single field value from BDP response."""
    for msg in msgs:
        if not msg.hasElement("securityData"):
            continue
        arr = msg.getElement("securityData")
        for i in range(arr.numValues()):
            sec = arr.getValueAsElement(i)
            if sec.hasElement("securityError"):
                return None
            fd = sec.getElement("fieldData") if sec.hasElement("fieldData") else None
            if fd and fd.hasElement(field):
                try:
                    return fd.getElementAsFloat(field)
                except Exception:
                    return fd.getElementAsString(field)
    return None


def _extract_bdh_rows(msgs, fields):
    """Extract rows from BDH response. Returns list of dicts."""
    rows = []
    for msg in msgs:
        if not msg.hasElement("securityData"):
            continue
        sec = msg.getElement("securityData")
        if sec.hasElement("securityError"):
            return []
        if not sec.hasElement("fieldData"):
            continue
        fd_arr = sec.getElement("fieldData")
        for i in range(fd_arr.numValues()):
            el = fd_arr.getValueAsElement(i)
            row = {}
            for f in fields:
                if el.hasElement(f):
                    try:
                        row[f] = el.getElementAsFloat(f)
                    except Exception:
                        row[f] = None
                else:
                    row[f] = None
            rows.append(row)
    return rows


def run_all_tests():
    global PASS_COUNT, FAIL_COUNT
    PASS_COUNT = 0
    FAIL_COUNT = 0

    print("=" * 60)
    print("  Bloomberg API Smoke Test")
    print("=" * 60)

    # ---- 1. Connection ----
    session = create_session()
    ok = session.start()
    check("1. Connect to Bloomberg (localhost:8194)", ok)
    if not ok:
        print("\n  FATAL: Cannot connect. Is Bloomberg Terminal running?")
        return False

    # ---- 2. Open refdata service ----
    ok2 = session.openService("//blp/refdata")
    check("2. Open //blp/refdata service", ok2)
    if not ok2:
        session.stop()
        return False
    svc = session.getService("//blp/refdata")

    end_str = datetime.now().strftime("%Y%m%d")
    start_str = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")

    # ---- 3. BDP: basic quote ----
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue("AAPL US Equity")
    req.getElement("fields").appendValue("PX_LAST")
    session.sendRequest(req)
    px = _extract_bdp_field(collect(session), "PX_LAST")
    check("3. BDP snapshot (AAPL PX_LAST)", px is not None and px > 0,
          f"val={px}")

    # ---- 4. BDH: historical price ----
    req = svc.createRequest("HistoricalDataRequest")
    req.getElement("securities").appendValue("AAPL US Equity")
    req.getElement("fields").appendValue("PX_LAST")
    req.set("periodicitySelection", "DAILY")
    req.set("startDate", start_str)
    req.set("endDate", end_str)
    session.sendRequest(req)
    rows = _extract_bdh_rows(collect(session), ["PX_LAST"])
    check("4. BDH daily price (AAPL, 10d)", len(rows) > 0,
          f"rows={len(rows)}")

    # ---- 5. BDH: CUR_MKT_CAP unit = millions USD ----
    req = svc.createRequest("HistoricalDataRequest")
    req.getElement("securities").appendValue("AAPL US Equity")
    req.getElement("fields").appendValue("CUR_MKT_CAP")
    req.set("periodicitySelection", "DAILY")
    req.set("startDate", start_str)
    req.set("endDate", end_str)
    session.sendRequest(req)
    rows = _extract_bdh_rows(collect(session), ["CUR_MKT_CAP"])
    cap = rows[-1]["CUR_MKT_CAP"] if rows else None
    # AAPL BDH mktcap should be ~3,000,000 (millions) = ~$3T
    ok5 = cap is not None and 100_000 < cap < 100_000_000
    check("5. BDH CUR_MKT_CAP unit=millionsUSD", ok5,
          f"val={cap:,.0f}M = ${cap/1000:,.0f}B" if cap else "None")

    # ---- 6. BDS: INDX_MEMBERS ----
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue("SPX Index")
    req.getElement("fields").appendValue("INDX_MEMBERS")
    session.sendRequest(req)
    msgs = collect(session)
    n_members = 0
    sample_ticker = ""
    for msg in msgs:
        if not msg.hasElement("securityData"):
            continue
        arr = msg.getElement("securityData")
        sec = arr.getValueAsElement(0)
        fd = sec.getElement("fieldData")
        if fd.hasElement("INDX_MEMBERS"):
            bulk = fd.getElement("INDX_MEMBERS")
            n_members = bulk.numValues()
            if n_members > 0:
                row = bulk.getValueAsElement(0)
                if row.hasElement("Member Ticker and Exchange Code"):
                    sample_ticker = row.getElementAsString(
                        "Member Ticker and Exchange Code")
    check("6. BDS INDX_MEMBERS (SPX Index)", n_members > 400,
          f"n={n_members}, sample='{sample_ticker}'")

    # ---- 7. Ticker format: sample should be exchange-specific ----
    has_exchange_code = False
    if sample_ticker:
        parts = sample_ticker.strip().split()
        has_exchange_code = len(parts) == 2 and parts[1] in {"UW", "UN", "UA", "UQ", "UP"}
    check("7. INDX_MEMBERS ticker uses exchange code (UW/UN)",
          has_exchange_code, f"'{sample_ticker}' -> need US conversion")

    # ---- 8. BDH: BEST_EPS with FPERIOD_OVERRIDE ----
    req = svc.createRequest("HistoricalDataRequest")
    req.getElement("securities").appendValue("AAPL US Equity")
    req.getElement("fields").appendValue("BEST_EPS")
    req.set("periodicitySelection", "DAILY")
    req.set("startDate", start_str)
    req.set("endDate", end_str)
    overrides = req.getElement("overrides")
    ov = overrides.appendElement()
    ov.setElement("fieldId", "BEST_FPERIOD_OVERRIDE")
    ov.setElement("value", "1BF")
    session.sendRequest(req)
    rows = _extract_bdh_rows(collect(session), ["BEST_EPS"])
    eps = rows[-1]["BEST_EPS"] if rows else None
    check("8. BDH BEST_EPS + FPERIOD_OVERRIDE=1BF",
          eps is not None and eps != 0, f"val={eps}")

    # ---- 9. BDH: BEST_EPS WITHOUT override -> behavior check ----
    # Note: Some stocks/dates return data without override (default period),
    # others return empty. Override is BEST PRACTICE for consistency.
    req = svc.createRequest("HistoricalDataRequest")
    req.getElement("securities").appendValue("AAPL US Equity")
    req.getElement("fields").appendValue("BEST_EPS")
    req.set("periodicitySelection", "DAILY")
    req.set("startDate", start_str)
    req.set("endDate", end_str)
    session.sendRequest(req)
    rows_no_ov = _extract_bdh_rows(collect(session), ["BEST_EPS"])
    # Always PASS — this is an informational check, not a blocker
    check("9. BDH BEST_EPS without override (info only)", True,
          f"rows={len(rows_no_ov)} (override still recommended for consistency)")

    # ---- 10. BDH: index price (SPX) ----
    req = svc.createRequest("HistoricalDataRequest")
    req.getElement("securities").appendValue("SPX Index")
    req.getElement("fields").appendValue("PX_LAST")
    req.set("periodicitySelection", "DAILY")
    req.set("startDate", (datetime.now() - timedelta(days=400)).strftime("%Y%m%d"))
    req.set("endDate", end_str)
    session.sendRequest(req)
    rows = _extract_bdh_rows(collect(session), ["PX_LAST"])
    check("10. BDH index price (SPX, 400d)", len(rows) > 200,
          f"rows={len(rows)}")

    session.stop()

    # ---- Summary ----
    total = PASS_COUNT + FAIL_COUNT
    print(f"\n{'='*60}")
    print(f"  Result: {PASS_COUNT}/{total} PASS, {FAIL_COUNT}/{total} FAIL")
    print(f"{'='*60}")
    if FAIL_COUNT > 0:
        print("  Some tests FAILED. Main program may not work correctly.")
        return False
    else:
        print("  All PASSED. Ready to run main program.")
        return True


if __name__ == "__main__":
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
