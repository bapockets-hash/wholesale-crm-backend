"""
Wholesale Real Estate CRM — FastAPI Backend
Reads CSV files directly on every request (no SQLite cache).
CSV paths are controlled by environment variables.
"""

import csv
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config — CSV paths via env vars
# ---------------------------------------------------------------------------
BASE = os.getenv("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))

def csv_path(name: str, default: str) -> str:
    return os.getenv(f"CSV_{name.upper()}", os.path.join(BASE, default))

# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _safe_float(v):
    try:
        return float(str(v).replace(",", "").replace("$", "").strip()) if v not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None

def _safe_int(v):
    try:
        return int(float(str(v).strip())) if v not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None

def read_csv(name: str, default_filename: str) -> List[Dict[str, Any]]:
    path = csv_path(name, default_filename)
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()})
    return rows

def write_csv(name: str, default_filename: str, rows: List[Dict[str, Any]], fieldnames: List[str]):
    path = csv_path(name, default_filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Wholesale CRM API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# LEADS
# ---------------------------------------------------------------------------

@app.get("/api/leads")
def list_leads(skip_traced: Optional[int] = Query(None), limit: int = Query(500)):
    rows = read_csv("leads", "leads.csv")
    if skip_traced is not None:
        flag = str(skip_traced)
        rows = [r for r in rows if str(r.get("skip_traced", "")).lower() in (flag, "true" if flag == "1" else "false")]
    rows.sort(key=lambda r: _safe_float(r.get("score")) or 0, reverse=True)
    return rows[:limit]

@app.get("/api/leads/{lead_id}")
def get_lead(lead_id: str):
    rows = read_csv("leads", "leads.csv")
    for r in rows:
        if r.get("lead_id") == lead_id:
            return r
    raise HTTPException(status_code=404, detail="Lead not found")

# ---------------------------------------------------------------------------
# SKIP TRACED LEADS
# ---------------------------------------------------------------------------

@app.get("/api/skip-traced-leads")
def list_skip_traced_leads(
    trace_status: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    limit: int = Query(500),
):
    rows = read_csv("skip_traced_leads", "skip_traced_leads.csv")
    if trace_status:
        rows = [r for r in rows if r.get("trace_status", "").lower() == trace_status.lower()]
    if state:
        rows = [r for r in rows if r.get("property_state", "").upper() == state.upper()]
    rows.sort(key=lambda r: _safe_float(r.get("score")) or 0, reverse=True)
    return rows[:limit]

# ---------------------------------------------------------------------------
# DEALS
# ---------------------------------------------------------------------------

DEAL_STATUSES = ["TITLE_ORDERED", "TITLE_CLEAR", "ASSIGNMENT_SIGNED", "CLOSING_CONFIRMED", "CLOSED"]

DEALS_FIELDNAMES = [
    "deal_id","property_address","seller_name","seller_email","seller_phone",
    "buyer_name","buyer_email","buyer_phone","title_company_name","title_company_contact",
    "title_company_email","title_company_phone","contract_price","buyers_price",
    "assignment_fee","estimated_closing_costs","expected_net_profit",
    "contract_execution_date","emd_due_date","emd_received_date","title_ordered_date",
    "title_clear_date","assignment_signed_date","scheduled_closing_date",
    "actual_closing_date","current_status","days_to_close","title_issues",
    "missing_documents","closing_notes","last_updated"
]

@app.get("/api/deals")
def list_deals(status: Optional[str] = Query(None)):
    rows = read_csv("deals", "deals_pipeline.csv")
    if status:
        rows = [r for r in rows if r.get("current_status", "").upper() == status.upper()]
    rows.sort(key=lambda r: r.get("last_updated", ""), reverse=True)
    return rows

@app.get("/api/deals/{deal_id}")
def get_deal(deal_id: str):
    rows = read_csv("deals", "deals_pipeline.csv")
    for r in rows:
        if r.get("deal_id") == deal_id:
            return r
    raise HTTPException(status_code=404, detail="Deal not found")

@app.patch("/api/deals/{deal_id}")
def update_deal(deal_id: str, body: Dict[str, Any]):
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")
    rows = read_csv("deals", "deals_pipeline.csv")
    found = False
    fieldnames = DEALS_FIELDNAMES
    for r in rows:
        if r.get("deal_id") == deal_id:
            r.update(body)
            r["last_updated"] = date.today().isoformat()
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail="Deal not found")
    if rows:
        fieldnames = list(rows[0].keys())
    write_csv("deals", "deals_pipeline.csv", rows, fieldnames)
    return {"ok": True}

# ---------------------------------------------------------------------------
# WHOLESALERS / JV PARTNERS
# ---------------------------------------------------------------------------

@app.get("/api/wholesalers")
def list_wholesalers():
    rows = read_csv("wholesalers", "wholesalers.csv")
    return rows

@app.post("/api/wholesalers", status_code=201)
def create_wholesaler(body: Dict[str, Any]):
    import uuid
    rows = read_csv("wholesalers", "wholesalers.csv")
    body["wholesaler_id"] = body.get("wholesaler_id") or f"W-{uuid.uuid4().hex[:8].upper()}"
    rows.append(body)
    fieldnames = list(rows[0].keys()) if rows else list(body.keys())
    write_csv("wholesalers", "wholesalers.csv", rows, fieldnames)
    return {"wholesaler_id": body["wholesaler_id"], "ok": True}

# ---------------------------------------------------------------------------
# LENDERS
# ---------------------------------------------------------------------------

@app.get("/api/lenders")
def list_lenders():
    rows = read_csv("lenders", "lenders.csv")
    rows.sort(key=lambda r: _safe_int(r.get("funding_days")) or 99)
    return rows

# ---------------------------------------------------------------------------
# BUYERS
# ---------------------------------------------------------------------------

@app.get("/api/buyers")
def list_buyers():
    rows = read_csv("buyers", "buyers.csv")
    return rows

# ---------------------------------------------------------------------------
# SELLERS CRM
# ---------------------------------------------------------------------------

@app.get("/api/crm")
def list_crm(status: Optional[str] = Query(None)):
    rows = read_csv("crm", "crm.csv")
    if status:
        rows = [r for r in rows if r.get("status", "").lower() == status.lower()]
    rows.sort(key=lambda r: r.get("follow_up_date", "") or "")
    return rows

@app.patch("/api/crm/{crm_id}")
def update_crm(crm_id: str, body: Dict[str, Any]):
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")
    rows = read_csv("crm", "crm.csv")
    found = False
    for r in rows:
        if r.get("id") == crm_id:
            r.update(body)
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail="CRM record not found")
    fieldnames = list(rows[0].keys()) if rows else []
    write_csv("crm", "crm.csv", rows, fieldnames)
    return {"ok": True}

# ---------------------------------------------------------------------------
# OUTREACH LOG
# ---------------------------------------------------------------------------

@app.get("/api/outreach-log")
def list_outreach_log(limit: int = Query(200)):
    rows = read_csv("outreach_log", "outreach_log.csv")
    rows.sort(key=lambda r: r.get("sent_at", ""), reverse=True)
    return rows[:limit]

# ---------------------------------------------------------------------------
# INBOUND LOG
# ---------------------------------------------------------------------------

@app.get("/api/inbound-log")
def list_inbound_log(limit: int = Query(100)):
    rows = read_csv("inbound_log", "inbound_log.csv")
    rows.sort(key=lambda r: r.get("received_at", "") or r.get("timestamp", ""), reverse=True)
    return rows[:limit]

# ---------------------------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------------------------

@app.get("/api/dashboard")
def dashboard():
    today_str = date.today().isoformat()
    today_dt = date.today()

    leads = read_csv("leads", "leads.csv")
    skip_traced_leads = read_csv("skip_traced_leads", "skip_traced_leads.csv")
    deals = read_csv("deals", "deals_pipeline.csv")
    wholesalers = read_csv("wholesalers", "wholesalers.csv")
    buyers = read_csv("buyers", "buyers.csv")
    outreach = read_csv("outreach_log", "outreach_log.csv")

    # -- Leads stats
    total_leads = len(leads)
    skip_traced_count = sum(
        1 for r in leads if str(r.get("skip_traced", "")).lower() in ("1", "true", "yes")
    )
    pending_skip = total_leads - skip_traced_count
    traced_with_phone = sum(
        1 for r in skip_traced_leads if r.get("owner_phone", "").strip()
    )
    traced_with_email = sum(
        1 for r in skip_traced_leads if r.get("owner_email", "").strip()
    )

    # lead types breakdown
    lead_types = {}
    for r in leads:
        lt = r.get("lead_type", "unknown")
        lead_types[lt] = lead_types.get(lt, 0) + 1

    # states breakdown
    lead_states = {}
    for r in leads:
        st = r.get("property_state", "?")
        lead_states[st] = lead_states.get(st, 0) + 1

    # -- Deal stats
    active_deals = [d for d in deals if d.get("current_status", "").upper() != "CLOSED"]
    closed_deals = [d for d in deals if d.get("current_status", "").upper() == "CLOSED"]
    stages = {s: 0 for s in DEAL_STATUSES}
    realized = 0.0
    projected = 0.0
    at_risk = 0
    pof_needed = 0

    for d in deals:
        status = (d.get("current_status") or "").upper()
        stages[status] = stages.get(status, 0) + 1
        profit = _safe_float(d.get("expected_net_profit")) or 0
        if status == "CLOSED":
            realized += profit
        else:
            projected += profit
        missing = (d.get("missing_documents") or "").lower()
        if missing.strip() and status != "CLOSED":
            at_risk += 1
        if status == "ASSIGNMENT_SIGNED" and "proof_of_funds" in missing:
            pof_needed += 1

    total_pipeline = sum(
        (_safe_float(d.get("assignment_fee")) or 0)
        for d in deals if (d.get("current_status") or "").upper() != "CLOSED"
    )

    # -- Outreach stats
    outreach_sent = len(outreach)
    outreach_delivered = sum(1 for r in outreach if r.get("status", "").lower() == "delivered")
    outreach_failed = sum(1 for r in outreach if r.get("status", "").lower() in ("failed", "error"))
    email_count = sum(1 for r in outreach if r.get("channel", "").lower() == "email")
    sms_count = sum(1 for r in outreach if r.get("channel", "").lower() == "sms")

    # -- Buyers stats
    total_buyers = len(buyers)
    buyers_with_email = sum(1 for r in buyers if r.get("email", "").strip())
    tier_a = sum(1 for r in buyers if (r.get("tier") or "").upper() == "A")
    tier_b = sum(1 for r in buyers if (r.get("tier") or "").upper() == "B")

    return {
        "leads": {
            "total": total_leads,
            "skip_traced": skip_traced_count,
            "pending_skip_trace": pending_skip,
            "traced_with_phone": traced_with_phone,
            "traced_with_email": traced_with_email,
            "by_type": lead_types,
            "by_state": lead_states,
        },
        "deals": {
            "active": len(active_deals),
            "closed": len(closed_deals),
            "stages": stages,
            "at_risk": at_risk,
            "pof_needed": pof_needed,
        },
        "financials": {
            "realized_profit": round(realized, 2),
            "projected_profit": round(projected, 2),
            "total_pipeline_value": round(total_pipeline, 2),
        },
        "network": {
            "wholesalers": len(wholesalers),
            "buyers": total_buyers,
            "buyers_with_email": buyers_with_email,
            "tier_a_buyers": tier_a,
            "tier_b_buyers": tier_b,
        },
        "outreach": {
            "total_sent": outreach_sent,
            "delivered": outreach_delivered,
            "failed": outreach_failed,
            "email": email_count,
            "sms": sms_count,
        },
    }

# ---------------------------------------------------------------------------
# ACTION QUEUE
# ---------------------------------------------------------------------------

@app.get("/api/action-queue")
def action_queue():
    today_dt = date.today()
    items = []
    deals = read_csv("deals", "deals_pipeline.csv")

    for d in deals:
        status = (d.get("current_status") or "").upper()
        missing = (d.get("missing_documents") or "").lower()
        addr = d.get("property_address", "Unknown")
        deal_id = d.get("deal_id", "")

        if status == "ASSIGNMENT_SIGNED" and "proof_of_funds" in missing:
            items.append({"priority": "HIGH", "deal_id": deal_id,
                          "message": f"POF needed — {addr}",
                          "detail": "Deal is ASSIGNMENT_SIGNED but proof of funds is missing."})

        scd = d.get("scheduled_closing_date", "")
        if scd and status != "CLOSED":
            try:
                delta = (date.fromisoformat(scd) - today_dt).days
                if 0 <= delta <= 3:
                    items.append({"priority": "HIGH", "deal_id": deal_id,
                                  "message": f"Closing in {delta}d — {addr}",
                                  "detail": f"Scheduled closing: {scd}"})
            except ValueError:
                pass

        emd_due = d.get("emd_due_date", "")
        emd_recv = d.get("emd_received_date", "")
        if emd_due and not emd_recv and status != "CLOSED":
            try:
                if date.fromisoformat(emd_due) < today_dt:
                    items.append({"priority": "MEDIUM", "deal_id": deal_id,
                                  "message": f"EMD overdue — {addr}",
                                  "detail": f"EMD was due {emd_due}, not yet received."})
            except ValueError:
                pass

    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    items.sort(key=lambda x: priority_order.get(x["priority"], 9))
    return items

# ---------------------------------------------------------------------------
# P&L
# ---------------------------------------------------------------------------

@app.get("/api/pnl")
def pnl(period: Optional[str] = Query("all")):
    today_dt = date.today()
    cutoff = None
    if period == "ytd":
        cutoff = date(today_dt.year, 1, 1).isoformat()
    elif period == "90d":
        cutoff = (today_dt - timedelta(days=90)).isoformat()
    elif period == "30d":
        cutoff = (today_dt - timedelta(days=30)).isoformat()

    all_deals = read_csv("deals", "deals_pipeline.csv")
    if cutoff:
        def in_period(d):
            if (d.get("current_status") or "").upper() != "CLOSED":
                return True
            cd = d.get("actual_closing_date") or d.get("contract_execution_date") or ""
            return cd >= cutoff
        all_deals = [d for d in all_deals if in_period(d)]

    closed = [d for d in all_deals if (d.get("current_status") or "").upper() == "CLOSED"]
    active = [d for d in all_deals if (d.get("current_status") or "").upper() != "CLOSED"]

    gross_revenue = sum((_safe_float(d.get("assignment_fee")) or 0) for d in closed)
    total_closing_costs = sum((_safe_float(d.get("estimated_closing_costs")) or 0) for d in closed)
    net_profit = sum((_safe_float(d.get("expected_net_profit")) or 0) for d in closed)
    projected_revenue = sum((_safe_float(d.get("assignment_fee")) or 0) for d in active)
    projected_net = sum((_safe_float(d.get("expected_net_profit")) or 0) for d in active)
    avg_assignment = (gross_revenue / len(closed)) if closed else 0
    days_vals = [_safe_int(d.get("days_to_close")) for d in closed if d.get("days_to_close")]
    avg_days = round(sum(days_vals) / len(days_vals), 1) if days_vals else None

    monthly = {}
    for d in all_deals:
        close_date = d.get("actual_closing_date") or d.get("contract_execution_date") or ""
        if not close_date:
            continue
        mk = close_date[:7]
        if mk not in monthly:
            monthly[mk] = {"month": mk, "closed_deals": 0, "gross_revenue": 0.0, "closing_costs": 0.0, "net_profit": 0.0}
        if (d.get("current_status") or "").upper() == "CLOSED":
            monthly[mk]["closed_deals"] += 1
            monthly[mk]["gross_revenue"] += _safe_float(d.get("assignment_fee")) or 0
            monthly[mk]["closing_costs"] += _safe_float(d.get("estimated_closing_costs")) or 0
            monthly[mk]["net_profit"] += _safe_float(d.get("expected_net_profit")) or 0

    monthly_list = sorted(monthly.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["gross_revenue"] = round(m["gross_revenue"], 2)
        m["closing_costs"] = round(m["closing_costs"], 2)
        m["net_profit"] = round(m["net_profit"], 2)

    deal_rows = [{
        "deal_id": d.get("deal_id"),
        "property_address": d.get("property_address"),
        "status": d.get("current_status"),
        "contract_price": _safe_float(d.get("contract_price")),
        "buyers_price": _safe_float(d.get("buyers_price")),
        "assignment_fee": _safe_float(d.get("assignment_fee")),
        "estimated_closing_costs": _safe_float(d.get("estimated_closing_costs")),
        "expected_net_profit": _safe_float(d.get("expected_net_profit")),
        "actual_closing_date": d.get("actual_closing_date"),
        "scheduled_closing_date": d.get("scheduled_closing_date"),
        "days_to_close": _safe_int(d.get("days_to_close")),
        "seller_name": d.get("seller_name"),
        "buyer_name": d.get("buyer_name"),
    } for d in all_deals]
    deal_rows.sort(key=lambda x: (x.get("actual_closing_date") or x.get("scheduled_closing_date") or ""), reverse=True)

    return {
        "summary": {
            "closed_deals": len(closed),
            "active_deals": len(active),
            "gross_revenue": round(gross_revenue, 2),
            "total_closing_costs": round(total_closing_costs, 2),
            "net_profit": round(net_profit, 2),
            "projected_revenue": round(projected_revenue, 2),
            "projected_net": round(projected_net, 2),
            "avg_assignment_fee": round(avg_assignment, 2),
            "avg_days_to_close": avg_days,
            "margin_pct": round((net_profit / gross_revenue * 100), 1) if gross_revenue else 0,
        },
        "monthly": monthly_list,
        "deals": deal_rows,
    }

# ---------------------------------------------------------------------------
# INBOUND WEBSITE LEADS  (seller form on anyproperty.vercel.app)
# ---------------------------------------------------------------------------

CRM_FIELDNAMES = [
    "id", "name", "property_address", "phone", "email", "condition",
    "source", "lead_type", "status", "score", "notes",
    "created_at", "follow_up_date", "last_contacted",
]

@app.post("/api/inbound-lead", status_code=201)
def inbound_lead(body: Dict[str, Any]):
    import uuid, httpx as _httpx
    rows = read_csv("crm", "crm.csv")
    lead_id = f"WEB-{uuid.uuid4().hex[:8].upper()}"
    today = date.today().isoformat()
    new_row = {
        "id": lead_id,
        "name": body.get("name", ""),
        "property_address": body.get("property_address", ""),
        "phone": body.get("phone", ""),
        "email": body.get("email", ""),
        "condition": body.get("condition", ""),
        "source": body.get("source", "website_seller_form"),
        "lead_type": body.get("lead_type", "inbound_website"),
        "status": "new",
        "score": "",
        "notes": f"Inbound from website — condition: {body.get('condition', 'not specified')}",
        "created_at": today,
        "follow_up_date": today,
        "last_contacted": "",
    }
    rows.append(new_row)
    existing_fields = list(rows[0].keys()) if rows else CRM_FIELDNAMES
    write_csv("crm", "crm.csv", rows, existing_fields)

    # Fire-and-forget Telegram alert
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat  = os.getenv("TELEGRAM_CHAT_ID", "")
    if tg_token and tg_chat:
        msg = (
            f"NEW WEBSITE LEAD\n"
            f"Name: {new_row['name']}\n"
            f"Address: {new_row['property_address']}\n"
            f"Phone: {new_row['phone']}\n"
            f"Email: {new_row['email'] or 'N/A'}\n"
            f"Condition: {new_row['condition'] or 'Not specified'}\n"
            f"Lead ID: {lead_id}"
        )
        try:
            _httpx.post(
                f"https://api.telegram.org/bot{tg_token}/sendMessage",
                json={"chat_id": tg_chat, "text": msg},
                timeout=5,
            )
        except Exception:
            pass

    return {"lead_id": lead_id, "ok": True}


# ---------------------------------------------------------------------------
# BUYERS  (POST — buyer form on anyproperty.vercel.app)
# ---------------------------------------------------------------------------

BUYERS_FIELDNAMES = [
    "buyer_id", "name", "email", "phone", "markets",
    "price_range", "property_type", "source", "tier", "created_at",
]

@app.post("/api/buyers", status_code=201)
def create_buyer(body: Dict[str, Any]):
    import uuid
    rows = read_csv("buyers", "buyers.csv")
    buyer_id = f"BUY-{uuid.uuid4().hex[:8].upper()}"
    today = date.today().isoformat()
    new_row = {
        "buyer_id": buyer_id,
        "name": body.get("name", ""),
        "email": body.get("email", ""),
        "phone": body.get("phone", ""),
        "markets": body.get("markets", ""),
        "price_range": body.get("price_range", ""),
        "property_type": body.get("property_type", ""),
        "source": body.get("source", "website_buyer_form"),
        "tier": body.get("tier", "B"),
        "created_at": today,
    }
    rows.append(new_row)
    existing_fields = list(rows[0].keys()) if rows else BUYERS_FIELDNAMES
    write_csv("buyers", "buyers.csv", rows, existing_fields)
    return {"buyer_id": buyer_id, "ok": True}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "Any Property CRM API v2"}

@app.get("/api/health")
def health():
    files = {}
    for name, default in [
        ("leads", "leads.csv"), ("deals", "deals_pipeline.csv"),
        ("skip_traced_leads", "skip_traced_leads.csv"),
        ("buyers", "buyers.csv"), ("outreach_log", "outreach_log.csv"),
        ("crm", "crm.csv"), ("wholesalers", "wholesalers.csv"),
    ]:
        path = csv_path(name, default)
        if os.path.exists(path):
            rows = read_csv(name, default)
            files[name] = {"path": path, "rows": len(rows), "ok": True}
        else:
            files[name] = {"path": path, "rows": 0, "ok": False}
    return {"status": "ok", "data_dir": BASE, "files": files}
