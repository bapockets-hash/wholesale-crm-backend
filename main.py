"""
Wholesale Real Estate CRM — FastAPI Backend
SQLite database, seeded from CSV files on first run.
"""

import csv
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "crm.db")

CSV_PATHS = {
    "leads":       os.getenv("CSV_LEADS",       "../../data/leads.csv"),
    "deals":       os.getenv("CSV_DEALS",       "../../data/task_tsk_069a/deals_pipeline.csv"),
    "wholesalers": os.getenv("CSV_WHOLESALERS", "../../data/wholesalers.csv"),
    "lenders":     os.getenv("CSV_LENDERS",     "../../data/lenders.csv"),
    "crm":         os.getenv("CSV_CRM",         "../../data/crm.csv"),
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------

CREATE_LEADS = """
CREATE TABLE IF NOT EXISTS leads (
    lead_id               TEXT PRIMARY KEY,
    date_found            TEXT,
    lead_type             TEXT,
    property_address      TEXT,
    property_city         TEXT,
    property_state        TEXT,
    property_zip          TEXT,
    estimated_value       REAL,
    value_source          TEXT,
    owner_name            TEXT,
    owner_mailing_address TEXT,
    owner_mailing_city    TEXT,
    owner_mailing_state   TEXT,
    owner_mailing_zip     TEXT,
    source_url            TEXT,
    source_site           TEXT,
    parcel_number         TEXT,
    days_delinquent       INTEGER,
    notes                 TEXT,
    skip_traced           INTEGER DEFAULT 0,
    score                 REAL DEFAULT 0
)
"""

CREATE_DEALS = """
CREATE TABLE IF NOT EXISTS deals (
    deal_id                  TEXT PRIMARY KEY,
    property_address         TEXT,
    seller_name              TEXT,
    seller_email             TEXT,
    seller_phone             TEXT,
    buyer_name               TEXT,
    buyer_email              TEXT,
    buyer_phone              TEXT,
    title_company_name       TEXT,
    title_company_contact    TEXT,
    title_company_email      TEXT,
    title_company_phone      TEXT,
    contract_price           REAL,
    buyers_price             REAL,
    assignment_fee           REAL,
    estimated_closing_costs  REAL,
    expected_net_profit      REAL,
    contract_execution_date  TEXT,
    emd_due_date             TEXT,
    emd_received_date        TEXT,
    title_ordered_date       TEXT,
    title_clear_date         TEXT,
    assignment_signed_date   TEXT,
    scheduled_closing_date   TEXT,
    actual_closing_date      TEXT,
    current_status           TEXT DEFAULT 'TITLE_ORDERED',
    days_to_close            INTEGER,
    title_issues             TEXT,
    missing_documents        TEXT,
    closing_notes            TEXT,
    last_updated             TEXT
)
"""

CREATE_WHOLESALERS = """
CREATE TABLE IF NOT EXISTS wholesalers (
    wholesaler_id        TEXT PRIMARY KEY,
    name                 TEXT,
    email                TEXT,
    phone                TEXT,
    company              TEXT,
    markets_served       TEXT,
    specialty            TEXT,
    tier                 TEXT,
    deals_closed_with_us INTEGER DEFAULT 0,
    avg_response_hours   REAL,
    last_contact_date    TEXT,
    notes                TEXT
)
"""

CREATE_LENDERS = """
CREATE TABLE IF NOT EXISTS lenders (
    lender_id        TEXT PRIMARY KEY,
    lender_name      TEXT,
    contact_name     TEXT,
    email            TEXT,
    phone            TEXT,
    website          TEXT,
    loan_type        TEXT,
    max_loan_amount  REAL,
    fee_percent      REAL,
    funding_days     INTEGER,
    states_served    TEXT,
    notes            TEXT
)
"""

CREATE_CRM = """
CREATE TABLE IF NOT EXISTS crm (
    id                TEXT PRIMARY KEY,
    lead_id           TEXT,
    seller_name       TEXT,
    phone             TEXT,
    email             TEXT,
    address           TEXT,
    arv               REAL,
    mao               REAL,
    last_contact_date TEXT,
    last_outcome      TEXT,
    follow_up_date    TEXT,
    status            TEXT,
    notes             TEXT
)
"""


def init_db():
    with get_db() as conn:
        conn.execute(CREATE_LEADS)
        conn.execute(CREATE_DEALS)
        conn.execute(CREATE_WHOLESALERS)
        conn.execute(CREATE_LENDERS)
        conn.execute(CREATE_CRM)
    print("[DB] Tables created / verified.")


# ---------------------------------------------------------------------------
# CSV seeding
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


def _read_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        print(f"[CSV] Not found: {path} — skipping.")
        return []
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()})
    return rows


def seed_from_csvs():
    """Seed each table from CSV only if the table is empty."""
    with get_db() as conn:

        # ---- LEADS ----
        if conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 0:
            rows = _read_csv(CSV_PATHS["leads"])
            for r in rows:
                conn.execute("""
                    INSERT OR IGNORE INTO leads VALUES (
                        :lead_id,:date_found,:lead_type,:property_address,
                        :property_city,:property_state,:property_zip,
                        :estimated_value,:value_source,:owner_name,
                        :owner_mailing_address,:owner_mailing_city,
                        :owner_mailing_state,:owner_mailing_zip,
                        :source_url,:source_site,:parcel_number,
                        :days_delinquent,:notes,:skip_traced,:score
                    )
                """, {
                    "lead_id": r.get("lead_id", ""),
                    "date_found": r.get("date_found", ""),
                    "lead_type": r.get("lead_type", ""),
                    "property_address": r.get("property_address", ""),
                    "property_city": r.get("property_city", ""),
                    "property_state": r.get("property_state", ""),
                    "property_zip": r.get("property_zip", ""),
                    "estimated_value": _safe_float(r.get("estimated_value")),
                    "value_source": r.get("value_source", ""),
                    "owner_name": r.get("owner_name", ""),
                    "owner_mailing_address": r.get("owner_mailing_address", ""),
                    "owner_mailing_city": r.get("owner_mailing_city", ""),
                    "owner_mailing_state": r.get("owner_mailing_state", ""),
                    "owner_mailing_zip": r.get("owner_mailing_zip", ""),
                    "source_url": r.get("source_url", ""),
                    "source_site": r.get("source_site", ""),
                    "parcel_number": r.get("parcel_number", ""),
                    "days_delinquent": _safe_int(r.get("days_delinquent")),
                    "notes": r.get("notes", ""),
                    "skip_traced": 1 if str(r.get("skip_traced", "")).lower() in ("1", "true", "yes") else 0,
                    "score": _safe_float(r.get("score")) or 0,
                })
            print(f"[CSV] Seeded {len(rows)} leads.")

        # ---- DEALS ----
        if conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0] == 0:
            rows = _read_csv(CSV_PATHS["deals"])
            for r in rows:
                conn.execute("""
                    INSERT OR IGNORE INTO deals VALUES (
                        :deal_id,:property_address,:seller_name,:seller_email,
                        :seller_phone,:buyer_name,:buyer_email,:buyer_phone,
                        :title_company_name,:title_company_contact,
                        :title_company_email,:title_company_phone,
                        :contract_price,:buyers_price,:assignment_fee,
                        :estimated_closing_costs,:expected_net_profit,
                        :contract_execution_date,:emd_due_date,:emd_received_date,
                        :title_ordered_date,:title_clear_date,
                        :assignment_signed_date,:scheduled_closing_date,
                        :actual_closing_date,:current_status,:days_to_close,
                        :title_issues,:missing_documents,:closing_notes,:last_updated
                    )
                """, {
                    "deal_id": r.get("deal_id", ""),
                    "property_address": r.get("property_address", ""),
                    "seller_name": r.get("seller_name", ""),
                    "seller_email": r.get("seller_email", ""),
                    "seller_phone": r.get("seller_phone", ""),
                    "buyer_name": r.get("buyer_name", ""),
                    "buyer_email": r.get("buyer_email", ""),
                    "buyer_phone": r.get("buyer_phone", ""),
                    "title_company_name": r.get("title_company_name", ""),
                    "title_company_contact": r.get("title_company_contact", ""),
                    "title_company_email": r.get("title_company_email", ""),
                    "title_company_phone": r.get("title_company_phone", ""),
                    "contract_price": _safe_float(r.get("contract_price")),
                    "buyers_price": _safe_float(r.get("buyers_price")),
                    "assignment_fee": _safe_float(r.get("assignment_fee")),
                    "estimated_closing_costs": _safe_float(r.get("estimated_closing_costs")),
                    "expected_net_profit": _safe_float(r.get("expected_net_profit")),
                    "contract_execution_date": r.get("contract_execution_date", ""),
                    "emd_due_date": r.get("emd_due_date", ""),
                    "emd_received_date": r.get("emd_received_date", ""),
                    "title_ordered_date": r.get("title_ordered_date", ""),
                    "title_clear_date": r.get("title_clear_date", ""),
                    "assignment_signed_date": r.get("assignment_signed_date", ""),
                    "scheduled_closing_date": r.get("scheduled_closing_date", ""),
                    "actual_closing_date": r.get("actual_closing_date", ""),
                    "current_status": r.get("current_status", "TITLE_ORDERED"),
                    "days_to_close": _safe_int(r.get("days_to_close")),
                    "title_issues": r.get("title_issues", ""),
                    "missing_documents": r.get("missing_documents", ""),
                    "closing_notes": r.get("closing_notes", ""),
                    "last_updated": r.get("last_updated", ""),
                })
            print(f"[CSV] Seeded {len(rows)} deals.")

        # ---- WHOLESALERS ----
        if conn.execute("SELECT COUNT(*) FROM wholesalers").fetchone()[0] == 0:
            rows = _read_csv(CSV_PATHS["wholesalers"])
            for r in rows:
                conn.execute("""
                    INSERT OR IGNORE INTO wholesalers VALUES (
                        :wholesaler_id,:name,:email,:phone,:company,
                        :markets_served,:specialty,:tier,
                        :deals_closed_with_us,:avg_response_hours,
                        :last_contact_date,:notes
                    )
                """, {
                    "wholesaler_id": r.get("wholesaler_id", ""),
                    "name": r.get("name", ""),
                    "email": r.get("email", ""),
                    "phone": r.get("phone", ""),
                    "company": r.get("company", ""),
                    "markets_served": r.get("markets_served", ""),
                    "specialty": r.get("specialty", ""),
                    "tier": r.get("tier", ""),
                    "deals_closed_with_us": _safe_int(r.get("deals_closed_with_us")) or 0,
                    "avg_response_hours": _safe_float(r.get("avg_response_hours")),
                    "last_contact_date": r.get("last_contact_date", ""),
                    "notes": r.get("notes", ""),
                })
            print(f"[CSV] Seeded {len(rows)} wholesalers.")

        # ---- LENDERS ----
        if conn.execute("SELECT COUNT(*) FROM lenders").fetchone()[0] == 0:
            rows = _read_csv(CSV_PATHS["lenders"])
            for r in rows:
                conn.execute("""
                    INSERT OR IGNORE INTO lenders VALUES (
                        :lender_id,:lender_name,:contact_name,:email,:phone,
                        :website,:loan_type,:max_loan_amount,:fee_percent,
                        :funding_days,:states_served,:notes
                    )
                """, {
                    "lender_id": r.get("lender_id", ""),
                    "lender_name": r.get("lender_name", ""),
                    "contact_name": r.get("contact_name", ""),
                    "email": r.get("email", ""),
                    "phone": r.get("phone", ""),
                    "website": r.get("website", ""),
                    "loan_type": r.get("loan_type", ""),
                    "max_loan_amount": _safe_float(r.get("max_loan_amount")),
                    "fee_percent": _safe_float(r.get("fee_percent")),
                    "funding_days": _safe_int(r.get("funding_days")),
                    "states_served": r.get("states_served", ""),
                    "notes": r.get("notes", ""),
                })
            print(f"[CSV] Seeded {len(rows)} lenders.")

        # ---- CRM ----
        if conn.execute("SELECT COUNT(*) FROM crm").fetchone()[0] == 0:
            rows = _read_csv(CSV_PATHS["crm"])
            for r in rows:
                conn.execute("""
                    INSERT OR IGNORE INTO crm VALUES (
                        :id,:lead_id,:seller_name,:phone,:email,:address,
                        :arv,:mao,:last_contact_date,:last_outcome,
                        :follow_up_date,:status,:notes
                    )
                """, {
                    "id": r.get("id", ""),
                    "lead_id": r.get("lead_id", ""),
                    "seller_name": r.get("seller_name", ""),
                    "phone": r.get("phone", ""),
                    "email": r.get("email", ""),
                    "address": r.get("address", ""),
                    "arv": _safe_float(r.get("arv")),
                    "mao": _safe_float(r.get("mao")),
                    "last_contact_date": r.get("last_contact_date", ""),
                    "last_outcome": r.get("last_outcome", ""),
                    "follow_up_date": r.get("follow_up_date", ""),
                    "status": r.get("status", ""),
                    "notes": r.get("notes", ""),
                })
            print(f"[CSV] Seeded {len(rows)} CRM records.")

    print("[DB] Seeding complete.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Wholesale CRM API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()
    seed_from_csvs()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class WholesalerCreate(BaseModel):
    wholesaler_id: Optional[str] = None
    name: str
    email: Optional[str] = ""
    phone: Optional[str] = ""
    company: Optional[str] = ""
    markets_served: Optional[str] = ""
    specialty: Optional[str] = ""
    tier: Optional[str] = "C"
    deals_closed_with_us: Optional[int] = 0
    avg_response_hours: Optional[float] = None
    last_contact_date: Optional[str] = ""
    notes: Optional[str] = ""


# ---------------------------------------------------------------------------
# LEADS endpoints
# ---------------------------------------------------------------------------

@app.get("/api/leads")
def list_leads(
    skip_traced: Optional[int] = Query(None),
    limit: int = Query(500),
):
    with get_db() as conn:
        if skip_traced is not None:
            rows = conn.execute(
                "SELECT * FROM leads WHERE skip_traced=? ORDER BY score DESC LIMIT ?",
                (skip_traced, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM leads ORDER BY score DESC LIMIT ?", (limit,)
            ).fetchall()
    return [row_to_dict(r) for r in rows]


@app.get("/api/leads/{lead_id}")
def get_lead(lead_id: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM leads WHERE lead_id=?", (lead_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")
    return row_to_dict(row)


LEAD_WRITABLE_COLS = {
    "date_found", "lead_type", "property_address", "property_city", "property_state",
    "property_zip", "estimated_value", "value_source", "owner_name",
    "owner_mailing_address", "owner_mailing_city", "owner_mailing_state",
    "owner_mailing_zip", "source_url", "source_site", "parcel_number",
    "days_delinquent", "notes", "skip_traced", "score",
}

@app.patch("/api/leads/{lead_id}")
def update_lead(lead_id: str, body: Dict[str, Any]):
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")
    bad = [k for k in body if k not in LEAD_WRITABLE_COLS]
    if bad:
        raise HTTPException(status_code=400, detail=f"Invalid fields: {bad}")
    cols = ", ".join(f"{k}=?" for k in body)
    vals = list(body.values()) + [lead_id]
    with get_db() as conn:
        conn.execute(f"UPDATE leads SET {cols} WHERE lead_id=?", vals)
    return {"ok": True}


# ---------------------------------------------------------------------------
# DEALS endpoints
# ---------------------------------------------------------------------------

DEAL_STATUSES = ["TITLE_ORDERED", "TITLE_CLEAR", "ASSIGNMENT_SIGNED", "CLOSING_CONFIRMED", "CLOSED"]


@app.get("/api/deals")
def list_deals(status: Optional[str] = Query(None)):
    with get_db() as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM deals WHERE current_status=? ORDER BY last_updated DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM deals ORDER BY last_updated DESC"
            ).fetchall()
    return [row_to_dict(r) for r in rows]


@app.get("/api/deals/{deal_id}")
def get_deal(deal_id: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM deals WHERE deal_id=?", (deal_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Deal not found")
    return row_to_dict(row)


DEAL_WRITABLE_COLS = {
    "property_address", "seller_name", "seller_email", "seller_phone",
    "buyer_name", "buyer_email", "buyer_phone",
    "title_company_name", "title_company_contact", "title_company_email", "title_company_phone",
    "contract_price", "buyers_price", "assignment_fee", "estimated_closing_costs", "expected_net_profit",
    "contract_execution_date", "emd_due_date", "emd_received_date",
    "title_ordered_date", "title_clear_date", "assignment_signed_date",
    "scheduled_closing_date", "actual_closing_date", "current_status",
    "days_to_close", "title_issues", "missing_documents", "closing_notes", "last_updated",
}

@app.patch("/api/deals/{deal_id}")
def update_deal(deal_id: str, body: Dict[str, Any]):
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")
    bad = [k for k in body if k not in DEAL_WRITABLE_COLS]
    if bad:
        raise HTTPException(status_code=400, detail=f"Invalid fields: {bad}")
    body["last_updated"] = date.today().isoformat()
    cols = ", ".join(f"{k}=?" for k in body)
    vals = list(body.values()) + [deal_id]
    with get_db() as conn:
        conn.execute(f"UPDATE deals SET {cols} WHERE deal_id=?", vals)
    return {"ok": True}


# ---------------------------------------------------------------------------
# WHOLESALERS endpoints
# ---------------------------------------------------------------------------

@app.get("/api/wholesalers")
def list_wholesalers():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM wholesalers ORDER BY tier ASC, deals_closed_with_us DESC"
        ).fetchall()
    return [row_to_dict(r) for r in rows]


@app.post("/api/wholesalers", status_code=201)
def create_wholesaler(w: WholesalerCreate):
    import uuid
    wid = w.wholesaler_id or f"W-{uuid.uuid4().hex[:8].upper()}"
    with get_db() as conn:
        conn.execute("""
            INSERT INTO wholesalers VALUES (
                :wholesaler_id,:name,:email,:phone,:company,
                :markets_served,:specialty,:tier,
                :deals_closed_with_us,:avg_response_hours,
                :last_contact_date,:notes
            )
        """, {
            "wholesaler_id": wid,
            "name": w.name,
            "email": w.email,
            "phone": w.phone,
            "company": w.company,
            "markets_served": w.markets_served,
            "specialty": w.specialty,
            "tier": w.tier,
            "deals_closed_with_us": w.deals_closed_with_us,
            "avg_response_hours": w.avg_response_hours,
            "last_contact_date": w.last_contact_date,
            "notes": w.notes,
        })
    return {"wholesaler_id": wid, "ok": True}


@app.patch("/api/wholesalers/{wholesaler_id}")
def update_wholesaler(wholesaler_id: str, body: Dict[str, Any]):
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")
    cols = ", ".join(f"{k}=?" for k in body)
    vals = list(body.values()) + [wholesaler_id]
    with get_db() as conn:
        conn.execute(f"UPDATE wholesalers SET {cols} WHERE wholesaler_id=?", vals)
    return {"ok": True}


# ---------------------------------------------------------------------------
# LENDERS endpoints
# ---------------------------------------------------------------------------

@app.get("/api/lenders")
def list_lenders():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM lenders ORDER BY funding_days ASC"
        ).fetchall()
    return [row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# CRM endpoints
# ---------------------------------------------------------------------------

@app.get("/api/crm")
def list_crm(status: Optional[str] = Query(None)):
    with get_db() as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM crm WHERE status=? ORDER BY follow_up_date ASC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM crm ORDER BY follow_up_date ASC"
            ).fetchall()
    return [row_to_dict(r) for r in rows]


@app.patch("/api/crm/{crm_id}")
def update_crm(crm_id: str, body: Dict[str, Any]):
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")
    cols = ", ".join(f"{k}=?" for k in body)
    vals = list(body.values()) + [crm_id]
    with get_db() as conn:
        conn.execute(f"UPDATE crm SET {cols} WHERE id=?", vals)
    return {"ok": True}


# ---------------------------------------------------------------------------
# DASHBOARD endpoint
# ---------------------------------------------------------------------------

@app.get("/api/dashboard")
def dashboard():
    today = date.today().isoformat()

    with get_db() as conn:
        # Leads
        total_leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        skip_traced = conn.execute("SELECT COUNT(*) FROM leads WHERE skip_traced=1").fetchone()[0]
        pending_skip = total_leads - skip_traced

        # Deals
        all_deals = conn.execute("SELECT * FROM deals").fetchall()
        active_deals = [d for d in all_deals if dict(d)["current_status"] != "CLOSED"]
        stages = {s: 0 for s in DEAL_STATUSES}
        realized = 0.0
        projected = 0.0
        at_risk = 0
        pof_needed = 0

        for d in all_deals:
            r = dict(d)
            status = r.get("current_status", "")
            stages[status] = stages.get(status, 0) + 1
            profit = r.get("expected_net_profit") or 0
            if status == "CLOSED":
                realized += profit
            else:
                projected += profit
            missing = r.get("missing_documents") or ""
            if missing.strip() and status != "CLOSED":
                at_risk += 1
            if status == "ASSIGNMENT_SIGNED" and "proof_of_funds" in missing.lower():
                pof_needed += 1

        total_pipeline = sum(
            (dict(d).get("assignment_fee") or 0) for d in all_deals if dict(d)["current_status"] != "CLOSED"
        )

        # Network
        wholesaler_count = conn.execute("SELECT COUNT(*) FROM wholesalers").fetchone()[0]

    return {
        "leads": {
            "total": total_leads,
            "skip_traced": skip_traced,
            "pending_skip_trace": pending_skip,
        },
        "deals": {
            "active": len(active_deals),
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
            "wholesalers": wholesaler_count,
        },
    }


# ---------------------------------------------------------------------------
# ACTION QUEUE endpoint
# ---------------------------------------------------------------------------

@app.get("/api/action-queue")
def action_queue():
    today = date.today()
    items = []

    with get_db() as conn:
        all_deals = [dict(d) for d in conn.execute("SELECT * FROM deals").fetchall()]

    for d in all_deals:
        status = d.get("current_status", "")
        missing = (d.get("missing_documents") or "").lower()
        addr = d.get("property_address", "Unknown")
        deal_id = d.get("deal_id", "")

        # HIGH: ASSIGNMENT_SIGNED missing proof of funds
        if status == "ASSIGNMENT_SIGNED" and "proof_of_funds" in missing:
            items.append({
                "priority": "HIGH",
                "deal_id": deal_id,
                "message": f"POF needed — {addr}",
                "detail": "Deal is ASSIGNMENT_SIGNED but proof of funds is missing.",
            })

        # HIGH: closing within 3 days
        scd = d.get("scheduled_closing_date", "")
        if scd and status != "CLOSED":
            try:
                close_dt = date.fromisoformat(scd)
                delta = (close_dt - today).days
                if 0 <= delta <= 3:
                    items.append({
                        "priority": "HIGH",
                        "deal_id": deal_id,
                        "message": f"Closing in {delta}d — {addr}",
                        "detail": f"Scheduled closing: {scd}",
                    })
            except ValueError:
                pass

        # MEDIUM: overdue EMD
        emd_due = d.get("emd_due_date", "")
        emd_recv = d.get("emd_received_date", "")
        if emd_due and not emd_recv and status != "CLOSED":
            try:
                emd_dt = date.fromisoformat(emd_due)
                if emd_dt < today:
                    items.append({
                        "priority": "MEDIUM",
                        "deal_id": deal_id,
                        "message": f"EMD overdue — {addr}",
                        "detail": f"EMD was due {emd_due}, not yet received.",
                    })
            except ValueError:
                pass

    # Sort: HIGH first
    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    items.sort(key=lambda x: priority_order.get(x["priority"], 9))
    return items


# ---------------------------------------------------------------------------
# P&L endpoint
# ---------------------------------------------------------------------------

@app.get("/api/pnl")
def pnl(period: Optional[str] = Query("all")):
    """
    Returns profit & loss data broken down by month and deal.
    period: 'all' | 'ytd' | '90d' | '30d'
    """
    from datetime import timedelta
    today_dt = date.today()

    cutoff = None
    if period == "ytd":
        cutoff = date(today_dt.year, 1, 1).isoformat()
    elif period == "90d":
        cutoff = (today_dt - timedelta(days=90)).isoformat()
    elif period == "30d":
        cutoff = (today_dt - timedelta(days=30)).isoformat()

    with get_db() as conn:
        all_deals = [dict(d) for d in conn.execute("SELECT * FROM deals").fetchall()]

    if cutoff:
        def in_period(d):
            # Active (non-closed) deals always included regardless of period
            if d.get("current_status") != "CLOSED":
                return True
            cd = d.get("actual_closing_date") or d.get("contract_execution_date") or ""
            return cd >= cutoff
        all_deals = [d for d in all_deals if in_period(d)]

    closed = [d for d in all_deals if d.get("current_status") == "CLOSED"]
    active = [d for d in all_deals if d.get("current_status") != "CLOSED"]

    gross_revenue = sum(d.get("assignment_fee") or 0 for d in closed)
    total_closing_costs = sum(d.get("estimated_closing_costs") or 0 for d in closed)
    net_profit = sum(d.get("expected_net_profit") or 0 for d in closed)
    projected_revenue = sum(d.get("assignment_fee") or 0 for d in active)
    projected_net = sum(d.get("expected_net_profit") or 0 for d in active)
    avg_assignment = (gross_revenue / len(closed)) if closed else 0
    days_vals = [d.get("days_to_close") for d in closed if d.get("days_to_close")]
    avg_days = round(sum(days_vals) / len(days_vals), 1) if days_vals else None

    monthly = {}
    for d in all_deals:
        close_date = d.get("actual_closing_date") or d.get("contract_execution_date") or ""
        if not close_date:
            continue
        month_key = close_date[:7]
        if month_key not in monthly:
            monthly[month_key] = {"month": month_key, "closed_deals": 0, "gross_revenue": 0.0, "closing_costs": 0.0, "net_profit": 0.0}
        if d.get("current_status") == "CLOSED":
            monthly[month_key]["closed_deals"] += 1
            monthly[month_key]["gross_revenue"] += d.get("assignment_fee") or 0
            monthly[month_key]["closing_costs"] += d.get("estimated_closing_costs") or 0
            monthly[month_key]["net_profit"] += d.get("expected_net_profit") or 0

    monthly_list = sorted(monthly.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["gross_revenue"] = round(m["gross_revenue"], 2)
        m["closing_costs"] = round(m["closing_costs"], 2)
        m["net_profit"] = round(m["net_profit"], 2)

    deal_rows = []
    for d in all_deals:
        deal_rows.append({
            "deal_id": d.get("deal_id"),
            "property_address": d.get("property_address"),
            "status": d.get("current_status"),
            "contract_price": d.get("contract_price"),
            "buyers_price": d.get("buyers_price"),
            "assignment_fee": d.get("assignment_fee"),
            "estimated_closing_costs": d.get("estimated_closing_costs"),
            "expected_net_profit": d.get("expected_net_profit"),
            "actual_closing_date": d.get("actual_closing_date"),
            "scheduled_closing_date": d.get("scheduled_closing_date"),
            "days_to_close": d.get("days_to_close"),
            "seller_name": d.get("seller_name"),
            "buyer_name": d.get("buyer_name"),
        })
    deal_rows.sort(key=lambda x: (x["actual_closing_date"] or x["scheduled_closing_date"] or ""), reverse=True)

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
# BUYERS endpoint
# ---------------------------------------------------------------------------

@app.get("/api/buyers")
def list_buyers():
    buyers_path = os.getenv("CSV_BUYERS", "../../data/buyers.csv")
    rows = _read_csv(buyers_path)
    return rows


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "Any Property CRM API"}
