#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import argparse
import hashlib
from datetime import datetime, timezone
from typing import Dict, List

import requests
from PyPDF2 import PdfReader
import pyodbc

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

# -----------------------------
# Config
# -----------------------------

BORME_API = "https://boe.es/datosabiertos/api/borme/sumario/{yyyymmdd}"  # API oficial 

KEYWORDS_POS = [
    r"\bcesi[oó]n\b",
    r"\bcesi[oó]n de empresa\b",
    r"\btraspaso\b",
    r"\btransmisi[oó]n\b",
    r"\bjubilaci[oó]n\b",
    r"\brelevo generacional\b",
    r"\bunidad productiva\b",
]

KEYWORDS_GOV = [
    r"\bconvocatoria de junta\b",
    r"\bjunta general\b",
    r"\bcambio accionario\b",
]

KEYWORDS_NEG = [
    r"\bdisoluci[oó]n\b",
    r"\bliquidaci[oó]n\b",
    r"\bconcurso\b",
    r"\binsolvenc",
]

# ⚠️ URLs base (hasta que tengas links exactos de listados)
# Si luego me das las URLs de listados reales, lo hacemos más preciso.
REGIONAL_SOURCES = [
    {"source": "RelevoAragon",    "ccaa": "Aragón",          "list_url": "https://relevoaragon.com/"},
    {"source": "RELEVACyL",       "ccaa": "Castilla y León", "list_url": "https://empresas.jcyl.es/web/es/economia-social-autonomos/novedad-programa-relevacyl.html"},
    {"source": "RelevoCantabria", "ccaa": "Cantabria",       "list_url": "https://relevocantabria.com/"},
]

# -----------------------------
# Helpers
# -----------------------------

def utc_today_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def make_company_key(name: str, province: str, ccaa: str) -> str:
    base = f"{norm_text(name)}|{norm_text(province)}|{norm_text(ccaa)}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def detect_signals(text: str) -> Dict[str, bool]:
    t = norm_text(text)
    pos = any(re.search(p, t) for p in KEYWORDS_POS)
    gov = any(re.search(p, t) for p in KEYWORDS_GOV)
    neg = any(re.search(p, t) for p in KEYWORDS_NEG)
    return {"pos": pos, "gov": gov, "neg": neg}

def pdf_text_from_url(url: str) -> str:
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    from io import BytesIO
    bio = BytesIO(r.content)
    reader = PdfReader(bio)
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)

# -----------------------------
# Azure SQL
# -----------------------------

def sql_conn():
    server = os.environ["AZURE_SQL_SERVER"]
    db = os.environ["AZURE_SQL_DB"]
    user = os.environ["AZURE_SQL_USER"]
    pwd = os.environ["AZURE_SQL_PASSWORD"]

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={db};"
        f"Uid={user};"
        f"Pwd={pwd};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def ensure_schema(cur, schema: str):
    cur.execute(f"""
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
BEGIN
    EXEC('CREATE SCHEMA [{schema}]');
END
""")

    cur.execute(f"""
IF OBJECT_ID('{schema}.companies', 'U') IS NULL
BEGIN
  CREATE TABLE {schema}.companies (
    company_id INT IDENTITY(1,1) PRIMARY KEY,
    company_key VARCHAR(32) NOT NULL UNIQUE,
    name NVARCHAR(512) NOT NULL,
    province NVARCHAR(128) NULL,
    ccaa NVARCHAR(128) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
END
""")

    cur.execute(f"""
IF OBJECT_ID('{schema}.events', 'U') IS NULL
BEGIN
  CREATE TABLE {schema}.events (
    event_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source NVARCHAR(64) NOT NULL,
    source_ref NVARCHAR(256) NULL,
    event_date DATE NULL,
    event_type NVARCHAR(64) NOT NULL,
    title NVARCHAR(512) NULL,
    url NVARCHAR(1024) NULL,
    raw_excerpt NVARCHAR(MAX) NULL,
    company_key VARCHAR(32) NULL,
    inserted_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_events_company_key ON {schema}.events(company_key);
  CREATE INDEX IX_events_date ON {schema}.events(event_date);
END
""")

    cur.execute(f"""
IF OBJECT_ID('{schema}.signals', 'U') IS NULL
BEGIN
  CREATE TABLE {schema}.signals (
    signal_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    company_key VARCHAR(32) NOT NULL,
    signal_date DATE NOT NULL,
    signal_kind NVARCHAR(64) NOT NULL,
    weight INT NOT NULL,
    source NVARCHAR(64) NOT NULL,
    event_id BIGINT NULL,
    notes NVARCHAR(1024) NULL,
    inserted_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_signals_company_key ON {schema}.signals(company_key);
  CREATE INDEX IX_signals_date ON {schema}.signals(signal_date);
END
""")

    cur.execute(f"""
IF OBJECT_ID('{schema}.scores', 'U') IS NULL
BEGIN
  CREATE TABLE {schema}.scores (
    company_key VARCHAR(32) PRIMARY KEY,
    score INT NOT NULL,
    band NVARCHAR(16) NOT NULL,
    score_version NVARCHAR(32) NOT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
END
""")

def upsert_company(cur, schema: str, ckey: str, name: str, province: str, ccaa: str):
    cur.execute(f"""
MERGE {schema}.companies AS tgt
USING (SELECT ? AS company_key, ? AS name, ? AS province, ? AS ccaa) AS src
ON tgt.company_key = src.company_key
WHEN MATCHED THEN
  UPDATE SET name = src.name, province = src.province, ccaa = src.ccaa
WHEN NOT MATCHED THEN
  INSERT (company_key, name, province, ccaa) VALUES (src.company_key, src.name, src.province, src.ccaa);
""", (ckey, name, province, ccaa))

def insert_event(cur, schema: str, ev: Dict) -> int:
    cur.execute(f"""
INSERT INTO {schema}.events (source, source_ref, event_date, event_type, title, url, raw_excerpt, company_key)
OUTPUT INSERTED.event_id
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
""", (
        ev.get("source"),
        ev.get("source_ref"),
        ev.get("event_date"),
        ev.get("event_type"),
        ev.get("title"),
        ev.get("url"),
        ev.get("raw_excerpt"),
        ev.get("company_key"),
    ))
    return cur.fetchone()[0]

def insert_signal(cur, schema: str, sig: Dict):
    cur.execute(f"""
INSERT INTO {schema}.signals (company_key, signal_date, signal_kind, weight, source, event_id, notes)
VALUES (?, ?, ?, ?, ?, ?, ?);
""", (
        sig["company_key"],
        sig["signal_date"],
        sig["signal_kind"],
        sig["weight"],
        sig["source"],
        sig.get("event_id"),
        sig.get("notes"),
    ))

def recompute_scores(cur, schema: str, version: str = "v0_rules_2026_02"):
    cur.execute(f"""
WITH agg AS (
  SELECT company_key, SUM(weight) AS raw_score
  FROM {schema}.signals
  GROUP BY company_key
),
norm AS (
  SELECT
    company_key,
    CASE
      WHEN raw_score < 0 THEN 0
      WHEN raw_score > 100 THEN 100
      ELSE raw_score
    END AS score
  FROM agg
),
banded AS (
  SELECT
    company_key,
    score,
    CASE
      WHEN score >= 75 THEN 'Hot'
      WHEN score >= 60 THEN 'Warm'
      WHEN score >= 45 THEN 'Watchlist'
      ELSE 'Cold'
    END AS band
  FROM norm
)
MERGE {schema}.scores AS tgt
USING (SELECT company_key, score, band FROM banded) AS src
ON tgt.company_key = src.company_key
WHEN MATCHED THEN
  UPDATE SET score = src.score, band = src.band, score_version = ?, updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT (company_key, score, band, score_version, updated_at)
  VALUES (src.company_key, src.score, src.band, ?, SYSUTCDATETIME());
""", (version, version))

# -----------------------------
# ETL BORME
# -----------------------------

def fetch_borme_sumario(yyyymmdd: str) -> Dict:
    url = BORME_API.format(yyyymmdd=yyyymmdd)
    r = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
    r.raise_for_status()
    return r.json()

def etl_borme(run_date: str, schema: str, cap: int):
    if not run_date:
        run_date = utc_today_yyyymmdd()

    sumario = fetch_borme_sumario(run_date)
    items = sumario.get("data", {}).get("items", []) or []
    pdf_items = [it for it in items if it.get("url_pdf")]

    print(f"[BORME] fecha={run_date} items={len(items)} pdf_items={len(pdf_items)} cap={cap}")

    conn = sql_conn()
    cur = conn.cursor()
    ensure_schema(cur, schema)

    processed = 0
    for it in pdf_items[:cap]:
        try:
            title = it.get("titulo") or ""
            ident = it.get("identificador") or ""
            url_pdf = it.get("url_pdf")

            text = pdf_text_from_url(url_pdf)
            sigs = detect_signals(text)

            # Control de ruido: si no hay señales, saltamos
            if not (sigs["pos"] or sigs["gov"] or sigs["neg"]):
                continue

            # MVP: usamos título como proxy de “empresa”
            company_name = (title[:220] or "Empresa (BORME)").strip()
            province = ""
            ccaa = ""
            ckey = make_company_key(company_name, province, ccaa)

            upsert_company(cur, schema, ckey, company_name, province, ccaa)

            excerpt = (text[:1200] if text else "")
            ev = {
                "source": "BORME",
                "source_ref": ident,
                "event_date": datetime.strptime(run_date, "%Y%m%d").date(),
                "event_type": "borme_pdf",
                "title": title[:500],
                "url": url_pdf,              # ✅ solo URL
                "raw_excerpt": excerpt,       # ✅ extracto
                "company_key": ckey,
            }
            event_id = insert_event(cur, schema, ev)

            if sigs["pos"]:
                insert_signal(cur, schema, {
                    "company_key": ckey,
                    "signal_date": ev["event_date"],
                    "signal_kind": "explicit_or_text_relevo",
                    "weight": 40,
                    "source": "BORME",
                    "event_id": event_id,
                    "notes": "Keywords positivas (traspaso/cesión/jubilación/relevo)."
                })
            if sigs["gov"]:
                insert_signal(cur, schema, {
                    "company_key": ckey,
                    "signal_date": ev["event_date"],
                    "signal_kind": "junta_o_cambio",
                    "weight": 15,
                    "source": "BORME",
                    "event_id": event_id,
                    "notes": "Keywords de junta/cambio accionario."
                })
            if sigs["neg"]:
                insert_signal(cur, schema, {
                    "company_key": ckey,
                    "signal_date": ev["event_date"],
                    "signal_kind": "negativa_distressed",
                    "weight": -50,
                    "source": "BORME",
                    "event_id": event_id,
                    "notes": "Keywords negativas (disolución/concurso/liquidación)."
                })

            processed += 1

        except Exception as e:
            print(f"[BORME] ERROR ident={it.get('identificador')} -> {e}")

    recompute_scores(cur, schema)
    conn.commit()
    conn.close()
    print(f"[BORME] procesados={processed} | scores recalculados")

# -----------------------------
# ETL Regionales (heurístico)
# -----------------------------

def scrape_listings_basic(url: str, max_items: int = 25) -> List[Dict]:
    if BeautifulSoup is None:
        raise RuntimeError("beautifulsoup4 no disponible.")
    r = requests.get(url, timeout=30, headers={"User-Agent": "arraigo-etl/1.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links = []
    for a in soup.select("a[href]"):
        txt = (a.get_text(" ", strip=True) or "").lower()
        href = a.get("href") or ""
        if any(k in txt for k in ["oferta", "oportunidad", "negocio", "traspaso", "relevo", "comprar", "vender"]):
            if href.startswith("/"):
                href = url.rstrip("/") + href
            if href.startswith("http"):
                links.append({"title": a.get_text(" ", strip=True)[:300], "url": href})

    seen = set()
    out = []
    for l in links:
        if l["url"] in seen:
            continue
        seen.add(l["url"])
        out.append(l)
        if len(out) >= max_items:
            break
    return out

def etl_regional(schema: str):
    today = datetime.now(timezone.utc).date()

    conn = sql_conn()
    cur = conn.cursor()
    ensure_schema(cur, schema)

    inserted = 0
    for src in REGIONAL_SOURCES:
        source = src["source"]
        ccaa = src["ccaa"]
        list_url = src["list_url"]

        try:
            listings = scrape_listings_basic(list_url, max_items=25)
            print(f"[REG] source={source} ccaa={ccaa} listings={len(listings)}")

            for li in listings:
                name = li["title"] or "Negocio en traspaso"
                province = ""
                ckey = make_company_key(name, province, ccaa)
                upsert_company(cur, schema, ckey, name, province, ccaa)

                ev = {
                    "source": source,
                    "source_ref": None,
                    "event_date": today,
                    "event_type": "listing",
                    "title": name[:500],
                    "url": li["url"],  # ✅ solo URL
                    "raw_excerpt": f"Listing detectado (heurístico) en {source} ({ccaa}).",
                    "company_key": ckey
                }
                event_id = insert_event(cur, schema, ev)

                insert_signal(cur, schema, {
                    "company_key": ckey,
                    "signal_date": today,
                    "signal_kind": "explicit_listing",
                    "weight": 40,
                    "source": source,
                    "event_id": event_id,
                    "notes": "Señal explícita: listing/bolsa (heurístico)."
                })
                inserted += 1

        except Exception as e:
            print(f"[REG] ERROR source={source} -> {e}")

    recompute_scores(cur, schema)
    conn.commit()
    conn.close()
    print(f"[REG] insertados={inserted} | scores recalculados")

# -----------------------------
# Main
# -----------------------------

def main():
    parser = argparse.ArgumentParser(description="ETL Fondo Arraigo -> Azure SQL (BORME + Regionales)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("borme", help="Ingesta diaria BORME")
    p1.add_argument("--date", default="", help="AAAAMMDD (vacío = hoy UTC)")
    p1.add_argument("--cap", type=int, default=50, help="Máximo PDFs por ejecución (default 50)")

    sub.add_parser("regional", help="Ingesta regional (2–3 veces/semana)")

    args = parser.parse_args()
    schema = os.environ.get("AZURE_SQL_SCHEMA", "dbo")

    if args.cmd == "borme":
        run_date = (args.date or "").strip()
        if not run_date:
            run_date = (os.environ.get("RUN_DATE") or "").strip()
        etl_borme(run_date, schema, args.cap)

    elif args.cmd == "regional":
        etl_regional(schema)

if __name__ == "__main__":
    main()
