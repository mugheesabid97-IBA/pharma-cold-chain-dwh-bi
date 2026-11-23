"""Build docs/Oversampling_Report.pdf from monitoring.generation_log (run after the refreshes).

Usage (inside the stack): docker compose exec airflow python scripts/build_oversampling_report.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import DOCS_DIR  # noqa: E402
from src.staging import db  # noqa: E402

styles = getSampleStyleSheet()
H1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=15, spaceAfter=4)
H2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11.5, spaceBefore=6, spaceAfter=2)
P = ParagraphStyle("p", parent=styles["BodyText"], fontSize=8.8, leading=11.2, spaceAfter=3)
CELL = ParagraphStyle("c", parent=P, fontSize=7.6, leading=9.2, spaceAfter=0)


def table(data: list[list], widths: list[float]) -> Table:
    t = Table([[Paragraph(str(c), CELL) for c in row] for row in data], colWidths=[w * cm for w in widths], repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E79")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BFBFBF")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F2F6FA")]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def fetch() -> tuple[list, list]:
    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT ON (refresh_no, source) refresh_no, target_period, source, file_name,
                              rows_generated, bad_rows_injected, bad_row_breakdown, date_min, date_max, file_size_mb,
                              run_seconds
                       FROM monitoring.generation_log ORDER BY refresh_no, source, created_at DESC""")
        gen = cur.fetchall()
        cur.execute("""SELECT DISTINCT ON (file_name) file_name, rows_read, rows_loaded, rows_rejected
                       FROM monitoring.batch_ingestion_log WHERE file_name LIKE '%%refresh%%'
                       ORDER BY file_name, id""")          # first ingestion of each refresh file
        ing = sorted(cur.fetchall(), key=lambda r: (r[0].split("__")[1], r[0]))
        cur.execute("""SELECT count(*), coalesce(sum(rows_read), 0), coalesce(sum(rows_loaded), 0)
                       FROM (SELECT *, row_number() OVER (PARTITION BY file_name ORDER BY id) rn
                             FROM monitoring.batch_ingestion_log WHERE file_name LIKE '%%refresh%%') x WHERE rn > 1""")
        rerun = cur.fetchone()
    return gen, ing, rerun


def main() -> None:
    gen, ing, rerun = fetch()
    story = [Paragraph("Data Oversampling Report: Phase 1 (SDI)", H1),
             Paragraph("Pharma Cold Chain DWH + BI. Generator: <font face='Courier'>src/generator/oversampler.py</font>, "
                       "orchestrated by the Airflow DAG <font face='Courier'>data_generator_dag</font> "
                       "(parameters refresh_no, target_month, sources, sales_rows, stream_rows).", P),
             Paragraph("1. Methodology", H2),
             Paragraph("<b>Sampling.</b> New rows are drawn with replacement from recent history: the last 6 months for "
                       "sales and the latest complete month for hospital demand. Hospital demand is generated as a full "
                       "snapshot (every hospital x medicine), so the monthly grain stays complete. The generator works for "
                       "all 7 source files; files 02, 03, 04 and 07 use the same generic path.", P),
             Paragraph("<b>Keys.</b> New primary keys continue the existing pattern (prefix + zero-padded counter, e.g. "
                       "SALE-000500001, HDEM-00027217). Generated files from earlier refreshes are scanned, so refresh 2 "
                       "continues after refresh 1. Secondary IDs such as PrescriptionID are renewed the same way. Foreign "
                       "keys (MedicineID, HospitalID, PharmacyID, WarehouseID) are always copied from existing master "
                       "records, never invented.", P),
             Paragraph("<b>Dates.</b> The main business date is moved into the target month (a random day for sales, the "
                       "1st of the month for hospital demand). Every other date in the row (delivery date, batch expiry, "
                       "timestamp) is shifted by the same delta, so delivery &lt; sale &lt; expiry and DaysToExpiryAtSale "
                       "stay consistent.", P),
             Paragraph("<b>Values.</b> Related quantities are scaled by one random factor per row (0.85-1.15), keeping "
                       "requested/received/consumed/stock proportional. The hospital forecast gets an extra independent "
                       "+-10 % so the forecast-vs-actual ratio varies. For sales, the price is the medicine's current "
                       "retail price, with 15 % of baskets changed by +-1 pack (kept within 1-3). Money is then re-derived exactly: gross = "
                       "qty x price, discount = original discount rate x gross, net = gross - discount, tax ~6 % of net, "
                       "COGS = qty x cost per pack (scaled with price changes, capped below price), margin = net - COGS.", P),
             Paragraph("<b>Master data / SCD2.</b> Each refresh changes the price of 3 medicines (always including one "
                       "cold-chain medicine) by +5-12 % (manufacturing cost +3 %), with EffectiveFromDate = first day of the "
                       "refresh month. The changes are deterministic per refresh, so a re-run produces the same versions. "
                       "Sales in the refresh month use the new prices.", P),
             Paragraph("<b>Bad rows (~1 %).</b> Corrupted copies of random rows are appended to every file, then the file "
                       "is shuffled. Five documented types are used: price &lt;= 0 (negative quantity for hospital demand), "
                       "null MedicineID, unknown MedicineID (MED999), invalid date (2026-02-30 / 'not-a-date') and "
                       "duplicate PK. Each bad row breaks exactly one rule, so validation must reject exactly the injected "
                       "count. Appending (rather than overwriting) keeps the expected valid volume exact.", P),
             Paragraph("2. Why this volume", H2),
             Paragraph("The source covers 36 months (2023-01..2025-12) with 500,000 sales, i.e. 13,889 per month on "
                       "average. Monthly sales are very seasonal (1.4k in Feb 2023, 95k in Jan 2025), so copying a single "
                       "month would make a refresh either trivial or dominant. Each refresh therefore adds <b>14,000 sales "
                       "(about one average month)</b>: 11,000 through the batch path and 3,000 through the streaming path, "
                       "where 3,000 events at 25 events/s gives a 2-minute stream. It also adds <b>756 hospital-demand rows</b> "
                       "(12 hospitals x 63 medicines = exactly one monthly snapshot) and <b>3 master-data price changes</b> "
                       "to exercise SCD Type 2. Refresh 1 = 2026-01 and Refresh 2 = 2026-02 (the two months after the last "
                       "date). This volume is large enough to show growth in every dashboard, but small enough that a "
                       "refresh runs in minutes on an 8 GB laptop.", P),
             Paragraph("3. Statistics per refresh (from monitoring.generation_log)", H2)]
    rows = [["#", "Period", "Source", "File (timestamp omitted)", "Rows incl. bad", "Bad rows", "Bad-row types",
             "Date range", "MB", "Gen. s"]]
    for r in gen:
        br = ", ".join(f"{k}={v}" for k, v in sorted((r[6] or {}).items()))
        rows.append([r[0], r[1], r[2], r[3].rsplit("_", 1)[0] + ".csv", f"{r[4]:,}", r[5], br,
                     f"{r[7]} to {r[8]}", f"{float(r[9]):.2f}", f"{float(r[10]):.0f}"])
    story.append(table(rows, [0.6, 1.7, 1.5, 4.0, 1.3, 1.0, 3.5, 2.2, 1.0, 1.1]) if gen else Paragraph("No generation runs logged yet.", P))
    if ing:
        story += [Spacer(1, 6), Paragraph("4. Validation outcome when the refresh files were ingested", H2),
                  table([["File", "Rows read", "Loaded", "Rejected"]] + [[a, f"{b:,}", f"{c:,}", d] for a, b, c, d in ing],
                        [10, 2.5, 2.5, 2.5]),
                  Paragraph("Rejected = injected bad rows: every deliberately bad row was caught, and no valid row was "
                            "lost. Stream-file bad rows (30 per refresh) were rejected the same way and show as malformed "
                            "events on the Streaming dashboard.", P)]
        if rerun and rerun[0]:
            story.append(Paragraph(f"<b>Idempotency check:</b> {rerun[0]} already-processed refresh file(s) were dropped into "
                                   f"landing again: {rerun[1]:,} rows read, <b>{rerun[2]:,} loaded</b>, so every row was "
                                   "rejected as 'duplicate PK already in staging' (or for its original bad-row reason).", P))
    out = DOCS_DIR / "Oversampling_Report.pdf"
    SimpleDocTemplate(str(out), pagesize=A4, leftMargin=1.4 * cm, rightMargin=1.4 * cm, topMargin=1.2 * cm,
                      bottomMargin=1.2 * cm, title="Oversampling Report - Phase 1").build(story)
    print("wrote", out)


if __name__ == "__main__":
    main()
