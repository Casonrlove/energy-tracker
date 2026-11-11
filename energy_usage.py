import csv
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path

TARGET_KWH = 1000
FILEPATH = str(Path(__file__).resolve().parent / "Daily_Data.csv")

# --- CALIBRATION KNOB: check against the "Meter Read Date" on every bill. ---
# Day of month the meter is read. Usage ON this day belongs to the NEW cycle.
# Nov 2026 bill: read 10/09 - 11/09 => billed usage days 10/09..11/08 (983 kWh).
# Oncor moves the read a day or two month to month (Apr 2026 it was the 10th/11th),
# so the period end is an ESTIMATE; SAFETY_DAYS is the buffer for that drift.
CYCLE_START_DAY = 9
SAFETY_DAYS = 2  # aim to cross 1,000 kWh this many days before the estimated read

# Gexa Eco Saver Plus 12 - Oncor (fitted to Nov 2026 bill: 983 kWh = $230.46)
ENERGY_CHARGE_PER_KWH = 0.15440   # $/kWh
USAGE_CREDIT = 125.00             # $ when >= 1000 kWh
TDU_FLAT = 4.23                   # $ per billing cycle (assumed; one bill can't split flat vs per-kWh)
TDU_PER_KWH = 0.068494            # $/kWh (($71.56 - 4.23) / 983 kWh)
TAX_RATE = 0.03188                # PUC assessment + gross receipts + city sales tax ($7.12 / $223.34)

GREEN, AMBER, RED, MUTED = "#2e9e5b", "#d9902b", "#d9534f", "#888888"


def calc_bill(kwh):
    """Calculate bill for a given kWh usage."""
    energy = kwh * ENERGY_CHARGE_PER_KWH
    tdu = TDU_FLAT + (kwh * TDU_PER_KWH)
    credit = USAGE_CREDIT if kwh >= TARGET_KWH else 0
    subtotal = energy + tdu - credit
    taxes = subtotal * TAX_RATE
    total = subtotal + taxes
    return energy, tdu, credit, taxes, total


def get_period_start(d):
    """First usage day of the billing cycle that date d falls into."""
    if d.day >= CYCLE_START_DAY:
        return d.replace(day=CYCLE_START_DAY)
    return (d.replace(day=1) - timedelta(days=1)).replace(day=CYCLE_START_DAY)


def get_period_end(start):
    """Last usage day of the cycle (estimated: day before the next meter read)."""
    next_start = (start.replace(day=1) + timedelta(days=32)).replace(day=CYCLE_START_DAY)
    return next_start - timedelta(days=1)


def load_data():
    """Return {period_start: {usage_date: kwh}}."""
    periods = defaultdict(dict)
    with open(FILEPATH) as f:
        for row in csv.DictReader(f):
            d = datetime.strptime(row["USAGE_DATE"], "%m/%d/%Y").date()
            periods[get_period_start(d)][d] = float(row["USAGE_KWH"])
    return periods


def get_stats(today=None, periods=None):
    today = today or date.today()
    periods = periods or load_data()

    # Current period if it has data, otherwise the most recent one
    start = get_period_start(today)
    if start not in periods:
        start = max(periods)
    end = get_period_end(start)
    daily = periods[start]

    s = {"start": start, "end": end, "total_days": (end - start).days + 1}
    s["used"] = sum(daily.values())
    s["last_date"] = max(daily)
    s["days_elapsed"] = (s["last_date"] - start).days + 1
    s["days_left"] = (end - s["last_date"]).days
    s["remaining"] = TARGET_KWH - s["used"]
    s["pct"] = s["used"] / TARGET_KWH * 100
    s["avg"] = s["used"] / len(daily)
    s["projected"] = s["avg"] * s["total_days"]
    s["safe_end"] = end - timedelta(days=SAFETY_DAYS)
    # Pace needed to cross the target SAFETY_DAYS before the estimated read
    s["needed"] = s["remaining"] / max(s["days_left"] - SAFETY_DAYS, 1)
    s["cross_date"] = s["last_date"] + timedelta(days=math.ceil(max(s["remaining"], 0) / s["avg"]))

    if s["remaining"] <= 0:
        s["status"], s["color"] = "TARGET HIT", GREEN
        s["detail"] = f"{-s['remaining']:.0f} kWh over 1,000. Credit earned if the meter is read on or after {end + timedelta(days=1):%b %d}."
    elif s["cross_date"] <= s["safe_end"]:
        s["status"], s["color"] = "ON PACE", GREEN
        s["detail"] = f"At {s['avg']:.1f} kWh/day you cross 1,000 around {s['cross_date']:%b %d}."
    elif s["projected"] >= TARGET_KWH:
        s["status"], s["color"] = "CUTTING IT CLOSE", AMBER
        s["detail"] = (f"You'd cross 1,000 around {s['cross_date']:%b %d}, inside the {SAFETY_DAYS}-day "
                       f"meter-read buffer. Use {s['needed']:.1f} kWh/day to be safe.")
    else:
        s["status"], s["color"] = "BEHIND PACE", RED
        s["detail"] = f"Use {s['needed']:.1f} kWh/day (now {s['avg']:.1f}) to cross 1,000 by {s['safe_end']:%b %d}."
    return s


def build_sections(s, new_rows=()):
    """One structure, rendered as both text and HTML: [(title, [(label, value)])]."""
    money = lambda total, kwh: f"${total:.2f} ({total / kwh * 100:.1f}¢/kWh)"
    energy, tdu, credit, taxes, bill_now = calc_bill(s["used"])
    bill_proj = calc_bill(s["projected"])[4]
    bill_1000, bill_999 = calc_bill(1000)[4], calc_bill(999)[4]

    progress = [
        ("Data through", f"{s['last_date']:%b %d} (day {s['days_elapsed']} of {s['total_days']})"),
        ("Remaining", f"{max(s['remaining'], 0):.1f} kWh"),
        ("Days left (est.)", str(s["days_left"])),
        ("Avg so far", f"{s['avg']:.1f} kWh/day"),
    ]
    if s["remaining"] > 0:
        progress.append((f"Needed by {s['safe_end']:%b %d}", f"{s['needed']:.1f} kWh/day"))
    progress.append(("Projected total", f"{s['projected']:.0f} kWh"))

    sections = []
    if new_rows:
        sections.append(("New data", [(d, f"{k:.1f} kWh") for d, k in new_rows]))
    sections += [
        ("Progress", progress),
        ("Bill if period ended today", [
            ("Energy charge", f"${energy:.2f}"),
            ("TDU delivery", f"${tdu:.2f}"),
            ("Usage credit", f"-${credit:.2f}"),
            ("Taxes/fees", f"${taxes:.2f}"),
            ("Total", money(bill_now, s["used"])),
        ]),
        ("Projected bill", [(f"At {s['projected']:.0f} kWh", money(bill_proj, s["projected"]))]),
        ("Sweet spot", [
            ("At 1,000 kWh", money(bill_1000, 1000)),
            ("At 999 kWh", money(bill_999, 999)),
            ("That 1 kWh is worth", f"${bill_999 - bill_1000:.2f}"),
        ]),
    ]
    return sections


def _note():
    return (f"Period end is an estimate: Oncor's meter-read date shifts a day or two each month, "
            f"so aim to pass 1,000 kWh {SAFETY_DAYS}+ days early.")


def generate_report(new_rows=(), stats=None):
    """Plain-text report (terminal, log, and email fallback). Lines stay short for phones."""
    s = stats or get_stats()
    lines = [
        "1,000 kWh TRACKER",
        f"{s['start']:%b %d} - {s['end']:%b %d, %Y} (est.)",
        "",
        f"{s['used']:.1f} kWh used ({s['pct']:.1f}%)",
        f"{s['status']}: {s['detail']}",
    ]
    for title, rows in build_sections(s, new_rows):
        lines += ["", title.upper()]
        lines += [f"  {label}: {value}" for label, value in rows]
    lines += ["", _note()]
    return "\n".join(lines) + "\n"


def generate_html(new_rows=(), stats=None):
    """Mobile-first HTML email. No background/text colours set, so dark mode just works."""
    s = stats or get_stats()
    out = [
        '<div style="max-width:480px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'
        "'Segoe UI',Roboto,Helvetica,Arial,sans-serif;font-size:16px;line-height:1.4\">",
        f'<div style="font-size:13px;color:{MUTED};letter-spacing:.06em;text-transform:uppercase">'
        f"1,000 kWh tracker &middot; {s['start']:%b %d} &ndash; {s['end']:%b %d} (est.)</div>",
        f'<div style="font-size:44px;font-weight:700;line-height:1.1;margin-top:6px">{s["used"]:.1f}'
        f'<span style="font-size:18px;font-weight:400;color:{MUTED}"> / 1,000 kWh</span></div>',
        '<div style="background:rgba(136,136,136,.3);border-radius:6px;height:12px;margin:12px 0">'
        f'<div style="background:{s["color"]};border-radius:6px;height:12px;width:{min(s["pct"], 100):.1f}%"></div></div>',
        f'<div style="display:inline-block;background:{s["color"]};color:#ffffff;font-size:13px;font-weight:700;'
        f'letter-spacing:.04em;padding:4px 10px;border-radius:999px">{s["status"]}</div>',
        f'<div style="margin-top:8px">{escape(s["detail"])}</div>',
    ]
    for title, rows in build_sections(s, new_rows):
        out.append(f'<div style="font-size:13px;color:{MUTED};letter-spacing:.06em;text-transform:uppercase;'
                   f'margin-top:24px;padding-bottom:4px;border-bottom:1px solid rgba(136,136,136,.4)">{escape(title)}</div>')
        out.append('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">')
        for label, value in rows:
            out.append(f'<tr><td style="padding:7px 0;color:{MUTED}">{escape(label)}</td>'
                       f'<td align="right" style="padding:7px 0;font-weight:600">{escape(value)}</td></tr>')
        out.append("</table>")
    out.append(f'<div style="font-size:12px;color:{MUTED};margin-top:24px">{escape(_note())}</div></div>')
    return "\n".join(out)


def main():
    print(generate_report())


if __name__ == "__main__":
    main()
