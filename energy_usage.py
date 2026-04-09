import csv
import io
from datetime import datetime, date
from collections import defaultdict

TARGET_KWH = 1000
FILEPATH = "/home/cason/Github/energy-tracker/Daily_Data.csv"

# Gexa Eco Saver Plus 12 - Oncor (EFL dated 02/04/2026)
ENERGY_CHARGE_PER_KWH = 0.15440   # $/kWh
USAGE_CREDIT = 125.00             # $ when >= 1000 kWh
TDU_FLAT = 4.23                   # $ per billing cycle
TDU_PER_KWH = 0.055833            # $/kWh


def calc_bill(kwh):
    """Calculate bill for a given kWh usage."""
    energy = kwh * ENERGY_CHARGE_PER_KWH
    tdu = TDU_FLAT + (kwh * TDU_PER_KWH)
    credit = USAGE_CREDIT if kwh >= TARGET_KWH else 0
    total = energy + tdu - credit
    return energy, tdu, credit, total


def get_billing_period(usage_date):
    """Return the billing period end date (the 11th) that this date falls into.
    Billing periods run from the 12th to the 11th of the next month."""
    if usage_date.day <= 11:
        return date(usage_date.year, usage_date.month, 11)
    else:
        if usage_date.month == 12:
            return date(usage_date.year + 1, 1, 11)
        else:
            return date(usage_date.year, usage_date.month + 1, 11)


def get_period_start(period_end):
    """Return the start date (12th of prior month) for a billing period."""
    if period_end.month == 1:
        return date(period_end.year - 1, 12, 12)
    else:
        return date(period_end.year, period_end.month - 1, 12)


def load_data():
    periods = defaultdict(lambda: {"days": 0, "kwh": 0.0, "last_date": None, "daily": []})
    with open(FILEPATH) as f:
        reader = csv.DictReader(f)
        for row in reader:
            usage_date = datetime.strptime(row["USAGE_DATE"], "%m/%d/%Y").date()
            kwh = float(row["USAGE_KWH"])
            period_end = get_billing_period(usage_date)
            periods[period_end]["days"] += 1
            periods[period_end]["kwh"] += kwh
            periods[period_end]["daily"].append((usage_date, kwh))
            if periods[period_end]["last_date"] is None or usage_date > periods[period_end]["last_date"]:
                periods[period_end]["last_date"] = usage_date
    return periods



def generate_report():
    """Generate the full report as a string."""
    buf = io.StringIO()
    periods = load_data()

    # Summary
    buf.write(f"{'Billing Period':<20} {'Days':<6} {'Total kWh':<12} {'Avg kWh/Day':<12} {'Est. Bill':<10}\n")
    buf.write("-" * 62 + "\n")

    for period_end in sorted(periods):
        data = periods[period_end]
        period_start = get_period_start(period_end)
        label = f"{period_start.strftime('%b %d')} - {period_end.strftime('%b %d, %Y')}"
        avg = data["kwh"] / data["days"]
        _, _, _, bill = calc_bill(data["kwh"])
        buf.write(f"{label:<20} {data['days']:<6} {data['kwh']:<12.1f} {avg:<12.1f} ${bill:<9.2f}\n")

    total_kwh = sum(d["kwh"] for d in periods.values())
    total_days = sum(d["days"] for d in periods.values())
    total_bill = sum(calc_bill(d["kwh"])[3] for d in periods.values())
    buf.write("-" * 62 + "\n")
    buf.write(f"{'Total':<20} {total_days:<6} {total_kwh:<12.1f} {total_kwh / total_days:<12.1f} ${total_bill:<9.2f}\n")

    # Target tracker for current period
    today = date.today()
    current_period_end = get_billing_period(today)
    period_start = get_period_start(current_period_end)
    total_days_in_period = (current_period_end - period_start).days + 1

    data = periods.get(current_period_end)
    if not data:
        buf.write("\nNo data for the current billing period yet.\n")
        return buf.getvalue()

    used = data["kwh"]
    recorded_days = data["days"]
    last_date = data["last_date"]
    days_remaining = (current_period_end - last_date).days
    kwh_remaining = TARGET_KWH - used
    pct = used / TARGET_KWH * 100
    days_elapsed = (last_date - period_start).days + 1
    pct_period = days_elapsed / total_days_in_period * 100
    avg_so_far = used / recorded_days

    if days_remaining > 0:
        target_daily = kwh_remaining / days_remaining
    else:
        target_daily = 0

    projected = avg_so_far * total_days_in_period

    buf.write(f"\n{'=' * 62}\n")
    buf.write(f"  1,000 kWh TARGET TRACKER  ({period_start.strftime('%b %d')} - {current_period_end.strftime('%b %d, %Y')})\n")
    buf.write(f"{'=' * 62}\n")
    buf.write(f"  Data through:          {last_date.strftime('%b %d')} ({days_elapsed}/{total_days_in_period} days, {pct_period:.0f}% of period)\n")
    buf.write(f"  Used so far:           {used:.1f} kWh ({pct:.1f}% of target)\n")
    buf.write(f"  Remaining to target:   {kwh_remaining:.1f} kWh\n")
    buf.write(f"  Days left in period:   {days_remaining}\n")
    buf.write("\n")
    buf.write(f"  Your avg daily usage:  {avg_so_far:.1f} kWh/day\n")
    if days_remaining > 0:
        buf.write(f"  Needed avg to hit 1k:  {target_daily:.1f} kWh/day\n")
        diff = target_daily - avg_so_far
        if diff > 0:
            buf.write(f"  You need to use        {diff:.1f} kWh/day MORE than your current average\n")
        else:
            buf.write(f"  You can use            {abs(diff):.1f} kWh/day LESS than your current average\n")
    buf.write("\n")
    buf.write(f"  Projected total:       {projected:.0f} kWh at current pace\n")
    if projected >= TARGET_KWH:
        buf.write(f"  Status:                ON TRACK (+{projected - TARGET_KWH:.0f} kWh over target)\n")
    else:
        buf.write(f"  Status:                UNDER TARGET ({TARGET_KWH - projected:.0f} kWh short)\n")

    bar_len = 40
    filled = int(bar_len * min(pct / 100, 1.0))
    bar = "=" * filled + "-" * (bar_len - filled)
    buf.write(f"\n  [{bar}] {pct:.1f}%\n")
    buf.write(f"  {'0 kWh':<20}{'1,000 kWh':>22}\n")

    # Bill estimate
    buf.write(f"\n{'=' * 62}\n")
    buf.write(f"  BILL ESTIMATE\n")
    buf.write(f"{'=' * 62}\n")

    energy_now, tdu_now, credit_now, bill_now = calc_bill(used)
    buf.write(f"  If period ended today ({used:.1f} kWh):\n")
    buf.write(f"    Energy charge:       ${energy_now:.2f}\n")
    buf.write(f"    TDU delivery:        ${tdu_now:.2f}\n")
    buf.write(f"    Usage credit:        -${credit_now:.2f}\n")
    buf.write(f"    Total:               ${bill_now:.2f}  ({bill_now / used * 100:.1f} cents/kWh)\n")

    _, _, _, bill_proj = calc_bill(projected)
    buf.write(f"\n  Projected bill ({projected:.0f} kWh at current pace):\n")
    buf.write(f"    Total:               ${bill_proj:.2f}  ({bill_proj / projected * 100:.1f} cents/kWh)\n")

    _, _, _, bill_at_1000 = calc_bill(1000)
    _, _, _, bill_at_999 = calc_bill(999)
    buf.write(f"\n  --- Sweet Spot Comparison ---\n")
    buf.write(f"  At 1,000 kWh:          ${bill_at_1000:.2f}  ({bill_at_1000 / 1000 * 100:.1f} cents/kWh)\n")
    buf.write(f"  At   999 kWh:          ${bill_at_999:.2f}  ({bill_at_999 / 999 * 100:.1f} cents/kWh)\n")
    buf.write(f"  1 kWh makes a          ${bill_at_999 - bill_at_1000:.2f} difference!\n")

    return buf.getvalue()


def main():
    report = generate_report()
    print(report)


if __name__ == "__main__":
    main()
