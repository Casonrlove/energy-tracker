"""Run: python3 test_energy_usage.py"""
from datetime import date, timedelta

import energy_usage as eu

D = date
# Nov 2026 bill: meter read 10/09 - 11/09, 983 kWh, $230.46
assert eu.get_period_start(D(2026, 11, 8)) == D(2026, 10, 9)
assert eu.get_period_start(D(2026, 11, 9)) == D(2026, 11, 9)
assert eu.get_period_end(D(2026, 10, 9)) == D(2026, 11, 8)
assert eu.get_period_start(D(2027, 1, 3)) == D(2026, 12, 9)
assert eu.get_period_end(D(2026, 12, 9)) == D(2027, 1, 8)
assert abs(sum(eu.load_data()[D(2026, 10, 9)].values()) - 983) < 1
assert abs(eu.calc_bill(983)[4] - 230.46) < 0.02


def stats(n_days, kwh):
    start = D(2026, 11, 9)
    return eu.get_stats(start, {start: {start + timedelta(i): kwh for i in range(n_days)}})


assert stats(1, 60.7)["status"] == "ON PACE"
assert stats(15, 20)["status"] == "BEHIND PACE"
assert stats(15, 33.5)["status"] == "CUTTING IT CLOSE"   # projects 1005, but crosses on the last day
assert stats(28, 36)["status"] == "TARGET HIT"
for s in (stats(1, 60.7), stats(15, 20), stats(28, 36)):
    assert "-" not in eu.generate_report(stats=s).split("PROGRESS")[1].split("BILL")[0], "negative progress value"
    assert eu.generate_html([("11/09/2026", 60.7)], s)
print("ok")
