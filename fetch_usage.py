import asyncio
import csv
import json
import logging
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path

import aiohttp
import pytz

from smart_meter_texas import Account, Client, ClientSSLContext
from energy_usage import generate_report

CONFIG_PATH = Path("~/.smt_config.json").expanduser()
CSV_PATH = Path("~/Github/energy-tracker/Daily_Data.csv").expanduser()
TIMEZONE = pytz.timezone("America/Chicago")

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def get_existing_dates():
    """Return set of USAGE_DATE strings already in the CSV."""
    dates = set()
    if CSV_PATH.exists():
        with open(CSV_PATH) as f:
            reader = csv.DictReader(f)
            for row in reader:
                dates.add(row["USAGE_DATE"])
    return dates


def append_row(esiid, usage_date, reading_kwh):
    """Append a row to the CSV."""
    now = datetime.now(TIMEZONE).strftime("%m/%d/%Y %H:%M:%S")
    row = {
        "ESIID": f"'{esiid}",
        "USAGE_DATE": usage_date,
        "REVISION_DATE": now,
        "START_READING": "",
        "END_READING": "",
        "USAGE_KWH": f"{reading_kwh:.3f}",
    }
    file_exists = CSV_PATH.exists() and CSV_PATH.stat().st_size > 0
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def send_email(cfg, subject, body):
    """Send email via Gmail SMTP."""
    gmail_user = cfg.get("gmail_user", "")
    gmail_pw = cfg.get("gmail_app_password", "")
    email_to = cfg.get("email_to", "")

    if not gmail_pw or gmail_pw == "YOUR_APP_PASSWORD":
        log.warning("Gmail app password not configured, skipping email.")
        return

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = gmail_user
    msg["To"] = email_to

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_pw)
        server.sendmail(gmail_user, email_to, msg.as_string())

    log.info(f"Email sent to {email_to}.")


async def main():
    cfg = load_config()
    if cfg["username"] == "YOUR_SMT_USERNAME":
        print("Please edit ~/.smt_config.json with your Smart Meter Texas credentials.")
        sys.exit(1)

    existing_dates = get_existing_dates()
    ssl_ctx = await ClientSSLContext().get_ssl_context()
    new_rows = []

    async with aiohttp.ClientSession() as session:
        account = Account(cfg["username"], cfg["password"])
        client = Client(session, account, ssl_ctx)

        log.info("Authenticating with Smart Meter Texas...")
        await client.authenticate()
        log.info("Authenticated successfully.")

        meters = await account.fetch_meters(client)
        log.info(f"Found {len(meters)} meter(s).")

        for meter in meters:
            esiid = meter.esiid
            log.info(f"Fetching data for ESIID {esiid}...")

            for days_back in range(1, 8):
                target_date = datetime.now(TIMEZONE).date() - timedelta(days=days_back)
                date_str = target_date.strftime("%m/%d/%Y")

                if date_str in existing_dates:
                    log.info(f"  {date_str} already in CSV, skipping.")
                    continue

                try:
                    intervals = await meter.get_15min(client, prevdays=days_back)
                    if intervals:
                        total_kwh = sum(float(v) for _, v in intervals if v)
                        if total_kwh > 0:
                            append_row(esiid, date_str, total_kwh)
                            new_rows.append((date_str, total_kwh))
                            log.info(f"  Added {date_str}: {total_kwh:.3f} kWh")
                        else:
                            log.info(f"  {date_str}: no usage data available yet.")
                    else:
                        log.info(f"  {date_str}: no interval data returned.")
                except Exception as e:
                    log.warning(f"  {date_str}: failed to fetch ({e})")

    # Generate report and email it
    report = generate_report()
    today_str = datetime.now(TIMEZONE).strftime("%m/%d/%Y")

    if new_rows:
        added_summary = "\n".join(f"  {d}: {k:.1f} kWh" for d, k in new_rows)
        email_body = f"New data added:\n{added_summary}\n\n{report}"
    else:
        email_body = f"No new data today.\n\n{report}"

    send_email(cfg, f"Energy Report - {today_str}", email_body)
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
