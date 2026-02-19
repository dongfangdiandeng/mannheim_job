import argparse
import hashlib
import json
import html
import os
import re
import sqlite3
import smtplib
import sys
from datetime import datetime, timezone
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.uni-mannheim.de/en/about/working-at-the-university-of-mannheim/employment-opportunities/"
ZEW_BASE_URL = "https://www.zew.de/en/career/job-offers"
ZEW_API_URL = "https://jobs.b-ite.com/api/v1/postings/search"
ZEW_LISTING_JS_URL = "https://cs-assets.b-ite.com/zew/jobs-api/mitarbeiter-en.js"
ZEW_DEFAULT_API_KEY = "be2895b17849cf534866c1db3bc9208ff29f8cc6"
ZEW_CATEGORIES = [
    "mitarbeiter",
    "studentische_hilfskraefte",
    "praktikum",
    "ausbildung",
]
DB_FILE = "jobs.db"
EMAIL_CONFIG_FILE = "email_config.json"
UA = "mannheim-jobs-bot/1.0 (+personal use)"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def parse_deadline_date(deadline: str) -> Optional[datetime]:
    text = normalize_ws(deadline)
    if not text:
        return None

    # Keep only the first candidate segment in noisy strings.
    text = text.split("|")[0].strip()
    patterns = [
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d %B %Y",
        "%d %b %Y",
    ]
    for fmt in patterns:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def content_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def stable_job_key(title: str, org: str, deadline: str, url: str) -> str:
    base = "|".join([
        normalize_ws(title).lower(),
        normalize_ws(org).lower(),
        normalize_ws(deadline).lower(),
        normalize_ws(url).lower(),
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_key TEXT NOT NULL UNIQUE,
            title TEXT,
            org TEXT,
            field TEXT,
            remuneration TEXT,
            deadline TEXT,
            url TEXT,
            source_type TEXT NOT NULL,
            raw_text TEXT,
            content_hash TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            UNIQUE(job_key, event_type)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS title_translations (
            source_text TEXT PRIMARY KEY,
            translated_text TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def fetch_html(session: requests.Session, url: str, timeout: int = 20) -> str:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def extract_detail_text(session: requests.Session, url: str) -> Tuple[str, str]:
    """
    Return (source_type, raw_text)
    source_type in {pdf, html, table_only}
    """
    if not url:
        return "table_only", ""

    lower = url.lower()
    if lower.endswith(".pdf"):
        return "pdf", ""

    try:
        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")
        text = normalize_ws(soup.get_text(" ", strip=True))
        return "html", text
    except Exception:
        return "html", ""


def parse_open_positions(page_html: str, base_url: str, session: requests.Session) -> List[Dict[str, str]]:
    soup = BeautifulSoup(page_html, "html.parser")

    open_positions_header = None
    for h in soup.find_all(["h2", "h3", "h4"]):
        if "open positions" in h.get_text(" ", strip=True).lower():
            open_positions_header = h
            break

    # Site markup varies over time. If the expected section heading is absent,
    # fall back to the first table on the page.
    table = open_positions_header.find_next("table") if open_positions_header else soup.find("table")
    if not table:
        raise RuntimeError("Could not find a job table on the page.")

    jobs: List[Dict[str, str]] = []
    rows = table.find_all("tr")

    for row in rows[1:]:
        cols = row.find_all(["td", "th"])
        if len(cols) < 5:
            continue

        title_link = cols[0].find("a")
        title = normalize_ws(cols[0].get_text(" ", strip=True))
        url = ""
        if title_link and title_link.get("href"):
            url = urljoin(base_url, title_link.get("href"))

        org = normalize_ws(cols[1].get_text(" ", strip=True)) if len(cols) > 1 else ""
        field = normalize_ws(cols[2].get_text(" ", strip=True)) if len(cols) > 2 else ""
        remuneration = normalize_ws(cols[3].get_text(" ", strip=True)) if len(cols) > 3 else ""
        deadline = normalize_ws(cols[4].get_text(" ", strip=True)) if len(cols) > 4 else ""

        source_type, detail_text = extract_detail_text(session, url)

        table_text = normalize_ws(" | ".join([title, org, field, remuneration, deadline]))
        raw_text = detail_text if detail_text else table_text

        job = {
            "title": title,
            "url": url,
            "org": org,
            "field": field,
            "remuneration": remuneration,
            "deadline": deadline,
            "source_type": source_type if source_type else "table_only",
            "raw_text": raw_text,
        }
        jobs.append(job)

    return jobs


def zew_api_key(session: requests.Session) -> str:
    try:
        js = fetch_html(session, ZEW_LISTING_JS_URL)
        m = re.search(r'key:\s*"([a-f0-9]{32,64})"', js, flags=re.I)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ZEW_DEFAULT_API_KEY


def fetch_zew_jobs(session: requests.Session) -> List[Dict[str, str]]:
    key = zew_api_key(session)
    jobs: List[Dict[str, str]] = []

    for cat in ZEW_CATEGORIES:
        payload = {
            "key": key,
            "offset": 0,
            "limit": 100,
            "channel": 0,
            "locale": "en",
            "sort": {"by": "startsOn", "order": "desc"},
            "filter": {
                "custom.zuordnung_homepage": {"in": [cat]},
                "locale": {"in": ["*"]},
            },
        }

        resp = session.post(ZEW_API_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        postings = data.get("jobPostings") or []

        for p in postings:
            title = normalize_ws(str(p.get("title", "")))
            url = normalize_ws(str(p.get("url", "")))
            deadline = normalize_ws(str(p.get("endsOn", "")))
            if len(deadline) >= 10 and deadline[4] == "-" and deadline[7] == "-":
                deadline = deadline[:10]

            org = normalize_ws(str((p.get("employer") or {}).get("name", ""))) or "ZEW"
            field = normalize_ws(str(p.get("employmentType", ""))) or cat
            raw_text = normalize_ws(
                " | ".join(
                    [
                        title,
                        normalize_ws(str(p.get("identification", ""))),
                        deadline,
                        normalize_ws(str(p.get("anr", ""))),
                    ]
                )
            )

            jobs.append(
                {
                    "title": title,
                    "url": url,
                    "org": org,
                    "field": field,
                    "remuneration": "",
                    "deadline": deadline,
                    "source_type": "api",
                    "raw_text": raw_text,
                }
            )

    # Deduplicate postings that appear in multiple categories.
    uniq: Dict[str, Dict[str, str]] = {}
    for j in jobs:
        key = stable_job_key(j["title"], j["org"], j["deadline"], j["url"])
        uniq[key] = j
    return list(uniq.values())


def upsert_jobs(
    conn: sqlite3.Connection, jobs: List[Dict[str, str]]
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], int]:
    now = utc_now_iso()
    new_jobs: List[Dict[str, str]] = []
    updated_jobs: List[Dict[str, str]] = []
    removed_n = 0

    prev_active_keys = {row[0] for row in conn.execute("SELECT job_key FROM jobs WHERE is_active = 1")}
    seen_keys = set()

    for job in jobs:
        jkey = stable_job_key(job["title"], job["org"], job["deadline"], job["url"])
        seen_keys.add(jkey)
        raw = job.get("raw_text", "")
        chash = content_hash(raw)

        cur = conn.execute(
            "SELECT content_hash FROM jobs WHERE job_key = ?",
            (jkey,),
        )
        row = cur.fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_key, title, org, field, remuneration, deadline, url,
                    source_type, raw_text, content_hash,
                    first_seen_at, last_seen_at, updated_at, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    jkey,
                    job["title"],
                    job["org"],
                    job["field"],
                    job["remuneration"],
                    job["deadline"],
                    job["url"],
                    job["source_type"],
                    raw,
                    chash,
                    now,
                    now,
                    now,
                ),
            )
            item = {**job, "job_key": jkey}
            new_jobs.append(item)
        else:
            old_hash = row[0] or ""
            if old_hash != chash:
                conn.execute(
                    """
                    UPDATE jobs
                    SET title = ?, org = ?, field = ?, remuneration = ?, deadline = ?,
                        url = ?, source_type = ?, raw_text = ?, content_hash = ?,
                        last_seen_at = ?, updated_at = ?, is_active = 1
                    WHERE job_key = ?
                    """,
                    (
                        job["title"],
                        job["org"],
                        job["field"],
                        job["remuneration"],
                        job["deadline"],
                        job["url"],
                        job["source_type"],
                        raw,
                        chash,
                        now,
                        now,
                        jkey,
                    ),
                )
                item = {**job, "job_key": jkey}
                updated_jobs.append(item)
            else:
                conn.execute(
                    "UPDATE jobs SET last_seen_at = ?, is_active = 1 WHERE job_key = ?",
                    (now, jkey),
                )

    removed_keys = prev_active_keys - seen_keys
    if removed_keys:
        removed_n = len(removed_keys)
        placeholders = ",".join(["?"] * len(removed_keys))
        conn.execute(
            f"UPDATE jobs SET is_active = 0 WHERE job_key IN ({placeholders})",
            tuple(removed_keys),
        )

    conn.commit()
    return new_jobs, updated_jobs, removed_n


def get_active_jobs(conn: sqlite3.Connection) -> List[Dict[str, str]]:
    rows = conn.execute(
        """
        SELECT job_key, title, org, field, remuneration, deadline, url, source_type, raw_text
        FROM jobs
        WHERE is_active = 1
        ORDER BY deadline, title
        """
    ).fetchall()
    result: List[Dict[str, str]] = []
    for row in rows:
        result.append(
            {
                "job_key": row[0],
                "title": row[1] or "",
                "org": row[2] or "",
                "field": row[3] or "",
                "remuneration": row[4] or "",
                "deadline": row[5] or "",
                "url": row[6] or "",
                "source_type": row[7] or "table_only",
                "raw_text": row[8] or "",
            }
        )
    return result


def attach_english_titles(conn: sqlite3.Connection, jobs: List[Dict[str, str]], enabled: bool) -> List[Dict[str, str]]:
    if not enabled:
        return [{**j, "title_en": ""} for j in jobs]

    try:
        from deep_translator import GoogleTranslator  # type: ignore
    except Exception:
        print("WARN: deep-translator not installed, skip title translation.")
        return [{**j, "title_en": ""} for j in jobs]

    translator = GoogleTranslator(source="auto", target="en")
    out: List[Dict[str, str]] = []
    local_cache: Dict[str, str] = {}

    for job in jobs:
        title = normalize_ws(job.get("title", ""))
        title_en = ""

        if title:
            if title in local_cache:
                title_en = local_cache[title]
            else:
                row = conn.execute(
                    "SELECT translated_text FROM title_translations WHERE source_text = ?",
                    (title,),
                ).fetchone()
                if row and row[0]:
                    title_en = row[0]
                else:
                    try:
                        title_en = normalize_ws(translator.translate(title) or "")
                    except Exception:
                        title_en = ""
                    if title_en:
                        conn.execute(
                            """
                            INSERT INTO title_translations (source_text, translated_text, updated_at)
                            VALUES (?, ?, ?)
                            ON CONFLICT(source_text) DO UPDATE SET
                                translated_text = excluded.translated_text,
                                updated_at = excluded.updated_at
                            """,
                            (title, title_en, utc_now_iso()),
                        )
                local_cache[title] = title_en

        out.append({**job, "title_en": title_en})

    conn.commit()
    return out


def unsent_events(conn: sqlite3.Connection, jobs: List[Dict[str, str]], event_type: str) -> List[Dict[str, str]]:
    pending: List[Dict[str, str]] = []
    for job in jobs:
        cur = conn.execute(
            "SELECT 1 FROM notifications WHERE job_key = ? AND event_type = ?",
            (job["job_key"], event_type),
        )
        if cur.fetchone() is None:
            pending.append(job)
    return pending


def mark_sent(conn: sqlite3.Connection, jobs: List[Dict[str, str]], event_type: str) -> None:
    now = utc_now_iso()
    for job in jobs:
        conn.execute(
            "INSERT OR IGNORE INTO notifications (job_key, event_type, sent_at) VALUES (?, ?, ?)",
            (job["job_key"], event_type, now),
        )
    conn.commit()


def build_email_content(
    all_jobs: List[Dict[str, str]],
    new_jobs: List[Dict[str, str]],
    updated_jobs: List[Dict[str, str]],
) -> Tuple[str, str]:
    new_keys = {j.get("job_key", "") for j in new_jobs}

    def sort_key(job: Dict[str, str]) -> Tuple[int, datetime, str]:
        dt = parse_deadline_date(job.get("deadline", ""))
        if dt is None:
            return (1, datetime.max, job.get("title", ""))
        return (0, dt, job.get("title", ""))

    sorted_jobs = sorted(all_jobs, key=sort_key)

    text_lines: List[str] = []
    text_lines.append("Jobs Alert")
    text_lines.append("")
    text_lines.append(f"Total active jobs: {len(all_jobs)}")
    text_lines.append(f"New jobs in this run: {len(new_jobs)}")
    text_lines.append(f"Updated jobs in this run: {len(updated_jobs)}")
    text_lines.append("")
    text_lines.append(f"[ALL ACTIVE JOBS] {len(all_jobs)}")
    for j in sorted_jobs:
        tag = "NEW" if j.get("job_key", "") in new_keys else "OLD"
        text_lines.append(f"- [{tag}][{j.get('source_type', '').upper()}] {j.get('title', 'N/A')}")
        if j.get("title_en"):
            text_lines.append(f"  Title (EN): {j.get('title_en')}")
        text_lines.append(f"  Org: {j.get('org', 'N/A')}")
        text_lines.append(f"  Deadline: {j.get('deadline', 'N/A')}")
        text_lines.append(f"  Link: {j.get('url', 'N/A')}")
    text_lines.append("")
    text_lines.append(f"Generated at (UTC): {utc_now_iso()}")

    cards: List[str] = []
    for j in sorted_jobs:
        tag = "NEW" if j.get("job_key", "") in new_keys else "OLD"
        title = html.escape(str(j.get("title", "N/A")))
        title_en_raw = normalize_ws(str(j.get("title_en", "")))
        title_en = html.escape(title_en_raw)
        org = html.escape(str(j.get("org", "N/A")))
        deadline = html.escape(str(j.get("deadline", "N/A")))
        source_type = html.escape(str(j.get("source_type", "")).upper())
        url = str(j.get("url", "")).strip()
        link_html = (
            f'<a href="{html.escape(url)}" '
            "style='display:inline-block;padding:6px 10px;border-radius:8px;background:#0f172a;color:#ffffff;text-decoration:none;font-size:12px;'>Open</a>"
            if url
            else "N/A"
        )
        badge_color = "#0ea5e9" if tag == "NEW" else "#64748b"
        en_line = (
            f"<div style='font-size:13px;line-height:1.5;color:#334155;margin-top:-4px;margin-bottom:8px;'><i>EN: {title_en}</i></div>"
            if title_en_raw and title_en_raw.lower() != normalize_ws(str(j.get('title', ''))).lower()
            else ""
        )
        card_html = (
            "<div style='border:1px solid #e2e8f0;border-radius:10px;padding:12px;margin:10px 0;background:#ffffff;'>"
            f"<div style='margin-bottom:8px;'><span style='display:inline-block;padding:2px 8px;border-radius:999px;background:{badge_color};color:#fff;font-size:12px'>{tag}</span></div>"
            f"<div style='font-size:15px;line-height:1.4;font-weight:600;margin-bottom:8px;word-break:break-word;'>{title}</div>"
            f"{en_line}"
            f"<div style='font-size:13px;color:#334155;line-height:1.6;'><b>Institution:</b> {org}</div>"
            f"<div style='font-size:13px;color:#334155;line-height:1.6;'><b>Deadline:</b> {deadline}</div>"
            f"<div style='font-size:13px;color:#334155;line-height:1.6;'><b>Type:</b> {source_type}</div>"
            f"<div style='margin-top:10px;'>{link_html}</div>"
            "</div>"
        )
        cards.append(card_html)

    html_body = f"""
<!doctype html>
<html>
  <body style="font-family:Segoe UI,Arial,sans-serif;background:#f8fafc;color:#0f172a;padding:16px;">
    <div style="max-width:980px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
      <div style="padding:16px 20px;background:#0f172a;color:#fff;">
        <h2 style="margin:0;font-size:20px;">Jobs Alert</h2>
      </div>
      <div style="padding:16px 20px;">
        <p style="margin:0 0 10px 0;">Total active jobs: <b>{len(all_jobs)}</b> | New: <b>{len(new_jobs)}</b> | Updated: <b>{len(updated_jobs)}</b></p>
        <div style="font-size:12px;color:#64748b;margin-bottom:8px;">All active jobs</div>
        {''.join(cards)}
        <p style="margin:12px 0 0 0;color:#475569;font-size:12px;">Generated at (UTC): {html.escape(utc_now_iso())}</p>
      </div>
    </div>
  </body>
</html>
""".strip()

    return "\n".join(text_lines), html_body


def load_email_config(path: str) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("Email config must be a JSON object.")
    return data


def parse_email_list(raw: object) -> List[str]:
    if isinstance(raw, str):
        return [x.strip() for x in raw.split(",") if x.strip()]
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return []


def send_email_smtp(subject: str, text_body: str, html_body: str, cfg: Dict[str, object], to_emails: List[str]) -> None:
    smtp_host = str(cfg.get("smtp_host", "")).strip()
    smtp_port = int(cfg.get("smtp_port", 587))
    smtp_user = str(cfg.get("smtp_user", "")).strip()
    smtp_pass = str(cfg.get("smtp_pass", "")).strip()
    smtp_ssl = bool(cfg.get("smtp_ssl", False))
    smtp_starttls = bool(cfg.get("smtp_starttls", not smtp_ssl))

    required = {
        "smtp_host": smtp_host,
        "smtp_user": smtp_user,
        "smtp_pass": smtp_pass,
        "to_emails": ",".join(to_emails),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing SMTP config values: {', '.join(missing)}")

    msg = MIMEMultipart("alternative")
    msg["From"] = smtp_user
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = Header(subject, "utf-8")
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if smtp_ssl:
        server_ctx = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)
    else:
        server_ctx = smtplib.SMTP(smtp_host, smtp_port, timeout=30)

    with server_ctx as server:
        if smtp_starttls and not smtp_ssl:
            server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, to_emails, msg.as_string())


def send_email_graph(subject: str, html_body: str, cfg: Dict[str, object], to_emails: List[str]) -> None:
    graph_cfg = cfg.get("graph", {})
    if not isinstance(graph_cfg, dict):
        raise RuntimeError("'graph' config must be an object.")

    client_id = str(graph_cfg.get("client_id", "")).strip()
    tenant = str(graph_cfg.get("tenant", "consumers")).strip() or "consumers"
    scopes = graph_cfg.get("scopes", ["https://graph.microsoft.com/Mail.Send"])
    if isinstance(scopes, str):
        scopes = [scopes]

    required = {
        "graph.client_id": client_id,
        "to_emails": ",".join(to_emails),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing Graph config values: {', '.join(missing)}")

    try:
        import msal  # type: ignore
    except Exception as exc:
        raise RuntimeError("Missing dependency 'msal'. Install with: python -m pip install msal") from exc

    authority = f"https://login.microsoftonline.com/{tenant}"
    app = msal.PublicClientApplication(client_id=client_id, authority=authority)

    flow = app.initiate_device_flow(scopes=list(scopes))
    if "user_code" not in flow:
        raise RuntimeError(f"Failed to start device flow: {flow}")

    print(flow.get("message", "Open browser and complete device code sign-in."))
    token_result = app.acquire_token_by_device_flow(flow)
    access_token = token_result.get("access_token")
    if not access_token:
        err = token_result.get("error_description") or token_result.get("error") or str(token_result)
        raise RuntimeError(f"Graph auth failed: {err}")

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": e}} for e in to_emails],
        },
        "saveToSentItems": True,
    }

    resp = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Graph send mail failed: HTTP {resp.status_code} - {resp.text}")


def send_email(subject: str, text_body: str, html_body: str, config_path: str) -> None:
    cfg = load_email_config(config_path)
    provider = str(cfg.get("provider", "smtp")).strip().lower()
    to_emails = parse_email_list(cfg.get("to_emails", []))

    if provider == "smtp":
        send_email_smtp(subject, text_body, html_body, cfg, to_emails)
        return
    if provider == "graph":
        send_email_graph(subject, html_body, cfg, to_emails)
        return

    raise RuntimeError("Unsupported email provider. Use 'smtp' or 'graph'.")


def print_summary(total: int, new_n: int, updated_n: int, removed_n: int, db_path: str) -> None:
    print(f"Fetched rows: {total}")
    print(f"New jobs: {new_n}")
    print(f"Updated jobs: {updated_n}")
    print(f"Removed jobs: {removed_n}")
    print(f"DB: {db_path}")


def run(send_mail: bool, include_updates: bool, db_path: str, url: str, email_config_path: str, force_email: bool) -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    try:
        html = fetch_html(session, url)
        mannheim_jobs = parse_open_positions(html, url, session)
        zew_jobs = fetch_zew_jobs(session)
        jobs = mannheim_jobs + zew_jobs
    except Exception as exc:
        print(f"ERROR: failed to scrape source(s): {exc}", file=sys.stderr)
        return 2

    if not jobs:
        print("No jobs found on page.")
        return 0

    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)
        new_jobs, updated_jobs, removed_n = upsert_jobs(conn, jobs)

        pending_new = unsent_events(conn, new_jobs, "new")
        pending_updated = unsent_events(conn, updated_jobs, "updated") if include_updates else []
        all_jobs = get_active_jobs(conn)

        print_summary(len(jobs), len(new_jobs), len(updated_jobs), removed_n, db_path)

        if send_mail and (pending_new or pending_updated or force_email):
            try:
                email_cfg = load_email_config(email_config_path)
                translate_titles = bool(email_cfg.get("translate_title_to_en", False))
            except Exception:
                translate_titles = False

            email_jobs = attach_english_titles(conn, all_jobs, translate_titles)
            subject = (
                f"[Jobs Alert] new={len(pending_new)} updated={len(pending_updated)} "
                f"removed={removed_n} total={len(all_jobs)}"
            )
            text_body, html_body = build_email_content(email_jobs, pending_new, pending_updated)
            send_email(subject, text_body, html_body, email_config_path)
            mark_sent(conn, pending_new, "new")
            mark_sent(conn, pending_updated, "updated")
            print("Email sent.")
        elif send_mail:
            print("No unsent events. Email skipped.")

        return 0
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Mannheim + ZEW jobs and send email alerts."
    )
    parser.add_argument("--url", default=BASE_URL, help="Jobs page URL")
    parser.add_argument("--db", default=DB_FILE, help="SQLite DB file path")
    parser.add_argument(
        "--email-config",
        default=EMAIL_CONFIG_FILE,
        help="Email JSON config file path",
    )
    parser.add_argument("--send-email", action="store_true", help="Send email for new/updated jobs")
    parser.add_argument("--force-email", action="store_true", help="Send email even if no new/updated jobs")
    parser.add_argument(
        "--include-updates",
        action="store_true",
        help="When sending email, include updated jobs in addition to new jobs",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(
        send_mail=args.send_email,
        include_updates=args.include_updates,
        db_path=args.db,
        url=args.url,
        email_config_path=args.email_config,
        force_email=args.force_email,
    )


if __name__ == "__main__":
    raise SystemExit(main())

