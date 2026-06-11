"""Inbound-message watcher: push new iMessages to a webhook.

Polls chat.db for new inbound rows (is_from_me = 0) and POSTs one JSON
payload per message to a webhook URL (e.g. a Hermes gateway webhook route).
Auth is a static token sent as ``X-Gitlab-Token``; idempotency uses
``X-Request-ID: imessage-<rowid>`` so webhook retries dedupe server-side.

State (last delivered ROWID) persists to a JSON file so restarts neither
flood history nor drop messages. The cursor only advances after a 2xx
response; a failed POST is retried on the next tick.

Run via the launcher app bundle so TCC's Full Disk Access grant applies:
    mac-messages-watch --url https://host/webhooks/imessage \
        --token-file ~/.config/imessage-mcp/webhook-token
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import timezone
from pathlib import Path

from .messages import (
    _from_apple_ns,
    extract_body_from_attributed,
    get_chat_mapping,
    get_contact_name,
    query_messages_db,
)

POLL_QUERY = """
SELECT
    m.ROWID,
    m.date,
    m.text,
    m.attributedBody,
    m.handle_id,
    m.cache_roomnames
FROM
    message m
WHERE
    m.ROWID > ?
    AND m.is_from_me = 0
ORDER BY m.ROWID ASC
LIMIT 50
"""

MAX_ROWID_QUERY = "SELECT COALESCE(MAX(ROWID), 0) AS max_rowid FROM message"


def _load_state(path: Path) -> int:
    try:
        return int(json.loads(path.read_text())["last_rowid"])
    except Exception:
        return -1


def _save_state(path: Path, last_rowid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"last_rowid": last_rowid}))
    tmp.replace(path)


def _current_max_rowid() -> int | None:
    """Current head ROWID, or None when chat.db is unreadable (e.g. no FDA)."""
    rows = query_messages_db(MAX_ROWID_QUERY)
    if rows and "error" not in rows[0]:
        return int(rows[0]["max_rowid"])
    return None


def _message_payload(row: dict, chat_mapping: dict) -> dict | None:
    """Build the webhook payload for one chat.db row, or None to skip."""
    body = row.get("text")
    if not body and row.get("attributedBody"):
        body = extract_body_from_attributed(row["attributedBody"])
    if not body:
        return None  # tapbacks, typing indicators, attachment-only stubs

    try:
        date_iso = _from_apple_ns(int(row["date"])).astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, OverflowError):
        date_iso = None

    try:
        sender = get_contact_name(row["handle_id"])
    except Exception:
        sender = f"handle:{row.get('handle_id')}"

    group = None
    if row.get("cache_roomnames"):
        group = chat_mapping.get(row["cache_roomnames"]) or row["cache_roomnames"]

    return {
        "event_type": "imessage_received",
        "rowid": row["ROWID"],
        "sender": sender,
        "text": body,
        "date": date_iso,
        "group": group,
    }


def _post(url: str, token: str, payload: dict, timeout: float = 15.0) -> bool:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Gitlab-Token": token,
            "X-Request-ID": f"imessage-{payload['rowid']}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        print(f"[watcher] POST {url} -> {e.code}: {e.read()[:200]!r}", flush=True)
        return False
    except Exception as e:
        print(f"[watcher] POST {url} failed: {e}", flush=True)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Push new inbound iMessages to a webhook")
    parser.add_argument("--url", required=True, help="Webhook URL to POST each message to")
    parser.add_argument("--token-file", required=True, help="File containing the static webhook token")
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds (default: 5)")
    parser.add_argument(
        "--state-file",
        default=str(Path.home() / ".config/imessage-mcp/watch-state.json"),
        help="JSON file persisting the last delivered ROWID",
    )
    args = parser.parse_args()

    token = Path(args.token_file).expanduser().read_text().strip()
    if not token:
        print("[watcher] FATAL: empty webhook token", flush=True)
        return 1

    state_path = Path(args.state_file).expanduser()
    last_rowid = _load_state(state_path)
    while last_rowid < 0:
        # First run: start at the current head — never flood history. If the
        # DB is unreadable (FDA not granted yet), keep waiting; persisting a
        # bogus cursor here would replay the entire history once access works.
        head = _current_max_rowid()
        if head is None:
            print("[watcher] chat.db unreadable (Full Disk Access?) — retrying", flush=True)
            time.sleep(max(args.interval, 15.0))
            continue
        last_rowid = head
        _save_state(state_path, last_rowid)
        print(f"[watcher] initialized cursor at ROWID {last_rowid}", flush=True)

    print(f"[watcher] watching for inbound messages > ROWID {last_rowid}, posting to {args.url}", flush=True)

    while True:
        try:
            rows = query_messages_db(POLL_QUERY, (last_rowid,))
            if rows and "error" in rows[0]:
                print(f"[watcher] chat.db error: {rows[0]['error']}", flush=True)
                rows = []

            if rows:
                chat_mapping = get_chat_mapping()
                for row in rows:
                    payload = _message_payload(row, chat_mapping)
                    if payload is not None and not _post(args.url, token, payload):
                        break  # retry this message next tick; don't advance past it
                    last_rowid = int(row["ROWID"])
                    _save_state(state_path, last_rowid)
        except Exception as e:
            print(f"[watcher] poll error: {e}", flush=True)

        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
