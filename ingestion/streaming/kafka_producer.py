#!/usr/bin/env python3
"""
Streaming ingestion: poll GitHub Events API and publish to Kafka topic
'github.events.raw'.  For a demo repo this is all you need; for production
you would replace this with a GitHub App webhook → Kafka Connect pipeline.

Usage:
    GITHUB_TOKEN=ghp_xxx python kafka_producer.py \
        --repos owner/repo1 owner/repo2 \
        --interval 30

Environment variables:
    GITHUB_TOKEN          GitHub personal access token (recommended)
    KAFKA_BOOTSTRAP       Kafka bootstrap servers  (default: localhost:9092)
    KAFKA_TOPIC           Target topic             (default: github.events.raw)
"""
from __future__ import annotations

import argparse
import json
import os
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

try:
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
except ImportError:
    pass

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",     "github.events.raw")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
API_URL         = os.getenv("API_URL",         "http://api:8000")
GH_API_BASE     = "https://api.github.com"


def fetch_tracked_repos(cli_repos: list[str] | None) -> list[str]:
    """
    Fetch dynamically tracked repositories from the API endpoint /streaming/repos.
    Falls back to CLI arguments, or default ['torvalds/linux'].
    """
    for base in [API_URL, "http://localhost:8000"]:
        if not base:
            continue
        try:
            url = f"{base.rstrip('/')}/streaming/repos"
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                repos = resp.json()
                if isinstance(repos, list) and len(repos) > 0:
                    return repos
        except Exception:
            pass
    if cli_repos:
        return cli_repos
    return ["torvalds/linux"]


# ── Kafka helpers ──────────────────────────────────────────────────────────

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 5,
        "linger.ms": 100,
    })


def ensure_topic(topic: str, partitions: int = 3, replication: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=5).topics
    if topic not in existing:
        admin.create_topics([NewTopic(topic, partitions, replication)])
        print(f"  Created topic '{topic}' with {partitions} partitions")


def delivery_report(err, msg) -> None:
    if err:
        print(f"  ✗ Delivery failed: {err}")


# ── GitHub API helpers ─────────────────────────────────────────────────────

def gh_headers() -> dict:
    h = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def fetch_events(owner: str, name: str, etag: str | None) -> tuple[list[dict], str | None]:
    """
    Fetch /repos/:owner/:name/events with ETag-based conditional polling
    so we don't hammer the API unnecessarily.
    """
    headers = gh_headers()
    if etag:
        headers["If-None-Match"] = etag

    resp = requests.get(
        f"{GH_API_BASE}/repos/{owner}/{name}/events",
        headers=headers,
        timeout=15,
    )

    if resp.status_code == 304:
        return [], etag
    if resp.status_code == 403:
        reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait  = max(reset - int(time.time()), 1)
        print(f"  Rate-limited. Sleeping {wait}s…")
        time.sleep(wait)
        return [], etag

    resp.raise_for_status()
    new_etag = resp.headers.get("ETag")
    return resp.json(), new_etag


# ── Main polling loop ──────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Events → Kafka producer")
    parser.add_argument("--repos",    nargs="*", default=None, help="owner/repo list (optional override)")
    parser.add_argument("--interval", type=int,  default=30,    help="Poll interval (seconds)")
    args = parser.parse_args()

    ensure_topic(KAFKA_TOPIC)
    producer = make_producer()

    etags: dict[str, str | None] = {}
    seen_ids: set[str] = set()

    print(f"Starting GitHub Events producer (polling every {args.interval}s → topic '{KAFKA_TOPIC}')")

    while True:
        tracked_repos = fetch_tracked_repos(args.repos)

        # Update etag registry for dynamically added / removed repos
        for r in tracked_repos:
            if r not in etags:
                etags[r] = None
        for r in list(etags.keys()):
            if r not in tracked_repos:
                del etags[r]

        for full_name in tracked_repos:
            parts = full_name.split("/")
            if len(parts) != 2:
                continue
            owner, name = parts
            try:
                events, new_etag = fetch_events(owner, name, etags.get(full_name))
                etags[full_name] = new_etag

                fresh = [e for e in events if e.get("id") not in seen_ids]
                seen_ids.update(e["id"] for e in fresh)

                for event in fresh:
                    payload = json.dumps({
                        "repo":       full_name,
                        "event_type": event.get("type"),
                        "actor":      event.get("actor", {}).get("login"),
                        "created_at": event.get("created_at"),
                        "payload":    event.get("payload", {}),
                    }).encode("utf-8")

                    producer.produce(
                        KAFKA_TOPIC,
                        key=full_name.encode(),
                        value=payload,
                        callback=delivery_report,
                    )

                if fresh:
                    print(f"  ✓ {full_name}: published {len(fresh)} event(s)")

                producer.poll(0)

            except Exception as exc:
                print(f"  ✗ {full_name}: {exc}")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
