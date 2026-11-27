"""Second-stage, store-only DB task pool. Separate from the UE crawler pool.

All requests are signed with the short-lived GitHub Actions OIDC token.
The Supabase Edge Function selects FR/FRT by the repository's signed identity.
"""
from __future__ import annotations

import json
import os
import random
import time
import urllib.parse
import urllib.request

import requests

ENDPOINT = "https://unapiwajmgqqtdkixaab.supabase.co/functions/v1/foodradar-db-pool"
AUDIENCE = "foodradar:db-pool"
RETRY_CODES = {408, 429, 500, 502, 503, 504}


class DBTaskPool:
    def __init__(self, session=None):
        self.http = session or requests.Session()
        self._token_value = None
        self._token_valid_until = 0.0

    def _token(self):
        if self._token_value is None or time.monotonic() >= self._token_valid_until:
            base = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
            query = urllib.parse.urlencode({"audience": AUDIENCE})
            url = base + ("&" if "?" in base else "?") + query
            request = urllib.request.Request(url, headers={
                "Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
            })
            with urllib.request.urlopen(request, timeout=15) as response:
                self._token_value = json.load(response)["value"]
            self._token_valid_until = time.monotonic() + 180
        return self._token_value

    def command(self, operation, **args):
        payload = {"op": operation, "args": args}
        for retry in range(5):
            try:
                response = self.http.post(ENDPOINT, json=payload, headers={
                    "X-FR-Workflow-Token": self._token(),
                    "Content-Type": "application/json",
                }, timeout=25)
                if response.status_code == 200:
                    return response.json()
                if response.status_code == 401 and retry == 0:
                    self._token_value = None
                    continue
                if response.status_code not in RETRY_CODES:
                    raise RuntimeError(
                        f"DB task pool {operation}: HTTP {response.status_code}")
                retry_after = response.headers.get("Retry-After", "0")
                try:
                    delay = max(0, float(retry_after))
                except ValueError:
                    delay = 0
            except requests.RequestException:
                delay = 0
            if retry == 4:
                break
            time.sleep(max(delay, min(20, 2 ** retry) + random.random()))
        raise RuntimeError(f"DB task pool {operation} exhausted retries")

    def purge(self):
        """00 only: refuse to clear any pool containing live leases."""
        return self.command("purge")

    def seed_from_crawl(self, source_run_id, source_attempt=1):
        """00 only: seed directly from verified first-stage store rows.

        This avoids a 7 GB scan, and never changes the crawl task table.
        """
        return self.command("seed_from_crawl",
                            source_run_id=str(source_run_id),
                            source_attempt=int(source_attempt))

    def claim(self, worker, preferred_artifact=""):
        """Claim exactly one store, with optional artifact-locality preference."""
        return self.command("claim", worker=int(worker),
                            preferred_artifact=preferred_artifact)

    def heartbeat(self, worker):
        return self.command("heartbeat", worker=int(worker))

    def local_done(self, worker, store_uuid, token):
        return self.command("local_done", worker=int(worker),
                            store_uuid=store_uuid, token=token)

    def finish(self, worker, store_uuid, token, artifact, **result):
        if not artifact:
            raise ValueError("Durable artifact reference is required")
        return self.command("finish", worker=int(worker), store_uuid=store_uuid,
                            token=token, result={"artifact": artifact, **result})

    def fail(self, worker, store_uuid, token, reason):
        return self.command("fail", worker=int(worker), store_uuid=store_uuid,
                            token=token, result={"reason": str(reason)[:512]})

    def status(self):
        return self.command("status")

    def results(self):
        after_id = 0
        while True:
            page = self.command("results", after_id=after_id)
            if not isinstance(page, list):
                raise ValueError("Invalid result page")
            for row in page:
                if row["id"] <= after_id:
                    raise ValueError("Nonmonotonic DB pool result page")
                after_id = row["id"]
                yield row
            if len(page) < 1000:
                break
