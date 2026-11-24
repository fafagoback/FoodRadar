"""Run-scoped, atomic store task pool on the existing Git repository.

Only small queue metadata is committed. Raw responses are persisted in immutable
Actions artifacts, not Git. Every update uses a fast-forward CAS; a concurrent
writer cannot silently overwrite a claim, heartbeat or completion.
"""
from __future__ import annotations

import json
import os
import random
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path

LEASE_SECONDS = 180
HEARTBEAT_SECONDS = 30


def initial_state(run_id, epoch, source_sha, stores):
    ids = sorted(set(stores))
    if not ids:
        raise ValueError("Empty task pool")
    return {"version": 1, "run_id": str(run_id), "epoch": epoch,
            "source_sha": source_sha, "tasks": ids, "pending": ids.copy(),
            "leased": {}, "completed": {}}


def validate_state(state):
    if state.get("version") != 1:
        raise ValueError("Unknown task queue version")
    tasks = state["tasks"]
    pending = state["pending"]
    leased, completed = state["leased"], state["completed"]
    if len(set(tasks)) != len(tasks) or len(set(pending)) != len(pending):
        raise ValueError("Duplicate task ID")
    groups = [set(pending), set(leased), set(completed)]
    if any(groups[i] & groups[j] for i in range(3) for j in range(i + 1, 3)):
        raise ValueError("Task has more than one state")
    if set(tasks) != set.union(*groups):
        raise ValueError("Task roster changed")
    for sid, row in {**leased, **completed}.items():
        if (not isinstance(row.get("worker"), int) or not 0 <= row["worker"] < 15
                or not row.get("owner") or not re.fullmatch("[a-f0-9]{32}", row.get("token", ""))):
            raise ValueError("Invalid task lease identity")
        if sid in completed and (type(row.get("artifact_id")) is not int or row["artifact_id"] <= 0):
            raise ValueError("Completed task lacks durable artifact ID")
    return state


def artifact_name(state, sid, lease):
    return "fr-task-" + state["epoch"] + "-" + sid + "-" + lease["token"]


class GitQueue:
    def __init__(self, repo, run_id, attempt=1, remote="origin", lease_seconds=LEASE_SECONDS):
        self.repo = Path(repo).resolve()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", str(run_id)) or not str(attempt).isdigit():
            raise ValueError("Invalid queue ref")
        self.ref = f"refs/heads/fr-queue/{run_id}-{attempt}"
        self.run_id = str(run_id)
        self.remote = remote
        self.lease_seconds = lease_seconds
        self.lock = threading.RLock()
        self.env = dict(os.environ, GIT_AUTHOR_NAME="FoodRadar Queue",
                        GIT_AUTHOR_EMAIL="foodradar-queue@users.noreply.github.com",
                        GIT_COMMITTER_NAME="FoodRadar Queue",
                        GIT_COMMITTER_EMAIL="foodradar-queue@users.noreply.github.com",
                        GIT_TERMINAL_PROMPT="0")

    def git(self, *args, data=None, check=True):
        return subprocess.run(["git", "-C", str(self.repo), *args], input=data,
                              text=True, capture_output=True, env=self.env,
                              check=check, timeout=45)

    def read_head(self):
        self.git("fetch", "--no-tags", "--depth=1", self.remote, self.ref)
        head = self.git("rev-parse", "FETCH_HEAD").stdout.strip()
        state = validate_state(json.loads(self.git("show", head + ":queue.json").stdout))
        if state["run_id"] != self.run_id:
            raise ValueError("Task queue belongs to another run")
        return head, state

    def read(self):
        with self.lock:
            return self.read_head()[1]

    def make_commit(self, state, parent=None):
        payload = json.dumps(validate_state(state), sort_keys=True, separators=(",", ":"))
        blob = self.git("hash-object", "-w", "--stdin", data=payload).stdout.strip()
        tree = self.git("mktree", data=f"100644 blob {blob}\tqueue.json\n").stdout.strip()
        args = ["commit-tree", tree, "-m", "FoodRadar task pool state"]
        if parent:
            args.extend(["-p", parent])
        return self.git(*args).stdout.strip()

    def initialize(self, state):
        if state["run_id"] != self.run_id:
            raise ValueError("Queue run mismatch")
        with self.lock:
            commit = self.make_commit(state)
            result = self.git("push", "--force-with-lease=" + self.ref + ":",
                              self.remote, commit + ":" + self.ref, check=False)
            if result.returncode:
                current = self.read()
                if any(current[k] != state[k] for k in ("run_id", "epoch", "source_sha", "tasks")):
                    raise RuntimeError("Existing queue identity differs")
                return current
            return state

    def transact(self, edit):
        # Retry a rejected fast-forward using the newly fetched state, never a
        # force push. If a network failure follows a successful push, the next
        # invocation observes the idempotent operation already committed.
        with self.lock:
            for n in range(80):
                try:
                    head, state = self.read_head()
                    changed, result = edit(state)
                    if not changed:
                        return result
                    commit = self.make_commit(state, head)
                    pushed = self.git("push", self.remote, commit + ":" + self.ref, check=False)
                    if pushed.returncode == 0:
                        return result
                except (subprocess.SubprocessError, OSError):
                    if n == 79:
                        raise
                time.sleep(random.uniform(.04, min(2.0, .10 * (n + 1))))
            raise RuntimeError("Task queue CAS retry budget exhausted")

    def claim(self, owner, worker, slots):
        if slots < 1:
            return []
        token = uuid.uuid4().hex
        def edit(s):
            existing = [(sid, v) for sid, v in s["leased"].items()
                        if v["owner"] == owner and v["token"] == token]
            if existing:
                return False, existing
            ids = s["pending"][:slots]
            if not ids:
                return False, []
            s["pending"] = s["pending"][len(ids):]
            now = time.time()
            result = []
            for sid in ids:
                row = {"owner": owner, "worker": worker, "token": token,
                       "expires": now + self.lease_seconds}
                s["leased"][sid] = row
                result.append((sid, row))
            return True, result
        return self.transact(edit)

    def renew(self, owner):
        def edit(s):
            now = time.time()
            rows = [v for v in s["leased"].values()
                    if v["owner"] == owner and v["expires"] > now]
            for row in rows:
                row["expires"] = now + self.lease_seconds
            return bool(rows), len(rows)
        return self.transact(edit)

    def finish(self, sid, token, artifact_id, recovered=False):
        if type(artifact_id) is not int or artifact_id <= 0:
            raise ValueError("Invalid durable artifact ID")
        def edit(s):
            prior = s["completed"].get(sid)
            if prior:
                return False, prior["token"] == token and prior["artifact_id"] == artifact_id
            row = s["leased"].get(sid)
            if not row or row["token"] != token:
                return False, False
            expired = row["expires"] <= time.time()
            if expired != recovered:
                return False, False
            s["completed"][sid] = {"owner": row["owner"], "worker": row["worker"],
                                   "token": token, "artifact_id": artifact_id}
            del s["leased"][sid]
            return True, True
        return self.transact(edit)

    def release_expired(self, sid, token):
        def edit(s):
            row = s["leased"].get(sid)
            if not row or row["token"] != token or row["expires"] > time.time():
                return False, False
            del s["leased"][sid]
            s["pending"].append(sid)
            return True, True
        return self.transact(edit)

    def recover_expired(self, find_artifact):
        """An uploaded result is recovered before an expired task is requeued.

        The lookup must distinguish absence from a service failure. Never
        release a lease on an ambiguous storage error. A late stale result is
        fenced by its old token and cannot replace a newer accepted result.
        """
        state = self.read()
        for sid, row in state["leased"].items():
            if row["expires"] > time.time():
                continue
            artifact_id = find_artifact(artifact_name(state, sid, row))
            if artifact_id is None:
                self.release_expired(sid, row["token"])
            else:
                self.finish(sid, row["token"], artifact_id, recovered=True)
