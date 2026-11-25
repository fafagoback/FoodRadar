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


def initial_state(run_id, epoch, source_sha, stores, weights=None):
    ids = sorted(set(stores))
    if not ids:
        raise ValueError("Empty task pool")
    return {"version": 1, "run_id": str(run_id), "epoch": epoch,
            "source_sha": source_sha, "tasks": ids, "pending": ids.copy(),
            "weights": {sid: max(1, int((weights or {}).get(sid, 400))) for sid in ids},
            "leased": {}, "local_done": {}, "completed": {}}


def validate_state(state):
    if state.get("version") != 1:
        raise ValueError("Unknown task queue version")
    tasks = state["tasks"]
    pending = state["pending"]
    leased, completed = state["leased"], state["completed"]
    local_done = state.get("local_done", {})
    if any(type(v) is not int or v < 1 for v in state.get("weights", {}).values()):
        raise ValueError("Invalid workload weight")
    if len(set(tasks)) != len(tasks) or len(set(pending)) != len(pending):
        raise ValueError("Duplicate task ID")
    groups = [set(pending), set(leased), set(local_done), set(completed)]
    if any(groups[i] & groups[j] for i in range(len(groups)) for j in range(i + 1, len(groups))):
        raise ValueError("Task has more than one state")
    if set(tasks) != set.union(*groups):
        raise ValueError("Task roster changed")
    for sid, row in {**leased, **local_done, **completed}.items():
        if (not isinstance(row.get("worker"), int) or not 0 <= row["worker"] < 15
                or not row.get("owner") or not re.fullmatch("[a-f0-9]{32}", row.get("token", ""))):
            raise ValueError("Invalid task lease identity")
        if sid in completed and (type(row.get("artifact_id")) is not int or row["artifact_id"] <= 0):
            raise ValueError("Completed task lacks durable artifact ID")
    return state


def validate_batch_group(sid, group):
    """Cross-machine native category roster; raw bytes remain on workers."""
    if group.get("format") != "batch-section-v2":
        raise ValueError("Unknown cross-machine catalog group")
    sections = group.get("sections")
    if (not isinstance(sections, list) or not sections
            or len(set(sections)) != len(sections)
            or not all(isinstance(s, str) and s for s in sections)):
        raise ValueError("Invalid native catalog section roster")
    slots = [set(group.get("pending", [])), set(group.get("leased", {})),
             set(group.get("local_done", {})), set(group.get("completed", {}))]
    if any(slots[i] & slots[j] for i in range(4) for j in range(i+1,4)):
        raise ValueError("Category belongs to multiple states")
    if set.union(*slots) != set(map(str, range(len(sections)))):
        raise ValueError("Native category coverage mismatch")
    for row in (list(group["leased"].values()) + list(group["local_done"].values())
                + list(group["completed"].values())):
        if not (isinstance(row.get("worker"), int) and 0 <= row["worker"] < 15
                and re.fullmatch(r"[a-f0-9]{32}", row.get("token", ""))
                and row.get("owner")):
            raise ValueError("Invalid category lease")
    return group


def artifact_name(state, sid, lease):
    return "fr-task-" + state["epoch"] + "-" + sid + "-" + lease["token"]


class GitQueue:
    def __init__(self, repo, run_id, attempt=1, remote="origin", lease_seconds=LEASE_SECONDS, lane=None):
        self.repo = Path(repo).resolve()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", str(run_id)) or not str(attempt).isdigit():
            raise ValueError("Invalid queue ref")
        self.ref = f"refs/heads/fr-queue/{run_id}-{attempt}"
        if lane is not None:
            self.ref += f"-lane-{lane:02d}"
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
            ids = sorted(s["pending"], key=lambda sid: (-s.get("weights", {}).get(sid, 400), sid))[:slots]
            if not ids:
                return False, []
            selected = set(ids)
            s["pending"] = [sid for sid in s["pending"] if sid not in selected]
            now = time.time()
            result = []
            attempts = s.setdefault("attempts", {})
            for sid in ids:
                attempts[sid] = attempts.get(sid, 0) + 1
                row = {"owner": owner, "worker": worker, "token": token,
                       "expires": now + self.lease_seconds, "attempt": attempts[sid]}
                s["leased"][sid] = row
                result.append((sid, row))
            return True, result
        return self.transact(edit)

    def renew(self, owner):
        def edit(s):
            now = time.time()
            rows = [v for v in s["leased"].values()
                    if v["owner"] == owner]
            for group in s.get("catalog_groups", {}).values():
                rows.extend(v for v in group["leased"].values() if v["owner"] == owner)
            for row in rows:
                row["expires"] = now + self.lease_seconds
            return bool(rows), len(rows)
        return self.transact(edit)

    def expire_owner(self,owner):
        """Called only after that process has joined every active capture."""
        def edit(s):
            rows=[v for v in s["leased"].values() if v["owner"]==owner]
            for group in s.get("catalog_groups",{}).values():
                rows.extend(v for v in group["leased"].values() if v["owner"]==owner)
            changed=[row for row in rows if row["expires"]>0]
            for row in changed:row["expires"]=0
            return bool(changed),len(changed)
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
            # A delayed writer with the still-current fencing token may
            # complete an already-saved result; a reassigned token cannot.
            if recovered and not expired:
                return False, False
            s["completed"][sid] = {"owner": row["owner"], "worker": row["worker"],
                                   "token": token, "artifact_id": artifact_id}
            del s["leased"][sid]
            return True, True
        return self.transact(edit)

    def mark_local_batch(self, owner, records):
        """Release a worker's locally saved results from active leases."""
        def edit(s):
            local = s.setdefault("local_done", {})
            changed = False
            for sid, token in records.items():
                prior = local.get(sid) or s["completed"].get(sid)
                if prior is not None:
                    if prior["owner"] != owner or prior["token"] != token:
                        raise ValueError("Conflicting locally completed store")
                    continue
                row = s["leased"].get(sid)
                if not row or row["owner"] != owner or row["token"] != token:
                    raise ValueError("Invalid local task lease")
                local[sid] = row
                del s["leased"][sid]
                changed = True
            return changed, len(records)
        return self.transact(edit)

    def finish_batch(self, owner, records, artifact_id, batch_name):
        """Only mark stores complete after the entire worker artifact exists."""
        if type(artifact_id) is not int or artifact_id <= 0:
            raise ValueError("Invalid durable batch artifact ID")
        def edit(s):
            changed = False
            for sid, token in records.items():
                prior = s["completed"].get(sid)
                if prior:
                    if (prior["owner"], prior["token"], prior["artifact_id"], prior.get("batch_name")) != (owner, token, artifact_id, batch_name):
                        raise ValueError("Conflicting completed store")
                    continue
                row = s.get("local_done", {}).get(sid) or s["leased"].get(sid)
                if not row or row["owner"] != owner or row["token"] != token:
                    raise ValueError("Stale or unowned batch task")
                s["completed"][sid] = {"owner": owner, "worker": row["worker"],
                    "token": token, "artifact_id": artifact_id, "batch_name": batch_name}
                s.get("local_done", {}).pop(sid, None)
                s["leased"].pop(sid, None)
                changed = True
            return changed, len(records)
        return self.transact(edit)

    def register_catalog(self, sid, token, sections):
        def edit(s):
            group = s.setdefault("catalog_groups", {}).get(sid)
            if group is not None:
                if group.get("sections") != sections:
                    raise ValueError("Native section roster changed")
                return False, len(sections)
            row = s["leased"].get(sid) or s.get("local_done", {}).get(sid)
            if not row or row["token"] != token:
                raise ValueError("Store lease missing for category expansion")
            group = {"format":"batch-section-v2", "sections":list(sections),
                     "pending":list(map(str, range(len(sections)))),
                     "leased":{}, "local_done":{}, "completed":{}, "attempts":{}}
            s["catalog_groups"][sid] = group
            return True, len(sections)
        return self.transact(edit)

    def claim_catalog(self, owner, worker, sid):
        token = uuid.uuid4().hex
        def edit(s):
            group = s.get("catalog_groups", {}).get(sid)
            if not group or not group["pending"]:
                return False, None
            key = group["pending"].pop(0)
            group["attempts"][key] = group["attempts"].get(key,0)+1
            lease = {"owner":owner, "worker":worker, "token":token,
                     "expires":time.time()+self.lease_seconds,
                     "attempt":group["attempts"][key]}
            group["leased"][key] = lease
            validate_batch_group(sid, group)
            return True, (sid, key, lease)
        return self.transact(edit)

    def mark_local_catalog_batch(self, owner, records):
        def edit(s):
            changed = False
            for sid, keys in records.items():
                group = s["catalog_groups"][sid]
                for key, token in keys.items():
                    prior = group["local_done"].get(key) or group["completed"].get(key)
                    if prior is not None:
                        if prior["owner"] != owner or prior["token"] != token:
                            raise ValueError("Conflicting category completion")
                        continue
                    row = group["leased"].get(key)
                    if not row or row["owner"] != owner or row["token"] != token:
                        raise ValueError("Unowned category result")
                    group["local_done"][key] = row
                    del group["leased"][key]
                    changed = True
                validate_batch_group(sid, group)
            return changed, sum(len(v) for v in records.values())
        return self.transact(edit)

    def finish_catalog_batch(self, owner, records, artifact_id, batch_name):
        if type(artifact_id) is not int or artifact_id <= 0:
            raise ValueError("Invalid durable category artifact")
        def edit(s):
            changed = False
            for sid, keys in records.items():
                group = s["catalog_groups"][sid]
                for key, token in keys.items():
                    prior = group["completed"].get(key)
                    if prior is not None:
                        if (prior["owner"],prior["token"],prior["artifact_id"]) != (owner,token,artifact_id):
                            raise ValueError("Conflicting durable category result")
                        continue
                    row = group["local_done"].get(key) or group["leased"].get(key)
                    if not row or row["owner"] != owner or row["token"] != token:
                        raise ValueError("Stale category result")
                    group["completed"][key] = {**row, "artifact_id":artifact_id,
                                                "batch_name":batch_name}
                    group["local_done"].pop(key,None)
                    group["leased"].pop(key,None)
                    changed = True
                validate_batch_group(sid, group)
            return changed, sum(len(v) for v in records.values())
        return self.transact(edit)

    def reset_terminated_wave(self):
        """Only after the entire previous Actions matrix has terminated."""
        def edit(s):
            ids = sorted(set(s["leased"]) | set(s.get("local_done", {})))
            total = len(ids)
            for sid, group in s.get("catalog_groups", {}).items():
                validate_batch_group(sid, group)
                keys = sorted(set(group["leased"]) | set(group["local_done"]), key=int)
                if keys:
                    group["pending"] = sorted(group["pending"] + keys, key=int)
                    group["leased"].clear()
                    group["local_done"].clear()
                    total += len(keys)
            if not total:
                return False, 0
            s["pending"] = sorted(s["pending"] + ids)
            s["leased"].clear()
            s.get("local_done", {}).clear()
            return True, total
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


class SharedGitQueue:
    """One logical pool, multiple independent CAS lanes to avoid one hot ref.

    Lanes partition only the synchronization metadata, not the work. Any
    worker may claim any lane, in a randomized order, with no estimates and
    no persistent worker assignment. The immutable root defines the roster.
    """
    def __init__(self, repo, run_id, attempt=1, remote="origin", lease_seconds=LEASE_SECONDS):
        self.root = GitQueue(repo, run_id, attempt, remote, lease_seconds)
        self.args = (repo, run_id, attempt, remote, lease_seconds)
        self.lock = self.root.lock
        self.meta = None
        self.lanes = []
        self.lane_of = {}
        self.available = set()
        self.owner_lanes = {}

    def _configure(self, state):
        count = state.get("lane_count")
        if type(count) is not int or not 1 <= count <= 32 or count > len(state["tasks"]):
            raise ValueError("Invalid synchronization lane count")
        self.meta = state
        self.lanes = [GitQueue(*self.args, lane=i) for i in range(count)]
        for lane in self.lanes:
            lane.lock = self.lock
        self.lane_of = {sid: i % count for i, sid in enumerate(state["tasks"])}
        self.available = set(range(count))
        self.pending_by_lane = {}

    def _ensure(self):
        if self.meta is None:
            self._configure(self.root.read())

    def initialize(self, state):
        with self.lock:
            # Lanes partition queue metadata, not physical-worker ownership.
            state = dict(state, lane_count=min(16, max(1, (len(state["tasks"]) + 7) // 8)))
            self._configure(state)
            commits = [(self.root.make_commit(state), self.root.ref)]
            for index, lane in enumerate(self.lanes):
                part = initial_state(state["run_id"], state["epoch"], state["source_sha"],
                                     [sid for sid, i in self.lane_of.items() if i == index],
                                     weights=state.get("weights", {}))
                commits.append((lane.make_commit(part), lane.ref))
            args = ["push", "--atomic"]
            args += ["--force-with-lease=" + ref + ":" for _, ref in commits]
            args += [self.root.remote] + [sha + ":" + ref for sha, ref in commits]
            result = self.root.git(*args, check=False)
            if result.returncode:
                # A repeated initialization can only reuse the exact roster.
                current = self.root.read()
                if any(current[k] != state[k] for k in ("run_id", "epoch", "source_sha", "tasks", "lane_count")):
                    raise RuntimeError("Existing task pool identity differs")
                self._configure(current)
                self.read()
            return state

    def read(self):
        with self.lock:
            self._ensure()
            # Fetch all lane heads in one network operation.
            refs = [f"+{q.ref}:refs/fr-local/lane-{i:02d}" for i, q in enumerate(self.lanes)]
            self.root.git("fetch", "--no-tags", "--depth=1", self.root.remote, *refs)
            merged = {k: self.meta[k] for k in ("version", "run_id", "epoch", "source_sha", "tasks", "weights") if k in self.meta}
            merged.update(pending=[], leased={}, local_done={}, completed={}, catalog_groups={})
            available = set()
            pending_by_lane = {}
            for i, lane in enumerate(self.lanes):
                state = validate_state(json.loads(self.root.git("show", f"refs/fr-local/lane-{i:02d}:queue.json").stdout))
                if (any(state[k] != self.meta[k] for k in ("run_id", "epoch", "source_sha"))
                    or set(state["tasks"]) != {sid for sid, index in self.lane_of.items() if index == i}):
                    raise ValueError("Synchronization lane identity changed")
                if state["pending"]:
                    available.add(i)
                    pending_by_lane[i] = sorted(state["pending"], key=lambda sid: (-self.meta.get("weights", {}).get(sid, 400), sid))
                merged["pending"].extend(state["pending"])
                merged["leased"].update(state["leased"])
                merged["local_done"].update(state.get("local_done", {}))
                merged["completed"].update(state["completed"])
                for sid, group in state.get("catalog_groups", {}).items():
                    if sid not in state["tasks"] or sid in merged["catalog_groups"]:
                        raise ValueError("Invalid catalog task group owner")
                    merged["catalog_groups"][sid] = group
            self.available = available
            self.pending_by_lane = pending_by_lane
            merged["pending"].sort()
            return validate_state(merged)

    def claim(self, owner, worker, slots):
        """Pick the heaviest unclaimed store across all synchronization lanes."""
        if slots < 1:
            return []
        with self.lock:
            self._ensure()
            if not self.pending_by_lane:
                self.read()
            order = sorted(self.available, key=lambda i: (
                -self.meta.get("weights", {}).get(self.pending_by_lane[i][0], 400),
                self.pending_by_lane[i][0]))
            for index in order:
                claimed = self.lanes[index].claim(owner, worker, slots)
                if claimed:
                    self.owner_lanes.setdefault(owner, set()).add(index)
                    used = {sid for sid, _ in claimed}
                    self.pending_by_lane[index] = [
                        sid for sid in self.pending_by_lane[index] if sid not in used]
                    if not self.pending_by_lane[index]:
                        self.pending_by_lane.pop(index)
                        self.available.discard(index)
                    return claimed
                self.pending_by_lane.pop(index, None)
                self.available.discard(index)
            return []

    def register_catalog(self, sid, token, sections):
        with self.lock:
            self._ensure()
            return self.lanes[self.lane_of[sid]].register_catalog(sid, token, sections)

    def claim_work(self, owner, worker, slots):
        """Globally claim available category work before any new whole store."""
        if slots < 1:
            return []
        with self.lock:
            self._ensure()
            state = self.read()
            jobs = []
            for sid, group in state.get("catalog_groups", {}).items():
                if group.get("format") == "batch-section-v2" and group["pending"]:
                    weight = state.get("weights", {}).get(sid, 400)
                    jobs.append((-1000000 - weight / len(group["sections"]),
                                 sid, "section", self.lane_of[sid]))
            for sid in state["pending"]:
                jobs.append((-state.get("weights", {}).get(sid, 400), sid,
                             "store", self.lane_of[sid]))
            jobs.sort()
            claimed = []
            for _, sid, kind, lane in jobs:
                if len(claimed) >= slots:
                    break
                if kind == "section":
                    row = self.lanes[lane].claim_catalog(owner, worker, sid)
                    if row:
                        _, key, lease = row
                        lease["section_uuid"] = state["catalog_groups"][sid]["sections"][int(key)]
                        claimed.append(("section", sid, key, lease))
                else:
                    rows = self.lanes[lane].claim(owner, worker, 1)
                    if rows:
                        actual_sid, lease = rows[0]
                        claimed.append(("store", actual_sid, None, lease))
                if claimed:
                    self.owner_lanes.setdefault(owner, set()).add(lane)
            return claimed

    def mark_local_sections(self, owner, records):
        with self.lock:
            self._ensure()
            groups = {}
            for sid, keys in records.items():
                groups.setdefault(self.lane_of[sid], {})[sid] = keys
            return sum(self.lanes[i].mark_local_catalog_batch(owner, rows)
                       for i, rows in sorted(groups.items()))

    def finish_section_batch(self, owner, records, artifact_id, batch_name):
        with self.lock:
            self._ensure()
            groups = {}
            for sid, keys in records.items():
                groups.setdefault(self.lane_of[sid], {})[sid] = keys
            return sum(self.lanes[i].finish_catalog_batch(owner, rows, artifact_id, batch_name)
                       for i, rows in sorted(groups.items()))

    def renew(self, owner):
        with self.lock:
            self._ensure()
            total = 0
            lanes = self.owner_lanes.get(owner, set())
            for i in list(lanes):
                n = self.lanes[i].renew(owner)
                if not n:
                    lanes.discard(i)
                total += n
            return total

    def expire_owner(self,owner):
        with self.lock:
            self._ensure()
            return sum(self.lanes[i].expire_owner(owner)
                       for i in list(self.owner_lanes.get(owner,set())))

    def finish(self, sid, token, artifact_id, recovered=False):
        with self.lock:
            self._ensure()
            return self.lanes[self.lane_of[sid]].finish(sid, token, artifact_id, recovered=recovered)

    def recover_expired(self, find_artifact):
        state = self.read()
        for sid, row in state["leased"].items():
            if row["expires"] > time.time():
                continue
            artifact_id = find_artifact(artifact_name(state, sid, row))
            with self.lock:
                lane = self.lanes[self.lane_of[sid]]
                if artifact_id is None:
                    if lane.release_expired(sid, row["token"]):
                        self.available.add(self.lane_of[sid])
                else:
                    lane.finish(sid, row["token"], artifact_id, recovered=True)

    def mark_local_batch(self, owner, records):
        with self.lock:
            self._ensure()
            groups = {}
            for sid, token in records.items():
                groups.setdefault(self.lane_of[sid], {})[sid] = token
            return sum(self.lanes[i].mark_local_batch(owner, rows)
                       for i, rows in sorted(groups.items()))

    def finish_batch(self, owner, records, artifact_id, batch_name):
        with self.lock:
            self._ensure()
            groups = {}
            for sid, token in records.items():
                groups.setdefault(self.lane_of[sid], {})[sid] = token
            return sum(self.lanes[i].finish_batch(owner, rows, artifact_id, batch_name)
                       for i, rows in sorted(groups.items()))

    def reset_terminated_wave(self):
        with self.lock:
            state = self.read()
            affected = {self.lane_of[sid] for sid in set(state["leased"]) | set(state.get("local_done", {}))}
            affected |= {self.lane_of[sid] for sid,g in state.get("catalog_groups",{}).items()
                         if g.get("leased") or g.get("local_done")}
            return sum(self.lanes[i].reset_terminated_wave() for i in sorted(affected))

    def cleanup(self):
        with self.lock:
            self._ensure()
            state = self.read()
            if (state["pending"] or state["leased"] or state.get("local_done")
                    or any(g["pending"] or g["leased"] or g["local_done"]
                           for g in state.get("catalog_groups", {}).values())):
                raise RuntimeError("Cannot delete an incomplete task pool")
            refs = [self.root.ref] + [q.ref for q in self.lanes]
            self.root.git("push", "--atomic", self.root.remote, *[":" + ref for ref in refs])
